from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit
import paho.mqtt.client as mqtt
import paho.mqtt.properties as props
from paho.mqtt.packettypes import PacketTypes
import uuid
import threading
import time
import os

# --- MQTT Configuration (Defaults, can be overridden by UI) ---
BROKER_HOST = "localhost"
DEFAULT_MQTT_PORT = 1883
DEFAULT_MQTTS_PORT = 8883
CA_CERT_PATH = "C:/Program Files/mosquitto/certs/ca.crt" # Used if MQTTS is selected

# --- Web App Configuration ---
MQTT_KEEPALIVE_SECONDS = 3600 # 1 hour keepalive
REQUEST_TIMEOUT_SECONDS = 10 # Timeout for request/response

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'a_very_dev_secret_key_for_mqtt_!@#$')
if app.config['SECRET_KEY'] == 'a_very_dev_secret_key_for_mqtt_!@#$':
    print("WARNING: FLASK_SECRET_KEY environment variable not set or is default. Using an insecure default.")
    print("For production, set a strong, random FLASK_SECRET_KEY environment variable.")
    print("Generate one with: import secrets; print(secrets.token_hex(24))")

socketio = SocketIO(app, async_mode='threading') # Explicitly using threading for paho-mqtt compatibility

# Stores {'sid': {'client': obj, 'loop_thread': obj, 'mqtt_user': str, 'client_id': str, 'pending_requests': {}}}
mqtt_clients = {}
# Stores {'client_id_str': threading.Event()}
client_stop_events = {} 

def create_mqtt_client_for_session(session_id, mqtt_username, mqtt_password, use_mqtts, lwt_options=None):
    # Generate a client ID based on username and a part of session_id for some uniqueness
    # MQTT Client IDs should typically be <= 23 bytes for full compatibility, though MQTTv5 allows more.
    # We use a part of the SID to help ensure uniqueness if the same user logs in from multiple tabs.
    unique_part = session_id.replace('-', '')[:8] # Use 8 chars from SID
    client_id_str = f"web_{mqtt_username[:10]}_{unique_part}" # Keep it relatively short
    
    print(f"SID {session_id}: Creating MQTT client. Desired ID: {client_id_str} for MQTT user: {mqtt_username}")
    
    stop_event = threading.Event()
    # Note: The actual client ID might be slightly different if Paho modifies it (e.g., if clean_session=False with MQTTv3)
    # We'll store the stop event keyed by the ID we intend to use, and update if Paho gives a different one.
    # However, with MQTTv5 and explicit client_id, it should be what we set.
    
    # Using VERSION1 callback API to match existing callback signatures
    mqtt_client = mqtt.Client(client_id=client_id_str, protocol=mqtt.MQTTv5, callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
    actual_client_id = mqtt_client._client_id.decode() # Get the actual client ID paho will use
    
    if actual_client_id != client_id_str:
        print(f"SID {session_id}: Paho-MQTT adjusted Client ID from '{client_id_str}' to '{actual_client_id}'. Using actual ID for stop event.")
    client_stop_events[actual_client_id] = stop_event # Key stop event by actual client ID

    mqtt_client.username_pw_set(mqtt_username, mqtt_password)
    mqtt_client.user_data_set({
        'sid': session_id, 
        'mqtt_user': mqtt_username, 
        'client_id': actual_client_id, # Store actual client ID
        'pending_requests': {}
    })

    if lwt_options and lwt_options.get('topic') and lwt_options.get('payload') is not None: # Payload can be empty string
        try:
            lwt_topic = lwt_options['topic']
            lwt_payload = lwt_options['payload']
            lwt_qos = int(lwt_options.get('qos', 0))
            lwt_retain = bool(lwt_options.get('retain', False))
            mqtt_client.will_set(lwt_topic, lwt_payload, lwt_qos, lwt_retain)
            print(f"SID {session_id} (User: {mqtt_username}, CID: {actual_client_id}): LWT configured for topic '{lwt_topic}'")
        except Exception as e:
            print(f"SID {session_id} (User: {mqtt_username}, CID: {actual_client_id}): Error setting LWT: {e}")

    def on_connect(client, userdata, flags, rc, properties=None):
        sid = userdata['sid']
        user = userdata['mqtt_user']
        cid = userdata['client_id'] # This is the actual client ID
        if rc == 0:
            print(f"SID {sid} (User: {user}, CID: {cid}): MQTT Client connected successfully.")
            socketio.emit('mqtt_status', {'message': f'MQTT Connected as {user}! (CID: {cid})'}, room=sid)
            response_topic_subscription = f"clients/{cid}/response/#"
            client.subscribe(response_topic_subscription, qos=1)
            print(f"SID {sid} (User: {user}, CID: {cid}): Auto-subscribed to own response topic: {response_topic_subscription}")
        else:
            reason = properties.ReasonString if properties and hasattr(properties, 'ReasonString') else f"rc: {rc}"
            print(f"SID {sid} (User: {user}, CID: {cid}): MQTT Client failed to connect. Reason: {reason}")
            socketio.emit('mqtt_status', {'message': f'MQTT Connection Failed for {user}: {reason}'}, room=sid)
            if cid in client_stop_events:
                print(f"SID {sid} (CID: {cid}): Signaling stop event due to connection failure.")
                client_stop_events[cid].set() 
            # Ensure full cleanup for this SID if connection fails
            handle_socket_disconnect(manual_sid=sid, reason=f"MQTT Connection Failed ({reason})")


    def on_disconnect(client, userdata, rc, properties=None):
        sid = userdata['sid']
        user = userdata['mqtt_user']
        cid = userdata['client_id']
        reason = properties.ReasonString if properties and hasattr(properties, 'ReasonString') else f"rc: {rc}"
        print(f"SID {sid} (User: {user}, CID: {cid}): MQTT Client disconnected. Reason: {reason}")
        socketio.emit('mqtt_status', {'message': f'MQTT Disconnected for {user}: {reason}'}, room=sid)
        # Note: loop_forever might try to reconnect internally based on 'rc'.
        # If the stop_event is set (e.g., due to initial auth failure), the custom loop target should exit.
        # If this is an unexpected disconnect after being connected, loop_forever's reconnect might proceed.

    def on_message(client, userdata, msg):
    # --- Essential context from userdata ---
        sid = userdata.get('sid')
        cid = userdata.get('client_id')
        mqtt_user = userdata.get('mqtt_user', 'UnknownUser')

        if not sid or not cid:
            print(f"ON_MESSAGE: Received message but userdata is missing sid or cid. Userdata: {userdata}. Topic: {msg.topic}. Payload: {msg.payload[:50]}")
            return  # Cannot proceed without session/client context

        # --- Get the CORRECT client session information and pending_requests dictionary ---
        client_session_info = mqtt_clients.get(sid)
        if not client_session_info:
            print(f"SID {sid} (User: {mqtt_user}, CID: {cid}): ON_MESSAGE - No active client session info found in mqtt_clients. Ignoring message on topic {msg.topic}.")
            return
        
        # Ensure 'pending_requests' exists for this session; it should have been initialized during login.
        pending_requests_for_client = client_session_info.get('pending_requests')
        if pending_requests_for_client is None:
            print(f"SID {sid} (User: {mqtt_user}, CID: {cid}): ON_MESSAGE - CRITICAL ERROR - 'pending_requests' dictionary was missing for this session! Re-initializing (potential data loss). Topic: {msg.topic}")
            client_session_info['pending_requests'] = {}  # This should not happen if login logic is correct
            pending_requests_for_client = client_session_info['pending_requests']
        # --- End of getting correct pending_requests ---

        response_prefix_for_this_client = f"clients/{cid}/response/"  # e.g., clients/web_client_actual_id/response/
        is_a_response_to_us = False  # Flag to track if we processed this as a direct response

        # --- Stage 1: Check if this message is a direct response to a request sent by THIS client ---
        if msg.topic.startswith(response_prefix_for_this_client):
            print(f"SID {sid} (User: {mqtt_user}, CID: {cid}): ON_MESSAGE - Message on potential response topic: {msg.topic}")

            correlation_id_from_topic = msg.topic.split('/')[-1]
            correlation_id_from_props = None
            if msg.properties and msg.properties.CorrelationData:
                try:
                    correlation_id_from_props = msg.properties.CorrelationData.decode('utf-8')
                except Exception as e:
                    print(f"SID {sid} (User: {mqtt_user}, CID: {cid}): ON_MESSAGE - Error decoding CorrelationData from properties: {e}")
            
            final_correlation_id = correlation_id_from_props if correlation_id_from_props else correlation_id_from_topic
            print(f"SID {sid} (User: {mqtt_user}, CID: {cid}): ON_MESSAGE - Extracted final_correlation_id: '{final_correlation_id}' (from_props: '{correlation_id_from_props}', from_topic: '{correlation_id_from_topic}')")
            print(f"SID {sid} (User: {mqtt_user}, CID: {cid}): ON_MESSAGE - Current pending_requests keys: {list(pending_requests_for_client.keys())}")

            if final_correlation_id in pending_requests_for_client:
                is_a_response_to_us = True  # Set the flag
                request_info = pending_requests_for_client.pop(final_correlation_id)  # Remove from pending
                send_time_ms = request_info['send_time_ms']
                latency_ms = (time.time() * 1000) - send_time_ms
                
                print(f"SID {sid} (User: {mqtt_user}, CID: {cid}): ON_MESSAGE - Successfully processed RESPONSE for CorrID {final_correlation_id}. Latency: {latency_ms:.2f}ms. Topic: {msg.topic}")
                
                socketio.emit('mqtt_request_response_update', {
                    'type': 'response', 'correlation_id': final_correlation_id, 'topic': msg.topic,
                    'payload': msg.payload.decode('utf-8', errors='replace'), 'latency_ms': latency_ms, 'status': 'completed'
                }, room=sid)
            else:
                print(f"SID {sid} (User: {mqtt_user}, CID: {cid}): ON_MESSAGE - Received msg on response topic for CorrID '{final_correlation_id}', BUT IT WAS NOT FOUND in pending_requests OR ALREADY PROCESSED/TIMED OUT. Topic: {msg.topic}")
                # It's possible the timeout handler already removed it. This is not necessarily an error.

        # --- Stage 2: If not a direct response to us, check if it's a general request we should respond to ---
        if not is_a_response_to_us:
            response_topic_from_req = None
            correlation_data_from_req = None  # This will be bytes
            is_a_general_request_we_can_echo = False

            if msg.properties:
                if hasattr(msg.properties, 'ResponseTopic') and msg.properties.ResponseTopic:
                    response_topic_from_req = msg.properties.ResponseTopic
                if hasattr(msg.properties, 'CorrelationData') and msg.properties.CorrelationData:
                    correlation_data_from_req = msg.properties.CorrelationData  # Keep as bytes

            if response_topic_from_req and correlation_data_from_req:
                # Avoid responding to requests we might have sent to a general topic if this client also subscribes to it
                # A more robust check might involve looking at the publisher's client ID if available in UserProps
                # For now, a simple topic check (e.g. don't respond if topic starts with 'clients/<own_cid>/request/')
                if not msg.topic.startswith(f"clients/{cid}/request/"):  # Avoid echoing own general requests
                    is_a_general_request_we_can_echo = True
                    print(f"SID {sid} (User: {mqtt_user}, CID: {cid}): ON_MESSAGE - Received a general REQUEST on topic {msg.topic}, will attempt echo response.")
                    try:
                        response_payload = f"ECHO RESPONSE from {cid}: {msg.payload.decode('utf-8', errors='replace').upper()}"
                        
                        response_props = props.Properties(PacketTypes.PUBLISH)
                        response_props.CorrelationData = correlation_data_from_req  # Send back original correlation data (bytes)
                        
                        client.publish(response_topic_from_req, response_payload, qos=1, properties=response_props)
                        print(f"SID {sid} (User: {mqtt_user}, CID: {cid}): ON_MESSAGE - Sent ECHO RESPONSE to {response_topic_from_req} for request on {msg.topic}")
                    except Exception as e:
                        print(f"SID {sid} (User: {mqtt_user}, CID: {cid}): ON_MESSAGE - Error sending echo response: {e}")
            
            # --- Stage 3: If not a direct response to us AND not a general request we echoed, treat as a standard subscribed message ---
            if not is_a_general_request_we_can_echo:  # Important: only if we didn't already process it as an echo
                print(f"SID {sid} (User: {mqtt_user}, CID: {cid}): ON_MESSAGE - Processing as standard message. Topic: {msg.topic}. is_a_response_to_us={is_a_response_to_us}, is_a_general_request_we_can_echo={is_a_general_request_we_can_echo}")
                
                publish_time_ms = None
                if msg.properties and hasattr(msg.properties, 'UserProperty'):
                    for key, value in msg.properties.UserProperty:
                        if key == 'client_publish_time_ms':
                            try: 
                                publish_time_ms = float(value)
                                break
                            except ValueError:
                                print(f"SID {sid} (User: {mqtt_user}, CID: {cid}): ON_MESSAGE - Error parsing client_publish_time_ms: {value}")
                
                latency_ms = ((time.time() * 1000) - publish_time_ms) if publish_time_ms is not None else None

                print(f"SID {sid} (User: {mqtt_user}, CID: {cid}): ON_MESSAGE - Standard MQTT Message. Topic: '{msg.topic}'. Latency: {latency_ms:.2f}ms" if latency_ms is not None else f"SID {sid} (User: {mqtt_user}, CID: {cid}): ON_MESSAGE - Standard MQTT Message. Topic: '{msg.topic}' (no client latency info)")
                
                socketio.emit('mqtt_message', {
                    'topic': msg.topic, 
                    'payload': msg.payload.decode('utf-8', errors='replace'),  # Handle potential decode errors
                    'qos': msg.qos,
                    'retain': msg.retain, 
                    'latency_ms': latency_ms
                }, room=sid)

    def on_publish(client, userdata, mid):
        sid = userdata['sid']
        print(f"SID {sid} (CID: {userdata['client_id']}): MQTT Message MID {mid} published (broker ack for QoS > 0).")
        socketio.emit('publish_ack', {'message': f'Message (MID: {mid}) confirmed by broker (QoS>0).'}, room=sid)
        
    def on_subscribe(client, userdata, mid, granted_qos, properties=None):
        sid = userdata['sid']
        granted_qos_str = [str(qos.value if hasattr(qos, 'value') else qos) for qos in granted_qos]
        print(f"SID {sid} (CID: {userdata['client_id']}): MQTT Subscribed MID {mid} with granted QoS: {granted_qos_str}")
        socketio.emit('mqtt_status', {'message': f'Subscribed (MID: {mid}, QoS: {", ".join(granted_qos_str)})'}, room=sid)

    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message
    mqtt_client.on_publish = on_publish
    mqtt_client.on_subscribe = on_subscribe

    current_port = DEFAULT_MQTTS_PORT if use_mqtts else DEFAULT_MQTT_PORT
    print(f"SID {session_id} (User: {mqtt_username}, CID: {actual_client_id}): Preparing to connect to MQTT at {BROKER_HOST}:{current_port} (MQTTS: {use_mqtts})")

    if use_mqtts:
        try:
            mqtt_client.tls_set(ca_certs=CA_CERT_PATH)
        except Exception as e:
            # ... (error handling and cleanup stop event)
            print(f"SID {session_id} (User: {mqtt_username}, CID: {actual_client_id}): Error setting up TLS: {e}")
            socketio.emit('mqtt_status', {'message': f'TLS Setup Error: {e}'}, room=session_id)
            if actual_client_id in client_stop_events: client_stop_events.pop(actual_client_id, None)
            return None


    connect_properties = props.Properties(PacketTypes.CONNECT)
    connect_properties.SessionExpiryInterval = 0 

    try:
        mqtt_client.connect_async(BROKER_HOST, current_port, MQTT_KEEPALIVE_SECONDS, properties=connect_properties)
        print(f"SID {session_id} (User: {mqtt_username}, CID: {actual_client_id}): MQTT connect_async initiated.")
    except Exception as e:
        # ... (error handling and cleanup stop event)
        print(f"SID {session_id} (User: {mqtt_username}, CID: {actual_client_id}): MQTT connect_async error: {e}")
        socketio.emit('mqtt_status', {'message': f'MQTT Connection Initiation Error: {e}'}, room=session_id)
        if actual_client_id in client_stop_events: client_stop_events.pop(actual_client_id, None)
        return None
            
    return mqtt_client

def mqtt_loop_with_stop_event(client_instance, client_id_for_event_key):
    print(f"MQTTLoop (CID: {client_id_for_event_key}): Starting loop...")
    stop_event_instance = client_stop_events.get(client_id_for_event_key)

    if not stop_event_instance:
        print(f"MQTTLoop (CID: {client_id_for_event_key}): ERROR - No stop event found. Loop will not start.")
        return

    try:
        # Paho's loop_forever handles reconnects internally.
        # If initial connect fails (e.g. auth), on_connect sets stop_event_instance.
        # Then, client.disconnect() is called from handle_socket_disconnect, which should break loop_forever.
        # This loop_forever will also try to reconnect if the connection drops *after* initially succeeding,
        # unless the stop_event is set for other reasons.
        client_instance.loop_forever(retry_first_connection=False) # Don't let paho retry first connect if we handle it via stop_event

    except Exception as e:
        print(f"MQTTLoop (CID: {client_id_for_event_key}): Exception: {e}")
    finally:
        print(f"MQTTLoop (CID: {client_id_for_event_key}): Exited.")
        # Clean up the stop event if it's ours and still exists
        if client_id_for_event_key in client_stop_events and client_stop_events[client_id_for_event_key] == stop_event_instance:
            client_stop_events.pop(client_id_for_event_key, None)


@app.route('/')
def index_route():
    return render_template('index.html')

@socketio.on('connect')
def handle_socket_connect():
    session_id = request.sid
    print(f"Socket.IO client connected: SID {session_id}. Waiting for web login.")
    socketio.emit('request_login', {'message': 'Please provide MQTT credentials and connection options.'}, room=session_id)

@socketio.on('web_login')
def handle_web_login(data):
    session_id = request.sid
    mqtt_username = data.get('username')
    mqtt_password = data.get('password')
    use_mqtts = data.get('use_mqtts', True)
    lwt_options = data.get('lwt')

    if not mqtt_username or not mqtt_password: # Basic validation
        socketio.emit('login_status', {'success': False, 'message': 'Username and password are required.'}, room=session_id)
        return

    print(f"SID {session_id}: Received web login for MQTT user: {mqtt_username}, MQTTS: {use_mqtts}, LWT: {bool(lwt_options)}")

    if session_id in mqtt_clients:
        print(f"SID {session_id}: Cleaning up previous MQTT client for re-login by user {mqtt_clients[session_id].get('mqtt_user','N/A')}.")
        handle_socket_disconnect(manual_sid=session_id, reason="Re-login attempt")

    socketio.emit('mqtt_status', {'message': f'Attempting MQTT connection for {mqtt_username}...'}, room=session_id)
    mqtt_client_instance = create_mqtt_client_for_session(session_id, mqtt_username, mqtt_password, use_mqtts, lwt_options)
    
    if mqtt_client_instance:
        client_actual_id = mqtt_client_instance._client_id.decode()
        loop_thread = threading.Thread(target=mqtt_loop_with_stop_event, args=(mqtt_client_instance, client_actual_id), daemon=True)
        loop_thread.name = f"MQTTLoop_{client_actual_id[:12]}"
        loop_thread.start()
        
        mqtt_clients[session_id] = {
            'client': mqtt_client_instance, 'loop_thread': loop_thread,
            'mqtt_user': mqtt_username, 'client_id': client_actual_id,
            'pending_requests': {} # Ensure this is initialized
        }
        socketio.emit('login_status', {'success': True, 'username': mqtt_username, 'client_id': client_actual_id}, room=session_id)
        print(f"SID {session_id}: MQTT client for {mqtt_username} (CID: {client_actual_id}) created and network loop started.")
    else:
        print(f"SID {session_id}: Failed to create or initiate MQTT client for {mqtt_username}.")
        socketio.emit('login_status', {'success': False, 'message': 'Failed to initialize MQTT client.'}, room=session_id)


def handle_socket_disconnect(manual_sid=None, reason="Unknown"):
    session_id = manual_sid if manual_sid else request.sid
    print(f"Socket.IO client disconnect processing for SID {session_id}. Reason: {reason}")
    
    client_info = mqtt_clients.pop(session_id, None)
    if not client_info:
        print(f"SID {session_id}: No active MQTT client info found for cleanup.")
        return

    mqtt_client_instance = client_info['client']
    loop_thread = client_info.get('loop_thread')
    mqtt_user = client_info.get('mqtt_user', 'unknown_user')
    client_id_str = client_info.get('client_id') # This should be the actual client ID
    
    print(f"SID {session_id} (User: {mqtt_user}, CID: {client_id_str}): Cleaning up MQTT client...")
    try:
        if client_id_str and client_id_str in client_stop_events:
            print(f"SID {session_id} (CID: {client_id_str}): Setting stop event for MQTT loop.")
            client_stop_events[client_id_str].set() # Signal loop to stop

        # It's important to call disconnect to break out of loop_forever gracefully
        if mqtt_client_instance._state != mqtt.mqtt_cs_disconnecting and mqtt_client_instance._state != mqtt.mqtt_cs_disconnected :
            print(f"SID {session_id} (User: {mqtt_user}, CID: {client_id_str}): Calling MQTT disconnect(). Current state: {mqtt_client_instance._state}")
            mqtt_client_instance.disconnect() # This should make loop_forever exit
        else:
            print(f"SID {session_id} (User: {mqtt_user}, CID: {client_id_str}): MQTT client already disconnecting/disconnected. State: {mqtt_client_instance._state}")


        if loop_thread and loop_thread.is_alive():
            print(f"SID {session_id} (User: {mqtt_user}, CID: {client_id_str}): Waiting for MQTT loop thread {loop_thread.name} to join...")
            loop_thread.join(timeout=3.0) # Increased timeout slightly
            if loop_thread.is_alive():
                print(f"SID {session_id} (User: {mqtt_user}, CID: {client_id_str}): MQTT loop thread {loop_thread.name} did NOT join in time. Client might have been stuck.")
            else:
                print(f"SID {session_id} (User: {mqtt_user}, CID: {client_id_str}): MQTT loop thread {loop_thread.name} joined.")
        
        if client_id_str and client_id_str in client_stop_events: # Cleanup the event
            client_stop_events.pop(client_id_str, None)

    except Exception as e:
        print(f"SID {session_id} (User: {mqtt_user}, CID: {client_id_str}): Exception during MQTT client cleanup: {e}")
    
    print(f"SID {session_id} (User: {mqtt_user}, CID: {client_id_str}): MQTT client resources cleanup process completed.")


@socketio.on('subscribe_to_topic')
def handle_subscribe(data):
    session_id = request.sid
    if session_id not in mqtt_clients:
        socketio.emit('mqtt_status', {'message': 'Not logged in or MQTT client not active.'}, room=session_id); return
    
    mqtt_client = mqtt_clients[session_id]['client']
    topic = data.get('topic')

    subscription_qos = 2

    if topic and mqtt_client.is_connected():
        print(f"SID {session_id} (CID: {mqtt_clients[session_id]['client_id']}): Subscribing to {topic}")
        mqtt_client.subscribe(topic, qos=2)
    elif not mqtt_client.is_connected():
        socketio.emit('mqtt_status', {'message': 'MQTT not connected. Cannot subscribe.'}, room=session_id)
    elif not topic:
        socketio.emit('mqtt_status', {'message': 'Subscription topic cannot be empty.'}, room=session_id)


@socketio.on('publish_mqtt_message')
def handle_publish(data):
    session_id = request.sid
    if session_id not in mqtt_clients:
        socketio.emit('mqtt_status', {'message': 'Not logged in or MQTT client not active.'}, room=session_id); return

    client_info = mqtt_clients[session_id]
    mqtt_client = client_info['client']
    cid = client_info['client_id']
    
    topic = data.get('topic')
    payload = data.get('payload')
    qos = data.get('qos', 0)
    retain = data.get('retain', False)
    client_publish_time_ms = data.get('client_publish_time_ms')
    message_expiry_interval = data.get('message_expiry_interval')

    if not topic:
        socketio.emit('publish_ack', {'message': 'Publish topic cannot be empty.'}, room=session_id); return

    if mqtt_client.is_connected():
        publish_properties = props.Properties(PacketTypes.PUBLISH)
        user_props_list = []
        if client_publish_time_ms:
            user_props_list.append(('client_publish_time_ms', str(client_publish_time_ms)))
        
        if message_expiry_interval is not None:
            try:
                expiry_int = int(message_expiry_interval)
                if expiry_int > 0: # 0 means no expiry, so only set if > 0
                    publish_properties.MessageExpiryInterval = expiry_int
            except ValueError:
                print(f"SID {session_id} (CID: {cid}): Invalid Message Expiry Interval: {message_expiry_interval}")
        
        if user_props_list:
            publish_properties.UserProperty = user_props_list
        
        print(f"SID {session_id} (CID: {cid}): Publishing to {topic}. QoS:{qos}, Retain:{retain}, Expiry:{getattr(publish_properties, 'MessageExpiryInterval', 'None')}")
        mqtt_client.publish(topic, payload, qos=int(qos), retain=bool(retain), properties=publish_properties)
    else:
        socketio.emit('mqtt_status', {'message': 'MQTT not connected. Cannot publish.'}, room=session_id)


@socketio.on('send_mqtt_request')
def handle_mqtt_request(data):
    session_id = request.sid
    if session_id not in mqtt_clients:
        socketio.emit('mqtt_request_response_update', {'type': 'error', 'message': 'Not logged in.'}, room=session_id); return

    client_info = mqtt_clients[session_id]
    mqtt_client = client_info['client']
    client_id = client_info['client_id']
    pending_requests_for_client = client_info.get('pending_requests', {}) # Ensure it exists

    request_topic = data.get('request_topic')
    request_payload = data.get('request_payload', "") # Default to empty string
    
    if not request_topic:
        socketio.emit('mqtt_request_response_update', {'type': 'error', 'message': 'Request topic missing.'}, room=session_id); return
    if not mqtt_client.is_connected():
        socketio.emit('mqtt_request_response_update', {'type': 'error', 'message': 'MQTT not connected.'}, room=session_id); return

    correlation_id = str(uuid.uuid4())
    response_topic = f"clients/{client_id}/response/{correlation_id}" 

    request_properties = props.Properties(PacketTypes.PUBLISH)
    request_properties.ResponseTopic = response_topic
    request_properties.CorrelationData = correlation_id.encode('utf-8')

    try:
        mqtt_client.publish(request_topic, request_payload, qos=1, properties=request_properties)
        send_time_ms = time.time() * 1000
        pending_requests_for_client[correlation_id] = {'send_time_ms': send_time_ms, 'request_topic': request_topic, 'payload': request_payload}
        
        print(f"SID {session_id} (CID: {client_id}): Sent REQUEST to {request_topic} with CorrID {correlation_id}, expecting response on {response_topic}")
        socketio.emit('mqtt_request_response_update', {
            'type': 'request_sent', 'correlation_id': correlation_id, 'request_topic': request_topic,
            'response_topic_subscribed': response_topic, 'status': 'sent'
        }, room=session_id)

        def request_timeout_handler(sid, cid, corr_id, pending_reqs_ref): # Pass ref
            # Check if the request is still pending before declaring timeout
            if corr_id in pending_reqs_ref:
                print(f"SID {sid} (CID: {cid}): REQUEST TIMEOUT for Correlation ID {corr_id}")
                pending_reqs_ref.pop(corr_id) # Remove from original dict
                socketio.emit('mqtt_request_response_update', {
                    'type': 'timeout', 'correlation_id': corr_id, 'status': 'timeout'
                }, room=sid)
            else:
                print(f"SID {sid} (CID: {cid}): Timeout for CorrID {corr_id} triggered, but request already completed/cleared.")
        
        # Pass the actual pending_requests_for_client dictionary
        timeout_thread = threading.Timer(REQUEST_TIMEOUT_SECONDS, request_timeout_handler, 
                                         args=(session_id, client_id, correlation_id, pending_requests_for_client))
        timeout_thread.daemon = True
        timeout_thread.start()

    except Exception as e:
        print(f"SID {session_id} (CID: {client_id}): Error publishing MQTT request: {e}")
        socketio.emit('mqtt_request_response_update', {'type': 'error', 'message': f'Error sending request: {e}'}, room=session_id)


if __name__ == '__main__':
    print("Starting Flask-SocketIO server...")
    print(f"MQTT Broker Target: {BROKER_HOST}, Plain Port: {DEFAULT_MQTT_PORT}, MQTTS Port: {DEFAULT_MQTTS_PORT}")
    print(f"MQTT Keepalive set to: {MQTT_KEEPALIVE_SECONDS} seconds")
    print("Ensure Mosquitto broker is running and configured correctly.")
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True)