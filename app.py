from flask import Flask, render_template, request, session as flask_session
from flask_socketio import SocketIO, emit
import paho.mqtt.client as mqtt
import paho.mqtt.properties as props
from paho.mqtt.packettypes import PacketTypes
import uuid
import threading
import time
import os

# --- MQTT Configuration ---
BROKER_HOST = "localhost"
MQTT_PORT = 1883
MQTTS_PORT = 8883
CA_CERT_PATH = "C:/Program Files/mosquitto/certs/ca.crt"

USE_MQTTS = True
MQTT_KEEPALIVE_SECONDS = 3600

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'dev_default_mqtt_web_secret_key_123!')
socketio = SocketIO(app)
mqtt_clients = {} # Stores {'sid': {'client': obj, 'loop_thread': obj, 'mqtt_user': str}}

# --- on_connect_failure_event ---
# Used to signal the loop_forever thread to stop if initial connect fails badly (e.g. auth)
# We'll store an event per client instance
client_stop_events = {} 

def create_mqtt_client_for_session(session_id, mqtt_username, mqtt_password):
    client_id = f"web_{mqtt_username}_{session_id.replace('-', '')[:8]}"
    print(f"SID {session_id}: Creating MQTT client ID: {client_id} for MQTT user: {mqtt_username}")
    
    # Create a stop event for this client instance
    stop_event = threading.Event()
    client_stop_events[client_id] = stop_event # Store it by client_id

    mqtt_client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv5, callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
    mqtt_client.username_pw_set(mqtt_username, mqtt_password)
    mqtt_client.user_data_set({'sid': session_id, 'mqtt_user': mqtt_username, 'client_id': client_id}) # Add client_id to userdata

    def on_connect(client, userdata, flags, rc, properties=None):
        sid = userdata['sid']
        user = userdata['mqtt_user']
        cid = userdata['client_id']

        if rc == 0:
            print(f"SID {sid} (User: {user}, CID: {cid}): MQTT Client connected successfully.")
            socketio.emit('mqtt_status', {'message': f'MQTT Connected as {user}!'}, room=sid)
        else: # Connection failed
            reason = properties.ReasonString if properties and hasattr(properties, 'ReasonString') else f"rc: {rc}"
            print(f"SID {sid} (User: {user}, CID: {cid}): MQTT Client failed to connect. Reason: {reason}")
            socketio.emit('mqtt_status', {'message': f'MQTT Connection Failed for {user}: {reason}'}, room=sid)
            
            # Signal the loop_forever thread for this client to stop
            if cid in client_stop_events:
                print(f"SID {sid} (CID: {cid}): Signaling stop event due to connection failure.")
                client_stop_events[cid].set() 
            
            # Call cleanup. This will also attempt to stop the loop and remove client from mqtt_clients.
            # This is important to prevent loop_forever from retrying with bad credentials.
            handle_socket_disconnect(manual_sid=sid, reason=f"MQTT Connection Failed ({reason})")


    def on_disconnect(client, userdata, rc, properties=None):
        sid = userdata['sid']
        user = userdata['mqtt_user']
        cid = userdata['client_id']
        reason = properties.ReasonString if properties and hasattr(properties, 'ReasonString') else f"rc: {rc}"
        
        print(f"SID {sid} (User: {user}, CID: {cid}): MQTT Client disconnected. Reason: {reason}")
        socketio.emit('mqtt_status', {'message': f'MQTT Disconnected for {user}: {reason}'}, room=sid)

        # If the disconnect was not initiated by our stop_event (e.g., network issue after successful connect)
        # and the stop_event is not already set, we might want to let loop_forever try to reconnect.
        # However, if it was an auth failure on first connect, the stop_event should already be set.
        # The handle_socket_disconnect triggered by the browser tab closing will also clean up.
        # For now, this callback mainly informs the UI. The stop_event handles stopping persistent retries on initial auth fail.


    # on_message, on_publish, on_subscribe, on_log remain the same as before
    def on_message(client, userdata, msg): # Copied for completeness
        sid = userdata['sid']
        publish_time_ms = None
        if msg.properties and hasattr(msg.properties, 'UserProperty'):
            for key, value in msg.properties.UserProperty:
                if key == 'client_publish_time_ms':
                    try: publish_time_ms = float(value)
                    except ValueError: pass
                    break
        latency_ms = None
        if publish_time_ms:
            latency_ms = (time.time() * 1000) - publish_time_ms
        print(f"SID {sid}: MQTT Message on topic '{msg.topic}'. Latency: {latency_ms:.2f}ms" if latency_ms else "")
        socketio.emit('mqtt_message', {
            'topic': msg.topic, 'payload': msg.payload.decode(), 'qos': msg.qos,
            'retain': msg.retain, 'latency_ms': latency_ms
        }, room=sid)

    def on_publish(client, userdata, mid): # Copied for completeness
        sid = userdata['sid']
        print(f"SID {sid}: MQTT Message MID {mid} published (acknowledged by broker for QoS > 0).")
        socketio.emit('publish_ack', {'message': f'Message (MID: {mid}) confirmed by broker (QoS>0).'}, room=sid)
        
    def on_subscribe(client, userdata, mid, granted_qos, properties=None): # Copied for completeness
        sid = userdata['sid']
        granted_qos_str = [str(qos.value if hasattr(qos, 'value') else qos) for qos in granted_qos]
        print(f"SID {sid}: MQTT Subscribed MID {mid} with granted QoS: {granted_qos_str}")
        socketio.emit('mqtt_status', {'message': f'Subscribed (MID: {mid}, QoS: {", ".join(granted_qos_str)})'}, room=sid)

    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message
    mqtt_client.on_publish = on_publish
    mqtt_client.on_subscribe = on_subscribe
    # mqtt_client.on_log = ...

    current_port = MQTTS_PORT if USE_MQTTS else MQTT_PORT
    print(f"SID {session_id} (User: {mqtt_username}): Preparing to connect to MQTT at {BROKER_HOST}:{current_port} (MQTTS: {USE_MQTTS})")

    if USE_MQTTS:
        try:
            mqtt_client.tls_set(ca_certs=CA_CERT_PATH)
        except Exception as e:
            print(f"SID {session_id} (User: {mqtt_username}): Error setting up TLS: {e}")
            socketio.emit('mqtt_status', {'message': f'TLS Setup Error: {e}'}, room=session_id)
            if client_id in client_stop_events: client_stop_events.pop(client_id, None) # Clean up stop event
            return None

    connect_properties = props.Properties(PacketTypes.CONNECT)
    connect_properties.SessionExpiryInterval = 0

    try:
        mqtt_client.connect_async(BROKER_HOST, current_port, MQTT_KEEPALIVE_SECONDS, properties=connect_properties)
        print(f"SID {session_id} (User: {mqtt_username}): MQTT connect_async initiated.")
    except Exception as e:
        print(f"SID {session_id} (User: {mqtt_username}): MQTT connect_async error: {e}")
        socketio.emit('mqtt_status', {'message': f'MQTT Connection Initiation Error: {e}'}, room=session_id)
        if client_id in client_stop_events: client_stop_events.pop(client_id, None) # Clean up stop event
        return None
        
    return mqtt_client

# Custom loop_forever target that respects a stop event
def mqtt_loop_with_stop_event(client_instance, client_id_for_event):
    print(f"MQTTLoop (CID: {client_id_for_event}): Starting loop_forever...")
    try:
        # loop_forever will block here. It has its own reconnect logic.
        # We need our stop_event to break out of it if the initial connect fails due to auth.
        # Paho's loop_forever doesn't directly take a stop event.
        # One way is that on_connect failure sets the event, and then the client disconnects itself.
        # The loop should then exit.
        
        # A more direct check could be:
        # while not client_stop_events.get(client_id_for_event, threading.Event()).is_set():
        #    rc = client_instance.loop(timeout=1.0) # Use non-blocking loop
        #    if rc != mqtt.MQTT_ERR_SUCCESS:
        #        break # Or handle error
        # client_instance.disconnect() # Ensure disconnect if loop broken by event

        # For now, relying on on_connect to trigger cleanup via handle_socket_disconnect
        # which should lead to client.disconnect() being called, stopping loop_forever.
        client_instance.loop_forever()

    except Exception as e:
        print(f"MQTTLoop (CID: {client_id_for_event}): Exception: {e}")
    finally:
        print(f"MQTTLoop (CID: {client_id_for_event}): Exited.")
        # Clean up the stop event for this client if it exists
        if client_id_for_event in client_stop_events:
            client_stop_events.pop(client_id_for_event, None)


@app.route('/')
def index_route():
    return render_template('index.html')

@socketio.on('connect')
def handle_socket_connect():
    session_id = request.sid
    print(f"Socket.IO client connected: SID {session_id}. Waiting for web login.")
    socketio.emit('request_login', {'message': 'Please provide MQTT credentials.'}, room=session_id)

@socketio.on('web_login')
def handle_web_login(data):
    session_id = request.sid
    mqtt_username = data.get('username')
    mqtt_password = data.get('password')

    if not mqtt_username or not mqtt_password:
        socketio.emit('login_status', {'success': False, 'message': 'Username and password are required.'}, room=session_id)
        return

    print(f"SID {session_id}: Received web login for MQTT user: {mqtt_username}")

    if session_id in mqtt_clients:
        print(f"SID {session_id}: Cleaning up previous MQTT client for re-login by user {mqtt_clients[session_id].get('mqtt_user','N/A')}.")
        handle_socket_disconnect(manual_sid=session_id, reason="Re-login attempt by new user or same user")

    socketio.emit('mqtt_status', {'message': f'Attempting MQTT connection for {mqtt_username}...'}, room=session_id)
    mqtt_client_instance = create_mqtt_client_for_session(session_id, mqtt_username, mqtt_password)
    
    if mqtt_client_instance:
        client_actual_id = mqtt_client_instance._client_id.decode() # Get the actual client ID used
        # Pass the actual client_id used by paho, not just the one we constructed for the event key
        loop_thread = threading.Thread(target=mqtt_loop_with_stop_event, args=(mqtt_client_instance, client_actual_id), daemon=True)
        loop_thread.name = f"MQTTLoop_{client_actual_id[:12]}"
        loop_thread.start()
        
        mqtt_clients[session_id] = {
            'client': mqtt_client_instance, 
            'loop_thread': loop_thread,
            'mqtt_user': mqtt_username,
            'client_id': client_actual_id # Store the actual client ID
        }
        socketio.emit('login_status', {'success': True, 'username': mqtt_username}, room=session_id)
        print(f"SID {session_id}: MQTT client for {mqtt_username} (CID: {client_actual_id}) created and network loop started.")
    else:
        print(f"SID {session_id}: Failed to create/initiate MQTT client for {mqtt_username}.")
        socketio.emit('login_status', {'success': False, 'message': 'Failed to initialize MQTT client.'}, room=session_id)


def handle_socket_disconnect(manual_sid=None, reason="Unknown"):
    session_id = manual_sid if manual_sid else request.sid
    
    print(f"Socket.IO client disconnect event for SID {session_id}. Reason: {reason}")
    client_info = mqtt_clients.pop(session_id, None) # Use pop with default
    
    if not client_info:
        print(f"SID {session_id}: No active MQTT client info found for cleanup (might have been cleaned already).")
        return

    mqtt_client_instance = client_info['client']
    loop_thread = client_info.get('loop_thread')
    mqtt_user = client_info.get('mqtt_user', 'unknown_user')
    client_id_for_event = client_info.get('client_id', None)
    
    print(f"SID {session_id} (User: {mqtt_user}, CID: {client_id_for_event}): Cleaning up MQTT client...")
    try:
        # Signal the loop_forever thread to stop if it's managed by our stop event
        if client_id_for_event and client_id_for_event in client_stop_events:
            print(f"SID {session_id} (CID: {client_id_for_event}): Setting stop event for MQTT loop.")
            client_stop_events[client_id_for_event].set()

        # Disconnect the client. This should make loop_forever exit.
        if mqtt_client_instance.is_connected():
            mqtt_client_instance.disconnect()
            print(f"SID {session_id} (User: {mqtt_user}): MQTT disconnect() called.")
        else:
            # If not connected, calling disconnect might not be necessary or could error
            # loop_stop() is non-blocking and might be better here if loop_forever hasn't fully exited
            print(f"SID {session_id} (User: {mqtt_user}): MQTT client was not connected. Attempting loop_stop().")
            mqtt_client_instance.loop_stop() # REMOVED 'force' ARGUMENT

        if loop_thread and loop_thread.is_alive():
            print(f"SID {session_id} (User: {mqtt_user}): Waiting for MQTT loop thread {loop_thread.name} to join...")
            loop_thread.join(timeout=2.0)
            if loop_thread.is_alive():
                print(f"SID {session_id} (User: {mqtt_user}): MQTT loop thread {loop_thread.name} did NOT join in time.")
            else:
                print(f"SID {session_id} (User: {mqtt_user}): MQTT loop thread {loop_thread.name} joined.")
        
        # Clean up the stop event if it exists
        if client_id_for_event and client_id_for_event in client_stop_events:
            client_stop_events.pop(client_id_for_event, None)

    except Exception as e:
        print(f"SID {session_id} (User: {mqtt_user}): Exception during MQTT client cleanup: {e}")
    
    print(f"SID {session_id} (User: {mqtt_user}): MQTT client resources cleanup process completed.")


# 'subscribe_to_topic' and 'publish_mqtt_message' remain largely the same,
# just ensure they check `session_id in mqtt_clients` before proceeding.
@socketio.on('subscribe_to_topic')
def handle_subscribe(data): # Copied for completeness
    session_id = request.sid
    if session_id not in mqtt_clients:
        socketio.emit('mqtt_status', {'message': 'Not logged in or MQTT client not active.'}, room=session_id)
        return
    mqtt_client = mqtt_clients[session_id]['client']
    topic = data.get('topic')
    if topic and mqtt_client.is_connected():
        mqtt_client.subscribe(topic, qos=1)
    elif not mqtt_client.is_connected():
        socketio.emit('mqtt_status', {'message': 'MQTT not connected.'}, room=session_id)

@socketio.on('publish_mqtt_message')
def handle_publish(data): # Copied for completeness
    session_id = request.sid
    if session_id not in mqtt_clients:
        socketio.emit('mqtt_status', {'message': 'Not logged in or MQTT client not active.'}, room=session_id)
        return
    mqtt_client = mqtt_clients[session_id]['client']
    topic = data.get('topic')
    payload = data.get('payload')
    qos = data.get('qos', 0)
    retain = data.get('retain', False)
    client_publish_time_ms = data.get('client_publish_time_ms')
    if mqtt_client.is_connected() and topic is not None and payload is not None:
        publish_properties = props.Properties(PacketTypes.PUBLISH)
        if client_publish_time_ms:
            publish_properties.UserProperty = [('client_publish_time_ms', str(client_publish_time_ms))]
        mqtt_client.publish(topic, payload, qos=int(qos), retain=bool(retain), properties=publish_properties)
    elif not mqtt_client.is_connected():
        socketio.emit('mqtt_status', {'message': 'MQTT not connected. Cannot publish.'}, room=session_id)


if __name__ == '__main__':
    # ... (startup messages as before) ...
    print("Starting Flask-SocketIO server...")
    print(f"MQTT Broker Target: {BROKER_HOST}, Plain Port: {MQTT_PORT}, MQTTS Port: {MQTTS_PORT}")
    print(f"Using MQTTS: {USE_MQTTS}")
    print(f"MQTT Keepalive set to: {MQTT_KEEPALIVE_SECONDS} seconds")
    print("Ensure Mosquitto broker is running and configured correctly (auth, TLS if MQTTS=True).")
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True)