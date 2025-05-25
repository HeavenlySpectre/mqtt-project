import paho.mqtt.client as mqtt
import paho.mqtt.properties as props # For MQTTv5 properties
from paho.mqtt.packettypes import PacketTypes # For MQTTv5 properties
import time
import ssl
import uuid # For correlation IDs
import threading

# --- Configuration ---
BROKER_HOST = "localhost" # Or your broker's IP/hostname
MQTT_PORT = 1883
MQTTS_PORT = 8883 # For MQTTSecure

USERNAME = "kelompoke" # As configured in Mosquitto's password file
PASSWORD = "inites"

# For MQTTSecure (TLS)
CA_CERT_PATH = "C:/Program Files/mosquitto/certs/ca.crt" # Path to the CA certificate the client trusts
# Optional client certs if server's require_certificate is true
# CLIENT_CERT_PATH = "/path/to/client.crt"
# CLIENT_KEY_PATH = "/path/to/client.key"

CLIENT_ID_BASE = "python_mqtt_demo_client_"
UNIQUE_CLIENT_ID = f"{CLIENT_ID_BASE}{uuid.uuid4()}" # Ensure unique client ID

# --- Topics ---
BASE_TOPIC = "demo/features"
QOS_TOPIC_0 = f"{BASE_TOPIC}/qos0"
QOS_TOPIC_1 = f"{BASE_TOPIC}/qos1"
QOS_TOPIC_2 = f"{BASE_TOPIC}/qos2"
RETAIN_TOPIC = f"{BASE_TOPIC}/retained"
LWT_TOPIC = f"{BASE_TOPIC}/lwt/{UNIQUE_CLIENT_ID}"
EXPIRY_TOPIC = f"{BASE_TOPIC}/expiry"
REQUEST_TOPIC_PREFIX = f"{BASE_TOPIC}/request"
RESPONSE_TOPIC_PREFIX = f"{BASE_TOPIC}/response"

# --- Global State (for demo purposes) ---
pending_requests = {} # {correlation_id: (timestamp, callback_event)}
keep_running = True

# --- MQTT Callbacks ---
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"Connected successfully to broker with client ID: {client._client_id.decode()}")
        # Subscribe to topics of interest
        client.subscribe(QOS_TOPIC_0, qos=0)
        client.subscribe(QOS_TOPIC_1, qos=1)
        client.subscribe(QOS_TOPIC_2, qos=2)
        client.subscribe(RETAIN_TOPIC, qos=1) # To see retained messages
        client.subscribe(f"{REQUEST_TOPIC_PREFIX}/#", qos=1) # For request/response demo (as responder)
        # For request/response demo (as requester, subscribe to our unique response topic)
        client.subscribe(f"{RESPONSE_TOPIC_PREFIX}/{UNIQUE_CLIENT_ID}/#", qos=1)
        print("Subscribed to demo topics.")
    else:
        print(f"Connection failed with code {rc}")
        if properties:
            print(f"Reason String: {properties.ReasonString}")

def on_disconnect(client, userdata, rc, properties=None):
    print(f"Disconnected with result code {rc}")
    if rc != 0:
        print("Unexpected disconnection.")
    if properties:
        print(f"Reason String: {properties.ReasonString}")

def on_message(client, userdata, msg):
    payload_str = msg.payload.decode()
    print(f"Received message: Topic='{msg.topic}', QoS={msg.qos}, Retain={msg.retain}, Payload='{payload_str}'")

    # Request/Response Pattern: Check if this is a response to our request
    if msg.topic.startswith(f"{RESPONSE_TOPIC_PREFIX}/{UNIQUE_CLIENT_ID}/"):
        correlation_id = None
        if msg.properties: # MQTTv5
            for prop in msg.properties.UserProperty:
                if prop[0] == "correlation_id":
                    correlation_id = prop[1]
                    break
            if not correlation_id and msg.properties.CorrelationData: # Alternative
                 correlation_id = msg.properties.CorrelationData.decode()

        if correlation_id in pending_requests:
            print(f"Received response for correlation ID {correlation_id}: {payload_str}")
            _timestamp, event = pending_requests.pop(correlation_id)
            event.set() # Notify the waiting request sender
        else:
            print(f"Received unexpected response on {msg.topic} with correlation ID {correlation_id}")

    # Request/Response Pattern: Act as a responder for general requests
    elif msg.topic.startswith(REQUEST_TOPIC_PREFIX) and msg.topic != f"{REQUEST_TOPIC_PREFIX}/{UNIQUE_CLIENT_ID}":
        print(f"Received a request on {msg.topic}: {payload_str}")
        response_topic = None
        correlation_id = None

        if msg.properties: # MQTTv5
            response_topic = msg.properties.ResponseTopic
            if msg.properties.CorrelationData:
                correlation_id = msg.properties.CorrelationData.decode()
            # Or check UserProperty if CorrelationData isn't used for some reason
            if not correlation_id:
                 for prop in msg.properties.UserProperty:
                    if prop[0] == "correlation_id":
                        correlation_id = prop[1]
                        break

        if response_topic and correlation_id:
            response_payload = f"Response to your request: '{payload_str.upper()}'"
            response_properties = props.Properties(PacketTypes.PUBLISH)
            response_properties.CorrelationData = correlation_id.encode()
            # You could add UserProperties too
            # response_properties.UserProperty = [("source_responder", UNIQUE_CLIENT_ID)]

            print(f"Sending response to {response_topic} with Correlation ID {correlation_id}")
            client.publish(response_topic, response_payload, qos=1, properties=response_properties)
        else:
            print("Request message missing ResponseTopic or CorrelationData properties.")

def on_publish(client, userdata, mid):
    # This callback is called when a message with QoS > 0 has been successfully sent to the broker.
    # Not necessarily delivered to the subscriber.
    print(f"Message (MID: {mid}) published successfully.")

def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print(f"Subscribed (MID: {mid}) with granted QoS: {granted_qos}")
    if properties:
        print(f"Subscription properties: {properties}")

def on_log(client, userdata, level, buf):
    # Useful for debugging
    # print(f"Log: {buf}")
    pass

# --- Feature Demonstration Functions ---

def publish_qos_messages(client):
    print("\n--- Demonstrating QoS ---")
    client.publish(QOS_TOPIC_0, "Hello QoS 0", qos=0)
    client.publish(QOS_TOPIC_1, "Hello QoS 1", qos=1)
    client.publish(QOS_TOPIC_2, "Hello QoS 2", qos=2)
    time.sleep(1) # Allow time for on_publish callbacks

def publish_retained_message(client):
    print("\n--- Demonstrating Retained Message ---")
    # Publish a retained message. New subscribers to this topic will get it immediately.
    client.publish(RETAIN_TOPIC, "This is a retained message!", qos=1, retain=True)
    print(f"Published retained message to {RETAIN_TOPIC}. Disconnect and reconnect another client to see it.")
    # To clear a retained message, publish an empty payload with retain=True
    # client.publish(RETAIN_TOPIC, "", qos=1, retain=True)

def publish_with_message_expiry(client):
    print("\n--- Demonstrating Message Expiry (MQTTv5) ---")
    if client._protocol != mqtt.MQTTv5:
        print("Message Expiry requires MQTTv5 protocol. Skipping.")
        return

    expiry_interval_seconds = 10
    message_properties = props.Properties(PacketTypes.PUBLISH)
    message_properties.MessageExpiryInterval = expiry_interval_seconds

    payload = f"This message will expire in {expiry_interval_seconds} seconds."
    client.publish(EXPIRY_TOPIC, payload, qos=1, properties=message_properties)
    print(f"Published message to {EXPIRY_TOPIC} with expiry of {expiry_interval_seconds}s.")
    print(f"If no client subscribes and consumes it within {expiry_interval_seconds}s, the broker should discard it.")

def perform_request_response(client):
    print("\n--- Demonstrating Request/Response (MQTTv5) ---")
    if client._protocol != mqtt.MQTTv5:
        print("Request/Response with properties requires MQTTv5 protocol. Skipping.")
        return

    correlation_id = str(uuid.uuid4())
    request_payload = f"Request from {UNIQUE_CLIENT_ID} - data: {time.time()}"
    
    # The topic the responder should send the reply to
    my_response_topic = f"{RESPONSE_TOPIC_PREFIX}/{UNIQUE_CLIENT_ID}/{correlation_id}"
    # We need to be subscribed to this topic (done in on_connect)

    request_properties = props.Properties(PacketTypes.PUBLISH)
    request_properties.ResponseTopic = my_response_topic
    request_properties.CorrelationData = correlation_id.encode()
    # Can also add user properties
    # request_properties.UserProperty = [("app_version", "1.0")]

    # Store pending request
    response_event = threading.Event()
    pending_requests[correlation_id] = (time.time(), response_event)

    target_request_topic = f"{REQUEST_TOPIC_PREFIX}/serviceA" # Example target service
    client.publish(target_request_topic, request_payload, qos=1, properties=request_properties)
    print(f"Sent request to {target_request_topic} with Correlation ID: {correlation_id}, expecting response on {my_response_topic}")

    # Wait for response with a timeout
    if response_event.wait(timeout=10.0): # Wait for 10 seconds
        print(f"Request {correlation_id} completed.")
    else:
        print(f"Request {correlation_id} timed out.")
        if correlation_id in pending_requests:
            pending_requests.pop(correlation_id) # Clean up

# --- Main Application Logic ---
# --- Main Application Logic ---
def main():
    global keep_running
    print("MQTT Feature Demo Client")
    print("Select connection type:")
    print("1. MQTT (Plain, Port 1883)")
    print("2. MQTTS (Secure, Port 8883)")
    choice = input("Enter choice (1 or 2): ")

    use_tls = False
    port = MQTT_PORT
    if choice == '2':
        use_tls = True
        port = MQTTS_PORT

    # Initialize MQTT Client (Specify MQTTv5 for features like Message Expiry, Request/Response Properties)
    # REMOVED clean_session=True from here for MQTTv5
    client = mqtt.Client(client_id=UNIQUE_CLIENT_ID, protocol=mqtt.MQTTv5)

    # --- Authentication ---
    client.username_pw_set(USERNAME, PASSWORD)

    # --- Last Will and Testament (LWT) ---
    lwt_payload = f"Client {UNIQUE_CLIENT_ID} disconnected unexpectedly."
    client.will_set(LWT_TOPIC, payload=lwt_payload, qos=1, retain=False)
    print(f"LWT configured: Topic='{LWT_TOPIC}', Message='{lwt_payload}'")

    # --- MQTTSecure (TLS/SSL) ---
    if use_tls:
        # ... (TLS setup code remains the same) ...
        print(f"Configuring TLS/SSL using CA: {CA_CERT_PATH}")
        try:
            client.tls_set(
                ca_certs=CA_CERT_PATH,
                cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLS_CLIENT
            )
        except Exception as e:
            print(f"Error setting up TLS: {e}")
            return

    # Assign callbacks
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.on_publish = on_publish
    client.on_subscribe = on_subscribe
    client.on_log = on_log # For debugging

    # --- Connect to Broker ---
    # Keepalive (Ping Pong): The Paho client handles PINGREQ/PINGRESP automatically.
    # The keepalive interval (in seconds) tells the broker how often to expect a message (or PINGREQ)
    # from this client. If the broker doesn't hear from the client within 1.5 * keepalive,
    # it will consider the client disconnected (and send LWT if configured).
    # --- Connect to Broker ---
    keepalive_interval = 60
    print(f"Attempting to connect to {BROKER_HOST}:{port} with keepalive {keepalive_interval}s...")

    # For MQTTv5, session behavior is often controlled by SessionExpiryInterval in connect properties
    # For a behavior similar to clean_session=True (discard session on disconnect):
    connect_properties = props.Properties(PacketTypes.CONNECT)
    connect_properties.SessionExpiryInterval = 0 # Set to 0 for session to not persist after disconnect

    try:
        client.connect(BROKER_HOST, port, keepalive_interval, properties=connect_properties)
    except Exception as e:
        print(f"Connection error: {e}")
        return

    # Start the network loop in a separate thread.
    # This handles reconnections, PINGREQ/PINGRESP, and dispatching callbacks.
    client.loop_start()
    print("MQTT client loop started.")

    # Wait a moment for connection and subscriptions to establish
    time.sleep(2)
    if not client.is_connected():
        print("Failed to connect. Exiting.")
        client.loop_stop()
        return

    # --- Demonstrate Features ---
    try:
        while keep_running:
            print("\n--- Available Actions ---")
            print("1. Publish QoS test messages")
            print("2. Publish a retained message")
            print("3. Publish a message with expiry")
            print("4. Perform a Request/Response")
            print("5. Send simple message (topic: test/message, payload: 'hello from python')")
            print("6. Simulate flow control (publish many messages quickly)")
            print("0. Exit")
            action = input("Choose action: ")

            if action == '1':
                publish_qos_messages(client)
            elif action == '2':
                publish_retained_message(client)
            elif action == '3':
                publish_with_message_expiry(client)
            elif action == '4':
                perform_request_response(client)
            elif action == '5':
                client.publish(f"{BASE_TOPIC}/simple", "Hello from Python CLI", qos=1)
            elif action == '6':
                print("\n--- Demonstrating Flow Control (Client-Side Implications) ---")
                # Paho client has internal queues. If you publish faster than the network can send
                # or faster than on_publish confirms (for QoS 1/2), messages queue up.
                # `max_inflight_messages_set()`: Max QoS 1/2 messages awaiting PUBACK/PUBCOMP. Default 20.
                # `max_queued_messages_set()`: Max outgoing messages (any QoS) in queue. Default 0 (unlimited).
                # Setting these can prevent excessive memory use if publishing too fast.
                # client.max_inflight_messages_set(10)
                # client.max_queued_messages_set(100)
                # print("Set max_inflight_messages to 10, max_queued_messages to 100 (example)")
                print("Publishing 50 messages quickly (QoS 1)...")
                for i in range(50):
                    client.publish(f"{BASE_TOPIC}/flow_control_test", f"Flow message {i+1}", qos=1)
                    # time.sleep(0.01) # Add small delay to see effect or avoid overwhelming tiny brokers
                print("Done publishing. Check on_publish callbacks and broker load.")
                print("If publishing too fast for network/broker, messages queue in client or are dropped if queue limits hit.")

            elif action == '0':
                keep_running = False
            else:
                print("Invalid action.")
            
            if keep_running:
                time.sleep(1) # Give time for operations

    except KeyboardInterrupt:
        print("Keyboard interrupt received.")
    finally:
        print("Cleaning up and disconnecting...")
        keep_running = False
        # Publish one last message before graceful disconnect (not LWT)
        client.publish(f"{BASE_TOPIC}/status/{UNIQUE_CLIENT_ID}", "Client shutting down gracefully.", qos=0)
        time.sleep(0.5) # allow time for publish
        client.loop_stop() # Stop the network loop
        client.disconnect() # Gracefully disconnect
        print("Disconnected.")

if __name__ == "__main__":
    main()