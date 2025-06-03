# slow_subscriber.py
import paho.mqtt.client as mqtt
import time
import os

MQTT_BROKER = os.environ.get("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883)) # Ganti jika broker Anda di port lain
MQTT_USER_SUB = "kev" # Ganti dengan user MQTT yang valid di broker Anda
MQTT_PASS_SUB = "inikev" # Ganti dengan password yang sesuai
SUBSCRIBE_TOPIC = "test/flow_control_bulk" # Harus sama dengan yang di UI
DELAY_SECONDS = 1 # Seberapa lambat subscriber ini memproses

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"SlowSubscriber: Terhubung ke broker MQTT (kode: {rc})")
        client.subscribe(SUBSCRIBE_TOPIC, qos=1) # Subscribe dengan QoS 1
        print(f"SlowSubscriber: Subscribe ke topik '{SUBSCRIBE_TOPIC}'")
    else:
        print(f"SlowSubscriber: Gagal terhubung ke broker, kode: {rc}")

def on_message(client, userdata, msg):
    print(f"SlowSubscriber: Terima pesan '{msg.payload.decode()}' di topik '{msg.topic}' (QoS: {msg.qos})")
    print(f"SlowSubscriber: Mulai simulasi proses lambat ({DELAY_SECONDS} detik)...")
    time.sleep(DELAY_SECONDS) 
    print(f"SlowSubscriber: Selesai memproses pesan.")
    # Untuk QoS 1, Paho akan mengirim PUBACK setelah callback ini selesai.
    # Untuk QoS 2, Paho akan mengirim PUBREC, lalu menunggu PUBREL, lalu kirim PUBCOMP.

def on_disconnect(client, userdata, rc, properties=None):
    print(f"SlowSubscriber: Terputus dari broker dengan kode: {rc}")
    if rc != 0:
        print("SlowSubscriber: Mencoba menghubungkan ulang...")
        # Logika reconnect sederhana bisa ditambahkan di sini jika diinginkan
        # client.reconnect() # Paho biasanya menangani ini di loop_forever jika bukan disconnect bersih

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="slow_subscriber_py_test")
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

if MQTT_USER_SUB and MQTT_PASS_SUB:
    client.username_pw_set(MQTT_USER_SUB, MQTT_PASS_SUB)

print(f"SlowSubscriber: Mencoba terhubung ke {MQTT_BROKER}:{MQTT_PORT}")
try:
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()
except KeyboardInterrupt:
    print("SlowSubscriber: Dihentikan.")
    client.disconnect()
except Exception as e:
    print(f"SlowSubscriber: Kesalahan - {e}")