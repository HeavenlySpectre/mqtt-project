from flask import Flask, render_template, request, redirect, url_for, session, flash
from flask_socketio import SocketIO, emit, join_room, leave_room
import paho.mqtt.client as mqtt
import paho.mqtt.properties as props
import paho.mqtt.subscribeoptions as subopts 
from paho.mqtt.packettypes import PacketTypes
import uuid
import threading
import time
import os
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
import csv 

# --- Konfigurasi Web User (Simpan di CSV) ---
WEB_USERS_FILE = 'web_app_users.csv'
# Field CSV: id,username,password_hash

def load_web_users_from_csv(): 
    users = {}
    next_id = 1
    try:
        with open(WEB_USERS_FILE, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                users[row['username']] = {'id': int(row['id']), 'password_hash': row['password_hash']}
                if int(row['id']) >= next_id:
                    next_id = int(row['id']) + 1
    except FileNotFoundError:
        with open(WEB_USERS_FILE, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['id', 'username', 'password_hash'])
        print(f"Berkas pengguna '{WEB_USERS_FILE}' tidak ditemukan, telah dibuat.")
    except Exception as e:
        print(f"Kesalahan saat memuat pengguna dari CSV: {e}")
    return users, next_id

def save_web_user_to_csv(user_id, username, password_hash): 
    try:
        fieldnames = ['id', 'username', 'password_hash']
        rows = []
        user_exists = False
        if os.path.exists(WEB_USERS_FILE) and os.path.getsize(WEB_USERS_FILE) > 0 : # Pastikan file ada dan tidak kosong
            with open(WEB_USERS_FILE, mode='r', newline='') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if row['username'] == username:
                        user_exists = True 
                    rows.append(row)
        
        if not user_exists: 
            # Tulis header jika file baru (atau kosong setelah dibuat)
            write_header = not (os.path.exists(WEB_USERS_FILE) and os.path.getsize(WEB_USERS_FILE) > 0)
            with open(WEB_USERS_FILE, mode='a', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                if write_header: # Hanya tulis header jika file benar-benar baru/kosong
                    # Fungsi load_web_users_from_csv sudah menangani pembuatan header jika file tidak ada.
                    # Cek ini untuk kasus file ada tapi kosong (jarang terjadi jika load benar)
                    if not rows: # Jika file ada tapi kosong setelah dibaca
                         writer.writeheader()
                writer.writerow({'id': user_id, 'username': username, 'password_hash': password_hash})
            return True
        else:
            print(f"Peringatan: Nama pengguna {username} sudah ada di CSV saat mencoba menyimpan.")
            return False 
    except Exception as e:
        print(f"Kesalahan saat menyimpan pengguna ke CSV: {e}")
        return False

web_users, next_user_id = load_web_users_from_csv() 

# --- Konfigurasi MQTT ---
BROKER_HOST = "localhost"
DEFAULT_MQTT_PORT = 1883
DEFAULT_MQTTS_PORT = 8883
DEFAULT_MQTT_WS_PORT = 8080 
DEFAULT_MQTTS_WS_PORT = 8081 
CA_CERT_PATH = "C:/Program Files/mosquitto/certs/ca.crt" 

# --- Konfigurasi Web App ---
MQTT_KEEPALIVE_SECONDS = 3600 
REQUEST_TIMEOUT_SECONDS = 10 
PERFORMANCE_UPDATE_INTERVAL_SECONDS = 5 
MAX_LATENCIES_TO_STORE = 50 

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'a_very_secure_web_app_secret_!@#$') 
if app.config['SECRET_KEY'] == 'a_very_secure_web_app_secret_!@#$':
    print("PERINGATAN: KUNCI RAHASIA APLIKASI WEB default.") 
    print("Gunakan kunci yang kuat untuk produksi.") 

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'web_login_page' 
login_manager.login_message = "Anda harus login untuk mengakses halaman ini."
login_manager.login_message_category = "info" 

class WebUser(UserMixin): 
    def __init__(self, id, username):
        self.id = id
        self.username = username

@login_manager.user_loader
def load_user(user_id): 
    for username_from_dict, data in web_users.items(): 
        if str(data['id']) == str(user_id):
            return WebUser(id=data['id'], username=username_from_dict)
    return None

socketio = SocketIO(app, async_mode='threading') 

mqtt_clients = {} 
client_stop_events = {} 

def get_mqtt_client_key(web_user_id, socket_io_sid): 
    return f"user_{web_user_id}_sid_{socket_io_sid}"


def create_mqtt_client_for_session(mqtt_client_key, web_username_for_log, mqtt_username, mqtt_password, use_mqtts, lwt_options=None,
                                   transport_protocol="tcp", ws_path="/mqtt", broker_port=None,
                                   connect_topic_alias_max_val=None):
    unique_part = mqtt_client_key.split('_sid_')[-1][:8] 
    client_id_str = f"web_{mqtt_username[:10]}_{unique_part}" 
    
    print(f"KUNCI {mqtt_client_key} (PenggunaWeb: {web_username_for_log}): Buat klien MQTT. ID: {client_id_str}, PenggunaMQTT: {mqtt_username}")
    
    stop_event = threading.Event()
    
    mqtt_client = mqtt.Client(client_id=client_id_str, protocol=mqtt.MQTTv5,
                              callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
                              transport=transport_protocol) 
    
    actual_client_id = mqtt_client._client_id.decode()
    
    if actual_client_id != client_id_str:
        print(f"KUNCI {mqtt_client_key}: Paho sesuaikan ID Klien MQTT ke '{actual_client_id}'.")
    client_stop_events[actual_client_id] = stop_event 

    mqtt_client.username_pw_set(mqtt_username, mqtt_password) 
    mqtt_client.user_data_set({
        'mqtt_client_key': mqtt_client_key, 
        'web_user_for_log': web_username_for_log, 
        'mqtt_user': mqtt_username, 
        'client_id': actual_client_id, 
        'pending_requests': {}
    })

    if transport_protocol == "websockets":
        mqtt_client.ws_set_options(path=ws_path) 
        print(f"KUNCI {mqtt_client_key}: Path WebSocket: {ws_path}")


    if lwt_options and lwt_options.get('topic') and lwt_options.get('payload') is not None:
        try:
            lwt_topic = lwt_options['topic']
            lwt_payload = lwt_options['payload']
            lwt_qos = int(lwt_options.get('qos', 0))
            lwt_retain = bool(lwt_options.get('retain', False))
            mqtt_client.will_set(lwt_topic, lwt_payload, lwt_qos, lwt_retain)
            print(f"KUNCI {mqtt_client_key} (CID: {actual_client_id}): LWT dikonfigurasi '{lwt_topic}'")
        except Exception as e:
            print(f"KUNCI {mqtt_client_key} (CID: {actual_client_id}): Kesalahan LWT: {e}")

    def on_connect(client, userdata, flags, rc, properties=None):
        key = userdata['mqtt_client_key']
        web_user_log = userdata['web_user_for_log']
        mqtt_user_log = userdata['mqtt_user']
        cid = userdata['client_id']
        client_session_info = mqtt_clients.get(key)

        if rc == 0:
            print(f"KUNCI {key} (Web: {web_user_log}, MQTT: {mqtt_user_log}, CID: {cid}): Klien MQTT terhubung.")
            socketio.emit('mqtt_status', {'message': f'MQTT Terhubung sebagai {mqtt_user_log} (CID: {cid})'}, room=key)
            
            response_topic_subscription = f"clients/{cid}/response/#"
            client.subscribe(response_topic_subscription, qos=1) 
            print(f"KUNCI {key} (CID: {cid}): Auto-subscribe topik respon: {response_topic_subscription}")

            direct_request_topic_subscription = f"clients/{cid}/direct_request/#"
            client.subscribe(direct_request_topic_subscription, qos=1)
            print(f"KUNCI {key} (CID: {cid}): Auto-subscribe direct request: {direct_request_topic_subscription}")

            if properties and hasattr(properties, 'TopicAliasMaximum'):
                broker_tam = properties.TopicAliasMaximum
                print(f"KUNCI {key} (CID: {cid}): Broker TopicAliasMaximum: {broker_tam}")
                if client_session_info: client_session_info['broker_topic_alias_max'] = broker_tam
        else:
            reason = properties.ReasonString if properties and hasattr(properties, 'ReasonString') else f"kode: {rc}"
            print(f"KUNCI {key} (Web: {web_user_log}, MQTT: {mqtt_user_log}, CID: {cid}): MQTT gagal terhubung. Alasan: {reason}")
            socketio.emit('mqtt_status', {'message': f'Koneksi MQTT Gagal untuk {mqtt_user_log}: {reason}'}, room=key)
            if client_session_info: client_session_info['performance_data']['connect_errors'] += 1
            if cid in client_stop_events:
                client_stop_events[cid].set() 
            cleanup_mqtt_resources(mqtt_client_key_to_clean=key, reason=f"MQTT Gagal ({reason})")


    def on_disconnect(client, userdata, rc, properties=None):
        key = userdata['mqtt_client_key']
        web_user_log = userdata['web_user_for_log']
        mqtt_user_log = userdata['mqtt_user']
        cid = userdata['client_id']
        reason = properties.ReasonString if properties and hasattr(properties, 'ReasonString') else f"kode: {rc}"
        print(f"KUNCI {key} (Web: {web_user_log}, MQTT: {mqtt_user_log}, CID: {cid}): MQTT terputus. Alasan: {reason}")
        socketio.emit('mqtt_status', {'message': f'MQTT Terputus untuk {mqtt_user_log}: {reason}'}, room=key)
        if rc != 0:
            client_session_info = mqtt_clients.get(key)
            if client_session_info:
                if not client_session_info['performance_data']['connect_errors'] > 0 or reason != f"kode: {rc}": 
                    client_session_info['performance_data']['connect_errors'] += 1


    def on_message(client, userdata, msg):
        key = userdata.get('mqtt_client_key')
        cid = userdata.get('client_id') 
        web_user_log = userdata.get('web_user_for_log') # Untuk logging jika perlu
        mqtt_user_log = userdata.get('mqtt_user') # Untuk logging jika perlu

        if not key or not cid: 
            print(f"ON_MESSAGE: Userdata hilang key/cid. Topik: {msg.topic}.")
            return

        client_session_info = mqtt_clients.get(key)
        if not client_session_info:
            print(f"KUNCI {key} (CID: {cid}): ON_MESSAGE - Sesi klien tidak ada. Abaikan {msg.topic}.")
            return
        
        pending_requests_for_client = client_session_info.get('pending_requests')
        if pending_requests_for_client is None: # Seharusnya tidak terjadi jika inisialisasi benar
            print(f"KUNCI {key} (CID: {cid}): ON_MESSAGE - KRITIS - 'pending_requests' hilang! Inisialisasi ulang.")
            client_session_info['pending_requests'] = {} 
            pending_requests_for_client = client_session_info['pending_requests']
        
        perf_data = client_session_info['performance_data']
        perf_data['msg_in_count'] += 1
        perf_data['bytes_in_count'] += len(msg.payload)

        actual_topic = msg.topic
        if msg.properties and hasattr(msg.properties, 'TopicAlias') and msg.properties.TopicAlias is not None:
            print(f"KUNCI {key} (CID: {cid}): Pesan diterima dengan Topic Alias: {msg.properties.TopicAlias} di topik '{msg.topic}'")

        response_prefix_for_this_client = f"clients/{cid}/response/"
        direct_request_prefix_for_this_client = f"clients/{cid}/direct_request/"
        
        is_a_response_to_us = False
        is_a_direct_request_to_us_and_echoed = False
        is_a_general_request_we_can_echo = False

        if actual_topic.startswith(response_prefix_for_this_client):
            is_a_response_to_us = True
            correlation_id_from_topic = actual_topic.split('/')[-1]
            correlation_id_from_props = None
            if msg.properties and msg.properties.CorrelationData:
                try:
                    correlation_id_from_props = msg.properties.CorrelationData.decode('utf-8')
                except Exception as e:
                    print(f"KUNCI {key} (CID: {cid}): ON_MESSAGE - Kesalahan decode CorrelationData: {e}")
            
            final_correlation_id = correlation_id_from_props if correlation_id_from_props else correlation_id_from_topic

            if final_correlation_id in pending_requests_for_client:
                request_info = pending_requests_for_client.pop(final_correlation_id)
                send_time_ms = request_info['send_time_ms']
                latency_ms = (time.time() * 1000) - send_time_ms
                
                print(f"KUNCI {key} (CID: {cid}): ON_MESSAGE - RESPON diterima CorrID {final_correlation_id}. Latensi: {latency_ms:.2f}ms.")
                
                socketio.emit('mqtt_request_response_update', {
                    'type': 'response', 'correlation_id': final_correlation_id, 'topic': actual_topic,
                    'payload': msg.payload.decode('utf-8', errors='replace'), 'latency_ms': latency_ms, 'status': 'completed'
                }, room=key)
            else:
                print(f"KUNCI {key} (CID: {cid}): ON_MESSAGE - Respon CorrID '{final_correlation_id}' tidak ditemukan atau sudah kadaluarsa.")


        if not is_a_response_to_us and actual_topic.startswith(direct_request_prefix_for_this_client):
            print(f"KUNCI {key} (CID: {cid}): ON_MESSAGE - DIRECT REQUEST diterima di {actual_topic}")
            response_topic_from_req = None
            correlation_data_from_req = None 

            if msg.properties:
                if hasattr(msg.properties, 'ResponseTopic') and msg.properties.ResponseTopic:
                    response_topic_from_req = msg.properties.ResponseTopic
                if hasattr(msg.properties, 'CorrelationData') and msg.properties.CorrelationData:
                    correlation_data_from_req = msg.properties.CorrelationData

            if response_topic_from_req and correlation_data_from_req:
                is_a_direct_request_to_us_and_echoed = True 
                try:
                    response_payload = f"DIRECT ECHO DARI {cid}: {msg.payload.decode('utf-8', errors='replace').upper()}"
                    response_props = props.Properties(PacketTypes.PUBLISH)
                    response_props.CorrelationData = correlation_data_from_req 
                    
                    client.publish(response_topic_from_req, response_payload, qos=1, properties=response_props)
                    print(f"KUNCI {key} (CID: {cid}): ON_MESSAGE - Kirim DIRECT ECHO RESPONSE ke {response_topic_from_req} untuk request di {actual_topic}")
                except Exception as e:
                    print(f"KUNCI {key} (CID: {cid}): ON_MESSAGE - Kesalahan kirim direct echo response: {e}")
            else:
                print(f"KUNCI {key} (CID: {cid}): ON_MESSAGE - Direct request di {actual_topic} tanpa ResponseTopic/CorrelationData. Tidak bisa di-echo.")


        if not is_a_response_to_us and not is_a_direct_request_to_us_and_echoed:
            response_topic_from_req = None
            correlation_data_from_req = None 

            if msg.properties:
                if hasattr(msg.properties, 'ResponseTopic') and msg.properties.ResponseTopic:
                    response_topic_from_req = msg.properties.ResponseTopic
                if hasattr(msg.properties, 'CorrelationData') and msg.properties.CorrelationData:
                    correlation_data_from_req = msg.properties.CorrelationData

            if response_topic_from_req and correlation_data_from_req:
                if not actual_topic.startswith(f"clients/{cid}/request/"): 
                    is_a_general_request_we_can_echo = True
                    print(f"KUNCI {key} (CID: {cid}): ON_MESSAGE - GENERAL REQUEST diterima di {actual_topic}, coba echo.")
                    try:
                        response_payload = f"GENERAL ECHO DARI {cid}: {msg.payload.decode('utf-8', errors='replace').upper()}"
                        response_props = props.Properties(PacketTypes.PUBLISH)
                        response_props.CorrelationData = correlation_data_from_req
                        client.publish(response_topic_from_req, response_payload, qos=1, properties=response_props)
                        print(f"KUNCI {key} (CID: {cid}): ON_MESSAGE - Kirim GENERAL ECHO RESPONSE ke {response_topic_from_req} untuk request di {actual_topic}")
                    except Exception as e:
                        print(f"KUNCI {key} (CID: {cid}): ON_MESSAGE - Kesalahan kirim general echo response: {e}")
            
        if not is_a_response_to_us and not is_a_direct_request_to_us_and_echoed and not is_a_general_request_we_can_echo:
            publish_time_ms = None
            if msg.properties and hasattr(msg.properties, 'UserProperty'):
                for prop_key, value in msg.properties.UserProperty: # Ganti nama variabel agar tidak bentrok
                    if prop_key == 'client_publish_time_ms':
                        try: publish_time_ms = float(value); break
                        except ValueError: 
                            print(f"KUNCI {key} (CID: {cid}): ON_MESSAGE - Kesalahan parsing client_publish_time_ms: {value}")
            
            latency_ms = ((time.time() * 1000) - publish_time_ms) if publish_time_ms is not None else None
            if latency_ms is not None:
                latencies = perf_data['latencies_ms']
                latencies.append(latency_ms)
                if len(latencies) > MAX_LATENCIES_TO_STORE:
                    perf_data['latencies_ms'] = latencies[-MAX_LATENCIES_TO_STORE:]

            print(f"KUNCI {key} (CID: {cid}): ON_MESSAGE - Pesan MQTT Standar. Topik: '{actual_topic}'. Latensi: {latency_ms:.2f}ms" if latency_ms is not None else f"KUNCI {key} (CID: {cid}): ON_MESSAGE - Pesan MQTT Standar. Topik: '{actual_topic}' (tanpa info latensi klien)")
            socketio.emit('mqtt_message', {
                'topic': actual_topic, 
                'payload': msg.payload.decode('utf-8', errors='replace'),
                'qos': msg.qos, 'retain': msg.retain, 'latency_ms': latency_ms,
                'topic_alias_used': msg.properties.TopicAlias if msg.properties and hasattr(msg.properties, 'TopicAlias') else None
            }, room=key)


    def on_publish(client, userdata, mid):
        key = userdata['mqtt_client_key']
        print(f"KUNCI {key} (CID: {userdata['client_id']}): Pesan MQTT MID {mid} terpublish (ack broker untuk QoS > 0).")
        socketio.emit('publish_ack', {'message': f'Pesan (MID: {mid}) dikonfirmasi oleh broker (QoS>0).'}, room=key)
        
    def on_subscribe(client, userdata, mid, granted_qos, properties=None):
        key = userdata['mqtt_client_key']
        granted_qos_str = [str(qos.value if hasattr(qos, 'value') else qos) for qos in granted_qos]
        print(f"KUNCI {key} (CID: {userdata['client_id']}): MQTT Subscribe MID {mid} dengan granted QoS: {granted_qos_str}")
        socketio.emit('mqtt_status', {'message': f'Berhasil subscribe (MID: {mid}, QoS: {", ".join(granted_qos_str)})'}, room=key)
        
        client_session_info = mqtt_clients.get(key)
        if client_session_info:
            for qos_val in granted_qos:
                if isinstance(qos_val, int) and qos_val >= 0x80: 
                    client_session_info['performance_data']['subscribe_failures'] += 1
                    print(f"KUNCI {key} (CID: {userdata['client_id']}): Subscribe gagal, QoS dari broker: {qos_val}")
                    break


    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message
    mqtt_client.on_publish = on_publish
    mqtt_client.on_subscribe = on_subscribe

    if use_mqtts: 
        try:
            mqtt_client.tls_set(ca_certs=CA_CERT_PATH) 
        except Exception as e:
            print(f"KUNCI {mqtt_client_key} (CID: {actual_client_id}): Kesalahan setup TLS: {e}")
            socketio.emit('mqtt_status', {'message': f'Kesalahan Setup TLS: {e}'}, room=mqtt_client_key)
            if actual_client_id in client_stop_events: client_stop_events.pop(actual_client_id, None)
            return None

    connect_properties = props.Properties(PacketTypes.CONNECT)
    connect_properties.SessionExpiryInterval = 0 
    if connect_topic_alias_max_val is not None:
        try:
            val = int(connect_topic_alias_max_val)
            if val >= 0: connect_properties.TopicAliasMaximum = val 
            print(f"KUNCI {mqtt_client_key}: Atur TopicAliasMaximum CONNECT: {val}")
        except ValueError:
            print(f"KUNCI {mqtt_client_key}: Nilai TopicAliasMaximum salah: {connect_topic_alias_max_val}")

    try:
        mqtt_client.connect_async(BROKER_HOST, broker_port, MQTT_KEEPALIVE_SECONDS, properties=connect_properties)
        print(f"KUNCI {mqtt_client_key} (CID: {actual_client_id}): MQTT connect_async ke {BROKER_HOST}:{broker_port} dimulai.")
    except Exception as e:
        print(f"KUNCI {mqtt_client_key} (CID: {actual_client_id}): Kesalahan connect_async: {e}")
        socketio.emit('mqtt_status', {'message': f'Kesalahan Inisiasi Koneksi MQTT: {e}'}, room=mqtt_client_key)
        if actual_client_id in client_stop_events: client_stop_events.pop(actual_client_id, None)
        return None
            
    return mqtt_client

def mqtt_loop_with_stop_event(client_instance, client_id_for_event_key):
    print(f"MQTTLoop (CID: {client_id_for_event_key}): Memulai loop...")
    stop_event_instance = client_stop_events.get(client_id_for_event_key)

    if not stop_event_instance:
        print(f"MQTTLoop (CID: {client_id_for_event_key}): KESALAHAN - Stop event tidak ditemukan. Loop tidak akan dimulai.")
        return

    try:
        client_instance.loop_forever(retry_first_connection=False) 
    except Exception as e:
        print(f"MQTTLoop (CID: {client_id_for_event_key}): Exception: {e}")
    finally:
        print(f"MQTTLoop (CID: {client_id_for_event_key}): Keluar.")
        if client_id_for_event_key in client_stop_events and client_stop_events[client_id_for_event_key] == stop_event_instance:
            client_stop_events.pop(client_id_for_event_key, None)

def cleanup_mqtt_resources(mqtt_client_key_to_clean, reason="Tidak diketahui"):
    print(f"CLEANUP: Memproses untuk Kunci MQTT {mqtt_client_key_to_clean}. Alasan: {reason}")
    
    client_info = mqtt_clients.pop(mqtt_client_key_to_clean, None)
    if not client_info:
        return

    mqtt_client_instance = client_info['client']
    loop_thread = client_info.get('loop_thread')
    web_user_log = client_info.get('web_user', 'pengguna_web_tidak_dikenal')
    mqtt_user_log = client_info.get('mqtt_user', 'pengguna_mqtt_tidak_dikenal')
    client_id_str = client_info.get('client_id') # This is the MQTT Client ID
    
    print(f"KUNCI {mqtt_client_key_to_clean} (Web: {web_user_log}, MQTT: {mqtt_user_log}, CID: {client_id_str}): Membersihkan klien MQTT...")
    try:
        is_clean_app_initiated_disconnect = "Klien Socket.IO terputus" in reason or "Pengguna web logout" in reason or "Percobaan Re-login MQTT" in reason

        if is_clean_app_initiated_disconnect and mqtt_client_instance.is_connected():
            status_topic = f"client_status/{client_id_str}/departure"
            status_payload = f"Klien {client_id_str} (PenggunaMQTT: {mqtt_user_log}, PenggunaWeb: {web_user_log}) telah disconnect secara normal. Alasan: {reason}"
            try:
                print(f"KUNCI {mqtt_client_key_to_clean} (CID: {client_id_str}): Publish status keberangkatan bersih ke '{status_topic}'")
                mqtt_client_instance.publish(status_topic, status_payload, qos=1, retain=False)
                time.sleep(0.1) 
            except Exception as e:
                print(f"KUNCI {mqtt_client_key_to_clean} (CID: {client_id_str}): Kesalahan saat publish status keberangkatan: {e}")

        # 2. Signal the MQTT loop to stop
        if client_id_str and client_id_str in client_stop_events:
            print(f"KUNCI {mqtt_client_key_to_clean} (CID: {client_id_str}): Mengatur stop event untuk loop MQTT.")
            client_stop_events[client_id_str].set()

        # 3. Call MQTT client's disconnect method
        # This will send a DISCONNECT packet if connected.
        print(f"KUNCI {mqtt_client_key_to_clean} (CID: {client_id_str}): Memanggil MQTT client.disconnect().")
        mqtt_client_instance.disconnect() 

        # 4. Join the MQTT loop thread
        if loop_thread and loop_thread.is_alive():
            print(f"KUNCI {mqtt_client_key_to_clean} (CID: {client_id_str}): Menunggu thread loop MQTT {loop_thread.name} untuk join...")
            loop_thread.join(timeout=3.0) 
            if loop_thread.is_alive():
                print(f"KUNCI {mqtt_client_key_to_clean} (CID: {client_id_str}): Thread loop MQTT {loop_thread.name} TIDAK join tepat waktu.")
            else:
                print(f"KUNCI {mqtt_client_key_to_clean} (CID: {client_id_str}): Thread loop MQTT {loop_thread.name} telah join.")
        
        # 5. Clean up the stop event
        if client_id_str and client_id_str in client_stop_events: 
            client_stop_events.pop(client_id_str, None)
            print(f"KUNCI {mqtt_client_key_to_clean} (CID: {client_id_str}): Stop event MQTT telah dihapus.")

    except Exception as e:
        print(f"KUNCI {mqtt_client_key_to_clean} (CID: {client_id_str}): Exception saat cleanup klien MQTT: {e}")
    
    print(f"KUNCI {mqtt_client_key_to_clean} (CID: {client_id_str}): Proses cleanup sumber daya klien MQTT selesai.")

@app.route('/')
@login_required 
def index_route():
    return render_template('index.html', web_username=current_user.username) 

@app.route('/web_signup', methods=['GET', 'POST'])
def web_signup_page():
    global next_user_id, web_users 
    if request.method == 'POST':
        username = request.form.get('web_username')
        password = request.form.get('web_password')

        if not username or not password:
            flash('Nama pengguna dan kata sandi web wajib diisi.', 'danger')
            return redirect(url_for('web_signup_page'))
        
        if username in web_users:
            flash('Nama pengguna web sudah ada. Silakan pilih yang lain.', 'warning')
            return redirect(url_for('web_signup_page'))

        current_id_to_assign = next_user_id
        hashed_password = generate_password_hash(password, method='pbkdf2:sha256')
        
        if save_web_user_to_csv(current_id_to_assign, username, hashed_password):
            web_users[username] = {'id': current_id_to_assign, 'password_hash': hashed_password}
            next_user_id += 1 
            
            user_obj = WebUser(id=current_id_to_assign, username=username)
            login_user(user_obj) 
            flash(f'Akun web {username} berhasil dibuat dan Anda sudah login!', 'success')
            print(f"AUTH WEB: Pengguna {username} mendaftar dan login (disimpan ke CSV).")
            return redirect(url_for('index_route'))
        else:
            flash('Gagal menyimpan akun web. Silakan coba lagi.', 'danger')
            return redirect(url_for('web_signup_page'))

    return render_template('signup.html') 

@app.route('/web_login', methods=['GET', 'POST'])
def web_login_page():
    if current_user.is_authenticated:
        return redirect(url_for('index_route')) 
        
    if request.method == 'POST':
        username = request.form.get('web_username')
        password = request.form.get('web_password')
        
        user_data = web_users.get(username) 
        if user_data and check_password_hash(user_data['password_hash'], password):
            user_obj = WebUser(id=user_data['id'], username=username)
            login_user(user_obj) 
            flash(f'Login web sebagai {username} berhasil!', 'success')
            print(f"AUTH WEB: Pengguna {username} login.")
            next_page = request.args.get('next')
            return redirect(next_page or url_for('index_route'))
        else:
            flash('Nama pengguna atau kata sandi web salah.', 'danger')
    return render_template('login.html') 

@app.route('/web_logout')
@login_required
def web_logout_route():
    keys_to_clean = [key for key in mqtt_clients if key.startswith(f"user_{current_user.id}_sid_")]
    for key in keys_to_clean:
        print(f"LOGOUT WEB: Jadwalkan cleanup untuk Kunci MQTT {key} milik pengguna {current_user.username}")
        cleanup_mqtt_resources(mqtt_client_key_to_clean=key, reason="Pengguna web logout")

    print(f"AUTH WEB: Pengguna {current_user.username} logout.")
    logout_user() 
    flash('Anda telah logout dari aplikasi web.', 'info')
    return redirect(url_for('web_login_page'))


@socketio.on('connect')
def handle_socket_connect(): 
    if not current_user.is_authenticated:
        print(f"SOCKET.IO: Koneksi dari pengguna anonim (SID: {request.sid}). Ditolak.")
        return False 

    mqtt_key = get_mqtt_client_key(current_user.id, request.sid)
    join_room(mqtt_key, sid=request.sid) # MODIFIED: Used imported join_room
    
    print(f"SOCKET.IO: Klien terhubung (PenggunaWeb: {current_user.username}, SID: {request.sid}, KunciMQTT: {mqtt_key}).")
    socketio.emit('request_mqtt_login', {'message': 'Silakan masukkan kredensial MQTT Anda.'}, room=mqtt_key)


@socketio.on('disconnect')
def handle_socket_disconnect_event(): 
    if not current_user.is_authenticated: 
        print(f"SOCKET.IO: Disconnect dari pengguna anonim (SID: {request.sid}).")
        return

    mqtt_key = get_mqtt_client_key(current_user.id, request.sid)
    print(f"SOCKET.IO: Klien terputus (PenggunaWeb: {current_user.username}, SID: {request.sid}, KunciMQTT: {mqtt_key}).")
    cleanup_mqtt_resources(mqtt_client_key_to_clean=mqtt_key, reason="Klien Socket.IO terputus")
    leave_room(mqtt_key, sid=request.sid)


@socketio.on('submit_mqtt_login') 
@login_required 
def handle_submit_mqtt_login(data):
    mqtt_key = get_mqtt_client_key(current_user.id, request.sid) 

    mqtt_username = data.get('username') 
    mqtt_password = data.get('password') 
    use_mqtts = data.get('use_mqtts', True) 
    lwt_options = data.get('lwt')
    use_websockets = data.get('use_websockets', False)
    ws_path = data.get('ws_path', "/mqtt") 
    connect_topic_alias_max = data.get('connect_topic_alias_max') 

    if not mqtt_username or not mqtt_password: 
        socketio.emit('mqtt_login_status', {'success': False, 'message': 'Nama pengguna & kata sandi MQTT wajib diisi.'}, room=mqtt_key)
        return

    print(f"KUNCI {mqtt_key} (PenggunaWeb: {current_user.username}): Terima login MQTT untuk pengguna MQTT: {mqtt_username}")

    if mqtt_key in mqtt_clients: 
        print(f"KUNCI {mqtt_key}: Membersihkan klien MQTT lama untuk re-login MQTT.")
        cleanup_mqtt_resources(mqtt_client_key_to_clean=mqtt_key, reason="Percobaan Re-login MQTT")

    socketio.emit('mqtt_status', {'message': f'Mencoba koneksi MQTT untuk {mqtt_username}...'}, room=mqtt_key)

    transport_type = "websockets" if use_websockets else "tcp"
    current_port: int 
    if use_websockets:
        current_port = DEFAULT_MQTTS_WS_PORT if use_mqtts else DEFAULT_MQTT_WS_PORT
    else:
        current_port = DEFAULT_MQTTS_PORT if use_mqtts else DEFAULT_MQTT_PORT
    
    mqtt_client_instance = create_mqtt_client_for_session(
        mqtt_key, current_user.username, 
        mqtt_username, mqtt_password, use_mqtts, lwt_options,
        transport_protocol=transport_type, ws_path=ws_path, broker_port=current_port,
        connect_topic_alias_max_val=connect_topic_alias_max
    )
    
    if mqtt_client_instance:
        client_actual_id = mqtt_client_instance._client_id.decode() 
        loop_thread = threading.Thread(target=mqtt_loop_with_stop_event, args=(mqtt_client_instance, client_actual_id), daemon=True)
        loop_thread.name = f"MQTTLoop_{client_actual_id[:12]}"
        loop_thread.start()
        
        mqtt_clients[mqtt_key] = { 
            'client': mqtt_client_instance, 'loop_thread': loop_thread,
            'web_user': current_user.username, 
            'mqtt_user': mqtt_username, 
            'client_id': client_actual_id, 
            'pending_requests': {},
            'performance_data': { 
                'msg_in_count': 0, 'msg_out_count': 0, 'bytes_in_count': 0, 'bytes_out_count': 0,
                'connect_errors': 0, 'publish_errors': 0, 'subscribe_failures': 0,
                'latencies_ms': [], 'last_update_time': time.time()
            },
            'broker_topic_alias_max': 0, 
            'client_topic_alias_map': {} 
        }
        socketio.emit('mqtt_login_status', {'success': True, 'mqtt_username': mqtt_username, 'mqtt_client_id': client_actual_id, 'transport': transport_type, 'secure': use_mqtts}, room=mqtt_key)
        print(f"KUNCI {mqtt_key}: Klien MQTT untuk {mqtt_username} (CID: {client_actual_id}) dibuat, loop dimulai.")
        socketio.start_background_task(target=periodic_performance_updater_task, mqtt_key_for_task=mqtt_key) 
    else:
        print(f"KUNCI {mqtt_key}: Gagal membuat klien MQTT untuk {mqtt_username}.")
        socketio.emit('mqtt_login_status', {'success': False, 'message': 'Gagal inisialisasi klien MQTT.'}, room=mqtt_key)

@socketio.on('subscribe_to_topic')
@login_required
def handle_subscribe(data):
    mqtt_key = get_mqtt_client_key(current_user.id, request.sid)
    client_info = mqtt_clients.get(mqtt_key)
    if not client_info:
        socketio.emit('mqtt_status', {'message': 'Klien MQTT tidak aktif atau belum login MQTT.'}, room=mqtt_key); return
    
    mqtt_client = client_info['client']
    topic = data.get('topic')
    subscription_qos = data.get('qos', 1) 
    no_local = data.get('no_local', False)
    retain_as_published = data.get('retain_as_published', False)
    retain_handling = data.get('retain_handling', subopts.SubscribeOptions.RETAIN_SEND_ON_SUBSCRIBE) 

    sub_options_obj = subopts.SubscribeOptions(
        qos=subscription_qos, 
        noLocal=no_local, 
        retainAsPublished=retain_as_published, 
        retainHandling=retain_handling
    )
    
    if topic and mqtt_client.is_connected():
        print(f"KUNCI {mqtt_key} (CID: {client_info['client_id']}): Subscribe ke {topic} dengan opsi: NL={no_local}, RAP={retain_as_published}, RH={retain_handling}")
        mqtt_client.subscribe(topic, options=sub_options_obj) 
    elif not mqtt_client.is_connected():
        socketio.emit('mqtt_status', {'message': 'MQTT tidak terhubung. Tidak dapat subscribe.'}, room=mqtt_key)
    elif not topic:
        socketio.emit('mqtt_status', {'message': 'Topik subscribe tidak boleh kosong.'}, room=mqtt_key)


@socketio.on('publish_mqtt_message')
@login_required
def handle_publish(data):
    mqtt_key = get_mqtt_client_key(current_user.id, request.sid)
    client_info = mqtt_clients.get(mqtt_key)
    if not client_info:
        socketio.emit('mqtt_status', {'message': 'Klien MQTT tidak aktif atau belum login MQTT.'}, room=mqtt_key); return

    mqtt_client = client_info['client']
    cid = client_info['client_id']
    perf_data = client_info['performance_data']
    
    topic = data.get('topic') 
    payload = data.get('payload', "")
    qos = data.get('qos', 0)
    retain = data.get('retain', False)
    client_publish_time_ms = data.get('client_publish_time_ms')
    message_expiry_interval = data.get('message_expiry_interval')
    topic_alias_val = data.get('topic_alias') 

    if not topic and topic_alias_val is None: 
        socketio.emit('publish_ack', {'message': 'Topik atau Alias publish tidak boleh kosong.'}, room=mqtt_key); return

    if mqtt_client.is_connected():
        publish_properties = props.Properties(PacketTypes.PUBLISH)
        user_props_list = []
        if client_publish_time_ms:
            user_props_list.append(('client_publish_time_ms', str(client_publish_time_ms)))
        
        if message_expiry_interval is not None:
            try:
                expiry_int = int(message_expiry_interval)
                if expiry_int > 0: publish_properties.MessageExpiryInterval = expiry_int
            except ValueError: 
                print(f"KUNCI {mqtt_key} (CID: {cid}): Interval Kadaluarsa Pesan tidak valid: {message_expiry_interval}")
        
        if topic_alias_val is not None: 
            try:
                alias_to_set = int(topic_alias_val)
                if alias_to_set > 0 and (client_info.get('broker_topic_alias_max', 0) == 0 or alias_to_set <= client_info.get('broker_topic_alias_max', 0)):
                    publish_properties.TopicAlias = alias_to_set
                    if not topic: 
                         print(f"KUNCI {mqtt_key} (CID: {cid}): Publish dengan Topic Alias {alias_to_set}")
                elif alias_to_set > client_info.get('broker_topic_alias_max', 0):
                     print(f"KUNCI {mqtt_key} (CID: {cid}): Alias {alias_to_set} > maks broker {client_info.get('broker_topic_alias_max', 0)}")
            except ValueError: 
                 print(f"KUNCI {mqtt_key} (CID: {cid}): Nilai Topic Alias tidak valid: {topic_alias_val}")

        if user_props_list: publish_properties.UserProperty = user_props_list
        
        print(f"KUNCI {mqtt_key} (CID: {cid}): Publish ke {topic if topic else f'Alias {topic_alias_val}'}. QoS:{qos}, Retain:{retain}, Expiry:{getattr(publish_properties, 'MessageExpiryInterval', 'None')}")
        mqtt_client.publish(topic if topic else "", payload, qos=int(qos), retain=bool(retain), properties=publish_properties)
        perf_data['msg_out_count'] += 1
        perf_data['bytes_out_count'] += len(payload.encode('utf-8', errors='replace'))
    else:
        socketio.emit('mqtt_status', {'message': 'MQTT tidak terhubung. Tidak dapat publish.'}, room=mqtt_key)
        perf_data['publish_errors'] +=1


@socketio.on('send_mqtt_request')
@login_required
def handle_mqtt_request(data):
    mqtt_key = get_mqtt_client_key(current_user.id, request.sid)
    client_info = mqtt_clients.get(mqtt_key)
    if not client_info:
        socketio.emit('mqtt_request_response_update', {'type': 'error', 'message': 'Klien MQTT tidak aktif.'}, room=mqtt_key); return

    mqtt_client = client_info['client']
    client_id = client_info['client_id'] 
    pending_requests_for_client = client_info.get('pending_requests', {})

    target_client_id = data.get('target_client_id') 
    service_or_topic_name = data.get('service_or_topic_name') 
    request_payload = data.get('request_payload', "")
    
    if not service_or_topic_name:
        socketio.emit('mqtt_request_response_update', {'type': 'error', 'message': 'Nama service/topik request tidak boleh kosong.'}, room=mqtt_key); return
    if not mqtt_client.is_connected():
        socketio.emit('mqtt_request_response_update', {'type': 'error', 'message': 'MQTT tidak terhubung.'}, room=mqtt_key); return

    actual_request_topic: str
    if target_client_id and target_client_id.strip():
        actual_request_topic = f"clients/{target_client_id.strip()}/direct_request/{service_or_topic_name}"
        print(f"KUNCI {mqtt_key} (CID: {client_id}): Siapkan DIRECT REQUEST ke {target_client_id} di {actual_request_topic}")
    else:
        actual_request_topic = service_or_topic_name
        print(f"KUNCI {mqtt_key} (CID: {client_id}): Siapkan GENERAL REQUEST ke {actual_request_topic}")

    correlation_id = str(uuid.uuid4())
    response_topic = f"clients/{client_id}/response/{correlation_id}" 

    request_properties = props.Properties(PacketTypes.PUBLISH)
    request_properties.ResponseTopic = response_topic
    request_properties.CorrelationData = correlation_id.encode('utf-8')

    try:
        mqtt_client.publish(actual_request_topic, request_payload, qos=1, properties=request_properties)
        send_time_ms = time.time() * 1000
        pending_requests_for_client[correlation_id] = {'send_time_ms': send_time_ms, 'request_topic': actual_request_topic, 'payload': request_payload}
        
        print(f"KUNCI {mqtt_key} (CID: {client_id}): Kirim REQUEST ke {actual_request_topic}, CorrID {correlation_id}, harap respon di {response_topic}")
        socketio.emit('mqtt_request_response_update', {
            'type': 'request_sent', 'correlation_id': correlation_id, 'request_topic': actual_request_topic,
            'response_topic_subscribed': response_topic, 'status': 'sent'
        }, room=mqtt_key)

        client_info['performance_data']['msg_out_count'] += 1 
        client_info['performance_data']['bytes_out_count'] += len(request_payload.encode('utf-8', errors='replace'))

        def request_timeout_handler(key_for_timeout, cid_for_timeout, corr_id_for_timeout, pending_reqs_ref_for_timeout):
            if corr_id_for_timeout in pending_reqs_ref_for_timeout:
                print(f"KUNCI {key_for_timeout} (CID: {cid_for_timeout}): REQUEST TIMEOUT CorrID {corr_id_for_timeout}")
                pending_reqs_ref_for_timeout.pop(corr_id_for_timeout) # Hapus dari dict asli
                socketio.emit('mqtt_request_response_update', {
                    'type': 'timeout', 'correlation_id': corr_id_for_timeout, 'status': 'timeout'
                }, room=key_for_timeout)
            else:
                 print(f"KUNCI {key_for_timeout} (CID: {cid_for_timeout}): Timeout CorrID {corr_id_for_timeout} sudah diproses/dibersihkan.")
        
        timeout_thread = threading.Timer(REQUEST_TIMEOUT_SECONDS, request_timeout_handler, 
                                         args=(mqtt_key, client_id, correlation_id, pending_requests_for_client))
        timeout_thread.daemon = True
        timeout_thread.start()

    except Exception as e:
        print(f"KUNCI {mqtt_key} (CID: {client_id}): Kesalahan publish request MQTT: {e}")
        socketio.emit('mqtt_request_response_update', {'type': 'error', 'message': f'Kesalahan mengirim request: {e}'}, room=mqtt_key)
        client_info['performance_data']['publish_errors'] +=1


def periodic_performance_updater_task(mqtt_key_for_task): 
    print(f"TASK PERFORMA: Mulai untuk Kunci MQTT {mqtt_key_for_task}.")
    try:
        while mqtt_key_for_task in mqtt_clients: 
            client_info = mqtt_clients.get(mqtt_key_for_task)
            if not client_info or not client_info.get('client'): break 

            socketio.sleep(PERFORMANCE_UPDATE_INTERVAL_SECONDS) 

            client_info = mqtt_clients.get(mqtt_key_for_task) 
            if not client_info or not client_info.get('client'): break
            
            perf_data = client_info['performance_data']
            current_time = time.time()
            time_delta_sec = current_time - perf_data.get('last_update_time', current_time)
            if time_delta_sec <= 0: time_delta_sec = 1 # Hindari div by zero

            msg_in_rate = perf_data.get('msg_in_count', 0) / time_delta_sec
            msg_out_rate = perf_data.get('msg_out_count', 0) / time_delta_sec
            bytes_in_rate = perf_data.get('bytes_in_count', 0) / time_delta_sec
            bytes_out_rate = perf_data.get('bytes_out_count', 0) / time_delta_sec

            update_payload = {
                'msg_in_rate_mps': msg_in_rate, 'msg_out_rate_mps': msg_out_rate,
                'bytes_in_rate_Bps': bytes_in_rate, 'bytes_out_rate_Bps': bytes_out_rate,
                'total_connect_errors': perf_data.get('connect_errors', 0),
                'total_publish_errors': perf_data.get('publish_errors', 0),
                'total_subscribe_failures': perf_data.get('subscribe_failures', 0),
                'recent_latencies_ms': list(perf_data.get('latencies_ms', [])),
                'client_id': client_info.get('client_id', 'N/A'),
                'broker_topic_alias_max': client_info.get('broker_topic_alias_max', 0)
            }
            socketio.emit('performance_update', update_payload, room=mqtt_key_for_task) 
            
            perf_data['msg_in_count'] = 0
            perf_data['msg_out_count'] = 0
            perf_data['bytes_in_count'] = 0
            perf_data['bytes_out_count'] = 0
            perf_data['last_update_time'] = current_time
        
    except Exception as e:
        print(f"TASK PERFORMA: Kesalahan untuk Kunci MQTT {mqtt_key_for_task}: {e}")
    finally:
        print(f"TASK PERFORMA: Selesai untuk Kunci MQTT {mqtt_key_for_task}.")


if __name__ == '__main__':
    print("Memulai server Flask-SocketIO...") 
    print(f"Target Broker MQTT: {BROKER_HOST}, Port Plain: {DEFAULT_MQTT_PORT}, MQTTS: {DEFAULT_MQTTS_PORT}, WS: {DEFAULT_MQTT_WS_PORT}, WSS: {DEFAULT_MQTTS_WS_PORT}")
    print(f"Keepalive MQTT diatur ke: {MQTT_KEEPALIVE_SECONDS} detik") 
    print(f"Akun pengguna web akan disimpan di: {WEB_USERS_FILE}")
    print("Pastikan broker Mosquitto berjalan dan dikonfigurasi dengan benar.") 
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True)