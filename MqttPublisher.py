"""
Contoh publisher MQTT untuk perangkat edge (Python) dengan penanganan koneksi async.

Install:  pip install "paho-mqtt>=2.0"
"""
import json
import os
import time
import uuid
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

SERIAL = os.getenv("DEVICE_SERIAL", "tes-tes")   
HOST = os.getenv("MQTT_HOST", "localhost")
PORT = int(os.getenv("MQTT_PORT", "1883"))
USERNAME = os.getenv("MQTT_USER", "device-dev")
PASSWORD = os.getenv("MQTT_PASS", "ganti-dev")

TOPIC_AUDIENCE = f"crowdsense/audience/{SERIAL}"
TOPIC_SNAPSHOT = f"crowdsense/snapshot/{SERIAL}"

SESSION_ID = str(uuid.uuid4())

# Variabel global untuk memantau status koneksi MQTT
is_connected = False


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def on_connect(client, userdata, flags, reason_code, properties):
    global is_connected
    if reason_code == 0:
        is_connected = True
        print(f"[mqtt] connected: Success")
    else:
        print(f"[mqtt] connection failed with code {reason_code}")


def on_disconnect(client, userdata, flags, reason_code, properties):
    global is_connected
    is_connected = False
    print(f"[mqtt] disconnected: {reason_code} (paho akan reconnect otomatis)")


client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id=SERIAL,
    protocol=mqtt.MQTTv311,
    clean_session=False,
)
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.reconnect_delay_set(min_delay=1, max_delay=30)


def publish_audience_batch(audiences: list[dict]) -> bool:
    payload = json.dumps(
        {
            "serialNumber": SERIAL,
            "sessionId": SESSION_ID,
            "audiences": audiences,
        },
        separators=(",", ":"),
    )
    info = client.publish(TOPIC_AUDIENCE, payload, qos=1)
    info.wait_for_publish(timeout=10)
    return info.is_published()


def publish_snapshot(snapshot_key: str, jpeg_bytes: bytes) -> bool:
    info = client.publish(f"{TOPIC_SNAPSHOT}/{SESSION_ID}/{snapshot_key}", jpeg_bytes, qos=1)
    info.wait_for_publish(timeout=10)
    return info.is_published()


if __name__ == "__main__":
    # Menggunakan connect_async agar tidak blocking saat NATS mati di awal
    client.connect_async(HOST, PORT, keepalive=30)
    client.loop_start()          

    print("Menghubungkan ke broker NATS (pastikan NATS menyala)...")
    
    # Tunggu sampai benar-benar tersambung (maksimal 30 detik)
    timeout = 30
    start_time = time.time()
    while not is_connected:
        if time.time() - start_time > timeout:
            print("[Error] Timeout: Gagal terhubung ke broker NATS! Pastikan kontainer NATS aktif.")
            client.loop_stop()
            exit(1)
        time.sleep(0.1)

    now = time.time()
    snapshot_key = str(uuid.uuid4())

    try:
        with open("female.jpg", "rb") as f:
            jpeg_data = f.read()
        ok_photo = publish_snapshot(snapshot_key, jpeg_data)
        print("foto snapshot terkirim:", ok_photo)
    except FileNotFoundError:
        print("[Warning] File 'female.jpg' tidak ditemukan, melewati pengiriman snapshot.")

    ok = publish_audience_batch([
        {
            "trackId": f"t-{int(now)}-1",
            "detectedAt": iso(now - 5),
            "exitedAt": iso(now),
            "dwellTimeSeconds": 5.0,
            "gender": "male",
            "genderConfidence": 0.91,
            "ageEstimate": 10,
            "ageMin": 6,
            "ageMax": 17,
            "ageConfidence": 0.8,
            "isView": True,
            "snapshotKey": snapshot_key,
        },
    ])
    print("audience terkirim:", ok)

    client.loop_stop()
    client.disconnect()