"""
Contoh publisher MQTT untuk perangkat edge (Python).

Install:  pip install "paho-mqtt>=2.0"

Payload memakai skema yang SAMA dengan JSON yang dulu dikirim ke POST /api/v1/ingest/audience:
  { "serialNumber": "...", "sessionId": "<uuid>", "audiences": [ {...}, ... ] }

Aturan penting (dicek oleh backend, pelanggaran masuk ke DLQ dan tidak ada balasan ke device):
  - serialNumber di payload harus SAMA dengan serial di topic.
  - sessionId harus UUID bentuk 8-4-4-4-12 (bukan nol semua).
  - trackId unik di dalam satu sessionId (kunci duplikat = device + session + track).
  - detectedAt wajib, format ISO 8601 (contoh 2026-10-21T10:00:00Z).
  - gender: "male" | "female" | "unknown".
  - maksimal MaxRowsPerMessage (default 500) item per pesan.
"""
import json
import os
import time
import uuid
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

SERIAL = os.getenv("DEVICE_SERIAL", "tes-tes")   # harus terdaftar di tabel device; tanpa titik/spasi/# + *
HOST = os.getenv("MQTT_HOST", "localhost")
PORT = int(os.getenv("MQTT_PORT", "1883"))
USERNAME = os.getenv("MQTT_USER", "device-dev")
PASSWORD = os.getenv("MQTT_PASS", "ganti-dev")

TOPIC_AUDIENCE = f"crowdsense/audience/{SERIAL}"
TOPIC_SNAPSHOT = f"crowdsense/snapshot/{SERIAL}"   # ditambah /{sessionId}/{snapshotKey} saat publish

# Satu sesi = satu kali kamera/pipeline AI berjalan. Buat sekali saat start, pakai untuk semua batch.
SESSION_ID = str(uuid.uuid4())


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def on_connect(client, userdata, flags, reason_code, properties):
    print(f"[mqtt] connected: {reason_code}")


def on_disconnect(client, userdata, flags, reason_code, properties):
    print(f"[mqtt] disconnected: {reason_code} (paho akan reconnect otomatis)")


# client_id tetap + clean_session=False: kalau koneksi putus saat pesan QoS 1
# belum di-ack, pesan dikirim ulang otomatis setelah reconnect.
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
    """Kirim satu batch. Return True kalau broker sudah mengonfirmasi (PUBACK)."""
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
    """Kirim foto crop (JPEG mentah, maksimal ~1 MB).

    Metadata ikut di topic karena MQTT 3.1.1 tidak punya header:
        crowdsense/snapshot/{serial}/{sessionId}/{snapshotKey}
    snapshot_key HARUS sama dengan field "snapshotKey" di data audience, hanya boleh berisi
    huruf, angka, '-' dan '_' (JANGAN pakai titik, karena titik memisahkan bagian subject di NATS),
    dan maksimal 128 karakter. Contoh: str(uuid.uuid4()).
    """
    info = client.publish(f"{TOPIC_SNAPSHOT}/{SESSION_ID}/{snapshot_key}", jpeg_bytes, qos=1)
    info.wait_for_publish(timeout=10)
    return info.is_published()


if __name__ == "__main__":
    client.connect(HOST, PORT, keepalive=30)
    client.loop_start()          # thread background untuk koneksi + reconnect
    time.sleep(1)

    now = time.time()

    snapshot_key = str(uuid.uuid4())

    with open("anabul.jpg", "rb") as f:
        jpeg_data = f.read()
    ok_photo = publish_snapshot(snapshot_key, jpeg_data)
    print("foto snapshot terkirim:", ok_photo)

    ok = publish_audience_batch([
        {
            "trackId": f"t-{int(now)}-1",
            "detectedAt": iso(now - 5),
            "exitedAt": iso(now),
            "dwellTimeSeconds": 5.0,
            "gender": "male",
            "genderConfidence": 0.91,
            "ageEstimate": 31,
            "ageMin": 28,
            "ageMax": 35,
            "ageConfidence": 0.8,
            "isView": True,
            "snapshotKey": snapshot_key,
        },
    ])
    print("audience terkirim:", ok)

    client.loop_stop()
    client.disconnect()