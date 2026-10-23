"""
Contoh publisher MQTT untuk perangkat edge (Python).

Install:  pip install "paho-mqtt>=2.0"

Payload memakai skema yang SAMA dengan JSON yang dulu dikirim ke POST /api/v1/ingest/audience:
  { "serialNumber": "...", "sessionId": "<uuid>", "audiences": [ {...}, ... ] }

Autentikasi: username = serialNumber, password = API key device (env DEVICE_API_KEY).
Device hanya boleh PUBLISH ke topic miliknya sendiri; SUBSCRIBE ditolak oleh broker.
Koneksi diputus broker secara berkala (maks. 60 menit) dan harus connect ulang; paho melakukannya otomatis.

Aturan penting (dicek oleh backend, pelanggaran masuk ke DLQ dan tidak ada balasan ke device):
  - serialNumber di payload harus SAMA dengan serial di topic.
  - sessionId harus UUID bentuk 8-4-4-4-12 (bukan nol semua).
  - trackId unik di dalam satu sessionId (kunci duplikat = device + session + track).
  - detectedAt wajib, format ISO 8601 (contoh 2026-10-21T10:00:00Z).
  - gender: "male" | "female" | "unknown".
  - maksimal MaxRowsPerMessage (default 500) item per pesan.
"""
from dotenv import load_dotenv
load_dotenv()

import json
import os
import time
import uuid
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

SERIAL = os.getenv("DEVICE_SERIAL", "SN-TEST-001")   # harus terdaftar di tabel device; tanpa titik/spasi/# + *
HOST = os.getenv("MQTT_HOST", "localhost")
PORT = int(os.getenv("MQTT_PORT", "1883"))
# Autentikasi per device: username = serialNumber, password = API key device (yang ditampilkan SEKALI
# oleh backend saat device dibuat atau key-nya dirotasi).
USERNAME = SERIAL
PASSWORD = os.environ["DEVICE_API_KEY"]

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
    
    # 1. Buat unique snapshot key (hanya huruf, angka, -, _)
    # Pastikan key ini sama dengan field "snapshotKey" di data audience jika ingin dihubungkan
    snap_key1 = str(uuid.uuid4())
    snap_key2 = str(uuid.uuid4())

    # 2. Kirim data audience (dengan menyertakan snapshotKey-nya)
    ok = publish_audience_batch([
        {
            "trackId": f"t-{int(now)}-1",  # Unik untuk orang pertama
            "detectedAt": iso(now - 5),
            "exitedAt": iso(now),
            "dwellTimeSeconds": 8.0,
            "gender": "female",
            "genderConfidence": 0.71,
            "ageEstimate": 22,
            "ageMin": 18,
            "ageMax": 35,
            "ageConfidence": 0.5,
            "isView": True,
            "snapshotKey": snap_key1,
        },
        {
            "trackId": f"t-{int(now)}-2",  # unik untuk orang kedua
            "detectedAt": iso(now - 5),
            "exitedAt": iso(now),
            "dwellTimeSeconds": 10.0,
            "gender": "male",
            "genderConfidence": 0.85,
            "ageEstimate": 25,
            "ageMin": 20,
            "ageMax": 40,
            "ageConfidence": 0.6,
            "isView": True,
            "snapshotKey": snap_key2,
        },
    ])
    print("audience terkirim:", ok)

    # 3. Kirim file foto crop mentah (JPEG bytes)
    # Contoh: membaca file gambar asli dari disk
    try:
        with open("female.jpg", "rb") as f:
            jpeg_data = f.read()
        
        ok_snap = publish_snapshot(snap_key1, jpeg_data)
        print("snapshot terkirim:", ok_snap)
    except FileNotFoundError:
        print("File 'female.jpg' tidak ditemukan, lewati pengiriman snapshot.")

    try:
        with open("male.jpg", "rb") as f:
            jpeg_data = f.read()
            
        ok_snap = publish_snapshot(snap_key2, jpeg_data)
        print("snapshot terkirim:", ok_snap)
    except FileNotFoundError:
        print("File 'male.jpg' tidak ditemukan, lewati pengiriman snapshot.")

    client.loop_stop()
    client.disconnect()