"""
Publisher MQTT khusus snapshot CrowdSense.
Mengirim JPEG tanpa mengirim data audience.

python -m venv env
venv/Scripts/activate / deactivate

Install:  pip install "paho-mqtt>=2.0" & pip install python-dotenv

running skrip:
python publish_snapshot.py --session-id "ISI_SESSION_ID_LAMA" --snapshot-key "ISI_SNAPSHOT_KEY_LAMA" --file "female.jpg"

session-id & snapshot-key dari mqtt_publisher.py baris 154-156:
#print(f"SESSION_ID: {SESSION_ID}")
#print(f"SNAPSHOT_KEY: {snap_key1}")

"""

from dotenv import load_dotenv
load_dotenv()

import os
import time
import argparse
import paho.mqtt.client as mqtt

SERIAL = os.getenv("DEVICE_SERIAL", "SN-TEST-001")
HOST = os.getenv("MQTT_HOST", "localhost")
PORT = int(os.getenv("MQTT_PORT", "1883"))

USERNAME = SERIAL
PASSWORD = os.environ["DEVICE_API_KEY"]


def on_connect(client, userdata, flags, reason_code, properties):
    print(f"[mqtt] connected: {reason_code}")


def on_disconnect(client, userdata, flags, reason_code, properties):
    print(f"[mqtt] disconnected: {reason_code}")


def publish_snapshot(session_id: str, snapshot_key: str, image_path: str) -> bool:
    # Validasi dasar agar key yang salah tidak langsung dikirim.
    allowed = set(
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789-_"
    )

    if not session_id or not snapshot_key:
        raise ValueError("SESSION_ID dan SNAPSHOT_KEY wajib diisi.")

    if len(snapshot_key) > 128 or any(c not in allowed for c in snapshot_key):
        raise ValueError(
            "SNAPSHOT_KEY hanya boleh berisi huruf, angka, '-' dan '_', "
            "maksimal 128 karakter."
        )

    if not os.path.isfile(image_path):
        raise FileNotFoundError(f"File tidak ditemukan: {image_path}")

    with open(image_path, "rb") as f:
        jpeg_bytes = f.read()

    if not jpeg_bytes:
        raise ValueError("File snapshot kosong.")

    # JPEG seharusnya memiliki signature SOI dan EOI.
    if not (
        jpeg_bytes.startswith(b"\xff\xd8")
        and jpeg_bytes.endswith(b"\xff\xd9")
    ):
        raise ValueError("File bukan JPEG yang valid atau tidak lengkap.")

    if len(jpeg_bytes) > 1_000_000:
        raise ValueError("Ukuran JPEG melebihi batas contoh 1 MB.")

    topic = f"crowdsense/snapshot/{SERIAL}/{session_id}/{snapshot_key}"

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"{SERIAL}-snapshot-test",
        protocol=mqtt.MQTTv311,
        clean_session=True,
    )

    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    try:
        client.connect(HOST, PORT, keepalive=30)
        client.loop_start()

        # Tunggu koneksi MQTT selesai sebelum publish.
        deadline = time.monotonic() + 10
        while not client.is_connected() and time.monotonic() < deadline:
            time.sleep(0.1)

        if not client.is_connected():
            raise TimeoutError("Gagal terhubung ke broker MQTT dalam 10 detik.")

        print(f"Topic       : {topic}")
        print(f"File        : {image_path}")
        print(f"Ukuran      : {len(jpeg_bytes)} bytes")
        print("Mengirim snapshot...")

        info = client.publish(topic, jpeg_bytes, qos=1)
        info.wait_for_publish(timeout=15)

        if not info.is_published():
            print("Snapshot belum mendapat konfirmasi publish.")
            return False

        print("Snapshot mendapat konfirmasi publish (QoS 1).")
        return True

    finally:
        client.disconnect()
        client.loop_stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Tes pengiriman snapshot CrowdSense melalui MQTT."
    )
    parser.add_argument("--session-id", required=True, help="SESSION_ID dari skrip sebelumnya")
    parser.add_argument("--snapshot-key", required=True, help="SNAPSHOT_KEY dari skrip sebelumnya")
    parser.add_argument("--file", default="female.jpg", help="Path file JPEG")
    args = parser.parse_args()

    try:
        success = publish_snapshot(
            session_id=args.session_id,
            snapshot_key=args.snapshot_key,
            image_path=args.file,
        )
        print("HASIL:", "PUBLISH BERHASIL" if success else "PUBLISH GAGAL")
    except Exception as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(1)