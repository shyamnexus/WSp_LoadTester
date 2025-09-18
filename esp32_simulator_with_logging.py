
import paho.mqtt.client as mqtt
import threading
import time
import json
import base64
import random
import numpy as np
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import os

# Configuration
BROKER = "10.67.97.252"
PORT = 1883
NODE_COUNT = 300
PUBLISH_INTERVAL = 0.1
ACTIVE_DURATION = 300
SLEEP_DURATION = 600
SAMPLE_RATE = 32
RETRY_ATTEMPTS = 3

# Logging directory and setup
LOG_DIR = "mqtt_logs"
os.makedirs(LOG_DIR, exist_ok=True)

def get_daily_rotating_logger():
    current_date = datetime.now().strftime("%Y-%m-%d")
    log_filename = os.path.join(LOG_DIR, f"mqtt_publish_{current_date}.log")

    handler = RotatingFileHandler(
        log_filename,
        maxBytes=10 * 1024 * 1024,
        backupCount=5
    )

    formatter = logging.Formatter('%(asctime)s %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger(f"mqtt_logger_{current_date}")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False

    return logger

mqtt_file_logger = get_daily_rotating_logger()
log_file_lock = threading.Lock()

class ESP32Node(threading.Thread):
    def __init__(self, device_id):
        super().__init__()
        self.device_id = device_id
        self.client = mqtt.Client(client_id=f"esp32_{device_id}", protocol=mqtt.MQTTv311)
        self.client.username_pw_set("wspmqtt", "WSP@2025")
        self.packet_seq = random.randint(10000, 50000)
        self.connected = False
        self.try_connect()

    def try_connect(self):
        for attempt in range(RETRY_ATTEMPTS):
            try:
                self.client.connect(BROKER, PORT)
                self.client.loop_start()
                self.connected = True
                print(f"[{self.device_id}] Connected to broker.")
                break
            except Exception as e:
                print(f"[{self.device_id}] Connection attempt {attempt + 1} failed: {e}")
                time.sleep(2)
        if not self.connected:
            print(f"[{self.device_id}] Could not connect to broker after {RETRY_ATTEMPTS} attempts.")

    def run(self):
        if not self.connected:
            return
        while True:
            start_time = time.time()
            while time.time() - start_time < ACTIVE_DURATION:
                try:
                    self.send_packet()
                except Exception as e:
                    print(f"[{self.device_id}] Failed to send packet: {e}")
                time.sleep(PUBLISH_INTERVAL)
            print(f"[{self.device_id}] Sleeping for {SLEEP_DURATION} seconds...")
            time.sleep(SLEEP_DURATION)

    def send_packet(self):
        timestamp = datetime.now().isoformat(timespec='microseconds')
        battery = random.randint(10, 100)
        temperature = round(random.uniform(29.0, 31.0), 6)
        lead_status = random.choice(["connected", "disconnected"])
        ecg_data_b64 = self.generate_random_ecg_base64()

        packet = {
            "timestamp": timestamp,
            "data": ecg_data_b64,
            "packet_id": f"{time.time()}.{self.device_id}.{self.packet_seq}",
            "seq_no": self.packet_seq,
            "battery": battery,
            "sample_rate": SAMPLE_RATE,
            "lead_status": lead_status,
            "is_moving":0,
            "temperature": temperature
        }

        topic = f"wsp/devices/data/{self.device_id}"
        payload = json.dumps(packet)
        self.client.publish(topic, payload, qos=0)

        with log_file_lock:
            mqtt_file_logger.info(json.dumps({
                "device_id": self.device_id,
                "topic": topic,
                "payload": packet
            }))

        self.packet_seq += 1

    def generate_random_ecg_base64(self):
        ecg_samples = np.random.randint(-2048, 2048, 128, dtype=np.int16)
        ecg_bytes = ecg_samples.tobytes()
        return base64.b64encode(ecg_bytes).decode('utf-8')

def main():
    print(f"Launching {NODE_COUNT} ESP32 simulated nodes...")
    nodes = [ESP32Node(f"esp32_{i:03d}") for i in range(NODE_COUNT)]
    for node in nodes:
        node.start()
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("Simulation interrupted by user.")

if __name__ == "__main__":
    main()
