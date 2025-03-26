import json
import time
import datetime
import logging
import threading
from logging.handlers import TimedRotatingFileHandler
import paho.mqtt.client as mqtt


# ---------- Load LICOR config ----------
with open("config.json") as f:
    config = json.load(f)

licors = config["INSTRUMENTS"]
protocol_path = config["PROTOCOL"]["FILE"]


# ---------- Load remarks from file ----------
def load_remarks(filepath="remarks.txt"):
    schedule = []
    with open(filepath, "r") as f:
        for line in f:
            parts = line.strip().split("\t")
            if not parts:
                continue
            try:
                timestamp = datetime.datetime.strptime(
                    parts[0], "%H:%M:%S"
                ).time()
                message = parts[-1]
                schedule.append((timestamp, message))
            except ValueError:
                continue
    return sorted(schedule, key=lambda x: x[0])


# ---------- Get current remark based on time ----------
def get_current_scheduled_remark(remarks):
    now = datetime.datetime.now().time()
    current_remark = None
    for t, msg in remarks:
        if t <= now:
            current_remark = msg
        else:
            break
    return current_remark


# ---------- Threaded MQTT Publisher ----------
def publish_to_licor(ip, topic, licor_name):
    try:
        client = mqtt.Client()
        client.connect(ip, 1883, 60)
        client.loop_start()
        message = get_current_scheduled_remark(remarks)
        if not message:
            logger.warning("No valid remark found for current time.")
            return
        client.publish(topic, message)
        client.loop_stop()
        client.disconnect()
        logger.info(f"Published '{message}' to {ip} ({licor_name})")
        time.sleep(1)
    except Exception as e:
        logger.error(f"Failed to publish to {ip} ({licor_name}): {e}")
        # client.loop_stop()
        # client.disconnect()


# ---------- Publish current remark to all LICORs in threads ----------
def publish_remark(remarks):
    threads = []
    for licor_name, licor_config in licors.items():
        ip = licor_config.get("IP")
        topic = "licor/niobrara/system/log_remark"
        thread = threading.Thread(
            target=publish_to_licor,
            args=(ip, topic, licor_name),
            daemon=True,
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


# ---------- Logger Setup ----------
def init_logger(log_level="info"):
    logger = logging.getLogger("remarkLogger")
    log_path = "D:/DATA/AC/remark_publish.log"
    handler = TimedRotatingFileHandler(
        log_path, when="midnight", interval=1, backupCount=7, encoding="utf-8"
    )
    handler.suffix = "%Y-%m-%d"
    formatter = logging.Formatter("%(asctime)s,%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(getattr(logging, log_level.upper()))
    return logger


# ---------- Main Loop ----------
if __name__ == "__main__":
    logger = init_logger()
    remarks = load_remarks(protocol_path)

    logger.info("Started threaded remark publisher.")

    try:
        while True:
            publish_remark(remarks)
    except KeyboardInterrupt:
        logger.info("Stopped remark publisher.")
