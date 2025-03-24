import json
import time
import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
import paho.mqtt.client as mqtt


# ---------- Load LICOR config ----------
with open("config.json") as f:
    licors = json.load(f)["INSTRUMENTS"]


# ---------- Load remarks from file ----------
def load_remarks(filepath="remarks.txt"):
    remarks = []
    with open(filepath, "r") as f:
        for line in f:
            parts = line.strip().split("\t")
            if not parts:
                continue
            try:
                t = datetime.datetime.strptime(parts[0], "%H:%M:%S").time()
                remarks.append((t, parts[-1]))
            except ValueError:
                continue
    return remarks


# ---------- Find latest remark for current time ----------
def get_current_remark(remarks):
    now = datetime.datetime.now().time()
    last_remark = None
    for t, msg in remarks:
        if t <= now:
            last_remark = msg
    return last_remark


# ---------- Publish message ----------
def publish_remark(ip, topic, message):
    try:
        client = mqtt.Client()
        client.connect(ip, 1883, 60)
        client.loop_start()
        time.sleep(1)
        client.publish(topic, message)
        time.sleep(1)
        client.loop_stop()
        client.disconnect()
        logger.info(f"Published remark to {ip} at {topic}: {message}")
    except Exception as e:
        logger.error(f"Failed to publish to {ip} at {topic}: {e}")


# ---------- Send remarks to all LICOR devices ----------
def send_remarks_to_all(remarks):
    remark = get_current_remark(remarks)
    if not remark:
        logger.warning("No valid remark found for current time.")
        return

    for licor_name, licor_config in licors.items():
        ip = licor_config.get("IP")
        topic = "licor/niobrara/system/log_remark"
        publish_remark(ip, topic, remark)


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


# ---------- Run ----------
if __name__ == "__main__":
    logger = init_logger()
    remarks = load_remarks("D:\Soft\ACW\protocol.ini")
    send_remarks_to_all(remarks)

