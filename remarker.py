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
# ---------- Persistent Threaded MQTT Publisher ----------
def licor_publisher_thread(
    ip, topic, licor_name, remarks, stop_event, interval=10
):
    client = mqtt.Client()
    try:
        client.connect(ip, 1883, 60)
        client.loop_start()
        logger.info(f"Started MQTT client for {licor_name} at {ip}")
    except Exception as e:
        logger.error(f"Failed to connect to {ip} ({licor_name}): {e}")
        return

    while not stop_event.is_set():
        message = get_current_scheduled_remark(remarks)
        if message:
            try:
                client.publish(topic, message)
                logger.info(f"Published '{message}' to {ip} ({licor_name})")
            except Exception as e:
                logger.error(f"Publish error to {ip} ({licor_name}): {e}")
        else:
            logger.warning(f"No valid remark found for {licor_name}")

        stop_event.wait(interval)

    client.loop_stop()
    client.disconnect()
    logger.info(f"Stopped MQTT client for {licor_name}")


# ---------- Start Threads Once ----------
def start_publisher_threads(remarks):
    threads = []
    stop_events = {}

    for licor_name, licor_config in licors.items():
        ip = licor_config.get("IP")
        topic = "licor/niobrara/system/log_remark"
        stop_event = threading.Event()
        thread = threading.Thread(
            target=licor_publisher_thread,
            args=(ip, topic, licor_name, remarks, stop_event),
            daemon=True,
        )
        thread.start()
        threads.append(thread)
        stop_events[licor_name] = stop_event
        logger.info(f"Started publisher thread for {licor_name}")

    return threads, stop_events


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

    logger.info("Started persistent remark publisher threads.")
    threads, stop_events = start_publisher_threads(remarks)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping remark publishers...")
        for event in stop_events.values():
            event.set()
        for thread in threads:
            thread.join()
        logger.info("All remark publisher threads stopped.")
