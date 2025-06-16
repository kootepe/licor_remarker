import paho.mqtt.client as mqtt
import sys
import datetime


# Define what happens when a message is received
def on_message(client, userdata, msg):
    print(
        f"Time: {datetime.datetime.now()} Topic: {msg.topic} | {msg.payload.decode()}"
    )


def main(ip, path):
    # Create an MQTT client instance
    client = mqtt.Client()

    # Set the message handling function
    client.on_message = on_message

    # Connect to the MQTT broker (replace with your broker address)
    client.connect(ip, 1883, 60)

    # diag path "licor/niobrara/output/diagnostics")

    client.subscribe(path)

    # Keep the connection alive and wait for messages
    client.loop_forever()


if __name__ == "__main__":
    if len(sys.argv) < 1:
        sys.exit("Provide IP address and optional mqtt path")
    ip = sys.argv[1]
    path = "#"
    try:
        arg = sys.argv[2]
        path = arg
    except IndexError:
        # print all paths if no arg provided
        path = "#"
    if path == "remark":
        path = "licor/niobrara/system/log_remark"
    main(ip, path)
