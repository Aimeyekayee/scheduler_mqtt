from typing import List
from datetime import datetime as dt
from paho.mqtt import client as mqtt_client


class MqttManager:
    def __init__(
        self,
        broker,
        port,
        sub_topics,
        pub_topics,
        client_id,
        on_connect_func,
        on_message_func,
        name="",
    ):
        self.name = name
        self.broker = broker
        self.port = port
        self.sub_topics = sub_topics
        self.pub_topics = pub_topics
        self.client_id = client_id
        self.client = None
        self.on_connect_func = on_connect_func
        self.on_message_func = on_message_func
        self.is_disconnected = False

    def on_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            print(f"Connected {self.name} client to MQTT Broker!: {client}")
            self.is_disconnected = False
            if isinstance(self.sub_topics, List):
                for topic in self.sub_topics:
                    self.subscribe(self.client, topic)
            if callable(self.on_connect_func):
                self.on_connect_func(self)
        else:
            print(f"Failed to connect client, return code: {rc}")

    def on_disconnect(self, userdata, rc, props):
        print(f"MQTT {self.name} client was disconnected!")
        self.is_disconnected = True
        while self.is_disconnected:
            print(f"MQTT {self.name} client try to reconnect")
            self.client.connect(self.broker, self.port)

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode()
        # logger.info(f"Received `{payload}` from `{topic}` topic")
        if callable(self.on_message_func):
            self.on_message_func(topic, payload)

    def subscribe(self, client: mqtt_client, topic: str):
        print(f"Subscribe topic: {topic}")
        client.subscribe(topic)
        client.on_message = self.on_message

    def publish(self, topic, payload):
        try:
            self.client.publish(topic, payload)
            print("Message published successfully.")
        except Exception as e:
            print(f"Error publishing message: {e}")

    def initialize(self):
        self.client = mqtt_client.Client(
            mqtt_client.CallbackAPIVersion.VERSION2, transport="websockets"
        )
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

        self.client.connect(self.broker, self.port)
        self.client.reconnect()
        self.client.reconnect_delay_set(10, 60)

        self.client.loop_start()
