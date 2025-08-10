import logging
import json, time, random
from datetime import datetime, timezone
from faker import Faker
from confluent_kafka import Producer
from time import sleep
class IotFakeProducer:

    def __init__(self, configs: dict):
        self.configs = configs


    def _get_kafka_configs(configs: dict) -> dict:
        return {
        "bootstrap.servers": configs.get('BOOTSTRAP'),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": configs.get('API_KEY'),
        "sasl.password": configs.get('API_SECRET'),
    }

    def _generate_fake_messages(self, id: str):
        fake = Faker()
        message = { "sensor_id":id,
        "device_id": fake.uuid4(),
        "location": fake.city(),
        "ts": datetime.now(timezone.utc).isoformat(),
        "temperature": float(round(random.uniform(10, 40), 2)),
        "humidity": float(round(random.uniform(20, 95), 2)),
        "lat": float(fake.latitude()),
        "lon": float(fake.longitude())
        }
        #message = json.dumps(message).encode('utf-8')
        logging.info(f"Produced message: {message}, to topic: {self.configs.get('TOPIC')}")
        return message

         

    def run(self):
        producer = Producer(self._get_kafka_configs(self.configs))     
        message = self._generate_fake_messages(random.randint(1, 1000))
        producer.produce(self.configs.get('TOPIC'), value=message)
        logging.info(f"Produced message: {message}, to topic: {self.configs.get('TOPIC')}")
        producer.flush()
        time.sleep(5)   


instance = IotFakeProducer({
    "BOOTSTRAP": "your_bootstrap_servers",
    "API_KEY": "your_api_key",
    "API_SECRET": "your_api_secret",
    "TOPIC": "your_topic_name"
})
while True:
    id = random.randint(1, 1000)
    print(instance._generate_fake_messages(id))
    sleep(5)