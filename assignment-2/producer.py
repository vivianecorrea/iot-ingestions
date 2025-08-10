import logging
import json, time, random
from datetime import datetime, timezone
from faker import Faker
from confluent_kafka import Producer
from time import sleep

logging.basicConfig(
    level=logging.INFO,  
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class IotFakeProducer:

    def __init__(self, configs: dict, seconds_run):
        self.configs = configs
        self.seconds_run= seconds_run 

    def run(self):
        logging.info(f"Starting IotFakeProducer for {self.seconds_run} seconds")
        end_time = time.time() + self.seconds_run
        while time.time() < end_time:
            self._produce_message()
            sleep(1)
    
    def _produce_message(self):
        producer = Producer(self._get_kafka_configs(self.configs))     
        message_key = str(random.randint(1, 1000)).encode('utf-8')
        message = self._generate_fake_messages(message_key)
        producer.produce(self.configs.get('TOPIC'), value=message, key=message_key)
        logging.info(f"Produced message: {message}, to topic: {self.configs.get('TOPIC')}")
        producer.flush()
        time.sleep(5)       

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
        message = json.dumps(message).encode('utf-8')
        logging.info(f"Produced message: {message}, to topic: {self.configs.get('TOPIC')}")
        return message

    @staticmethod
    def _get_kafka_configs(configs: dict) -> dict:
        return {
        "bootstrap.servers": configs.get('BOOTSTRAP'),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": configs.get('API_KEY'),
        "sasl.password": configs.get('API_SECRET'),
    }

   

         

    
            
    
