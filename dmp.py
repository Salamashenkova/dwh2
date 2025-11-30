import yaml  
import json
import hashlib
import pandas as pd
from sqlalchemy import create_engine
from confluent_kafka import Consumer
from datetime import datetime
import binascii
import psycopg2  

class DMPProcessor:
    def __init__(self, config_path, db_connection_string, kafka_config):
        self.config_path = config_path
        self.db_engine = create_engine(db_connection_string)
        self.kafka_config = kafka_config
        self.tables_config = self.load_config()
        self.consumer = self.setup_kafka_consumer()

    def load_config(self):
        """Загружает ddl_config.yaml."""
        with open(self.config_path, 'r') as file:
            config = yaml.safe_load(file)
            print(f" Loaded config: {list(config['ddl_config'].keys())}")
            return config

    def setup_kafka_consumer(self):
        """Настраивает Kafka Consumer."""
        consumer = Consumer(self.kafka_config)
        #  Kafka topics из ddl_config (Debezium формат!)
        topics = ['user_service_db.public.USERS', 'order_service_db.public.ORDERS']
        consumer.subscribe(topics)
        print(f" Subscribed to CDC topics: {topics}")
        return consumer

    def process_kafka_messages(self):
        """Обрабатывает Debezium CDC."""
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None: continue
                if msg.error():
                    print(f" Kafka error: {msg.error()}")
                    continue

                topic = msg.topic()
                message = msg.value().decode('utf-8')
                print(f"📨 CDC '{topic}': {message[:100]}...")
                self.process_message(topic, message)
        finally:
            self.consumer.close()

    def process_message(self, topic, message):
        """Парсит Debezium payload."""
        try:
            data = json.loads(message)
            if 'payload' not in data or 'after' not in data['payload']:
                print(f" Skip non-CDC: {topic}")
                return

            payload = data['payload']
            business_data = payload['after']
            operation = payload['op']
            print(f"   Operation: {operation} | Data: {business_data}")

            #  Mapping topic → ddl_config
            config_key = topic.replace('.', '_')
            table_config = self.tables_config['ddl_config'].get(config_key)
            if not table_config:
                print(f" No config for {config_key}")
                return

            self.insert_data_vault(business_data, table_config, operation)
        except Exception as e:
            print(f" Process error: {e}")

    def create_hash(self, value):
        """Data Vault 2.0 HK (MD5 → BYTEA)."""
        if isinstance(value, dict):
            value = '|'.join([f"{k}:{v}" for k, v in sorted(value.items())])
        hash_bytes = hashlib.md5(str(value).encode()).digest()
        return hash_bytes  # ✅ PostgreSQL BYTEA

    def insert_data_vault(self, data, table_config, operation):
        """HUB/SAT/LINK по ddl_config.yaml."""
        table_type = table_config['type']
        business_key = table_config['business_key']
        source_system = table_config['source_system']
        schema = table_config['schema']
        table = table_config['table']

        with self.db_engine.connect() as conn:
            if table_type == 'hub':
                hk = self.create_hash(data[business_key])
                conn.execute(f"""
                    INSERT INTO {schema}.{table} (hk_{table}, {business_key}, load_dts, rec_src)
                    VALUES (%s, %s, NOW(), %s)
                    ON CONFLICT (hk_{table}) DO NOTHING
                """, (hk, data[business_key], source_system))
                print(f"   ✅ HUB {table}: {data[business_key]}")

            elif table_type == 'sat':
                hk = self.create_hash(data[business_key])
                hashdiff = self.create_hash(data)
                conn.execute(f"""
                    INSERT INTO {schema}.{table} (hk_{table[:-5]}, load_dts, rec_src, hashdiff, {business_key})
                    VALUES (%s, NOW(), %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (hk, source_system, hashdiff, data[business_key]))
                print(f"   ✅ SAT {table}: hashdiff={hashdiff[:8].hex()}")

        conn.commit()
        print(f"    {table_type.upper()} {table} OK!")

if __name__ == "__main__":
    kafka_config = {
        'bootstrap.servers': 'localhost:29092',  
        'group.id': 'dmp-vault-1',
        'auto.offset.reset': 'earliest'
    }
    
    processor = DMPProcessor(
        config_path="ddl_config.yaml",  
        db_connection_string="postgresql://postgres:postgres@postgres_dwh:5434/dwh_main",  # ✅ Порт 5434!
        kafka_config=kafka_config
    )
    
    print("Data Vault 2.0 DMP LIVE! Waiting CDC...")
    processor.process_kafka_messages()
