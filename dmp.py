import yaml
import json
import hashlib
import pandas as pd
from sqlalchemy import create_engine
from confluent_kafka import Consumer, KafkaException
from datetime import datetime

class DMPProcessor:
    def __init__(self, config_path, db_connection_string, kafka_config):
        """Инициализация процессора."""
        self.config_path = config_path
        self.db_engine = create_engine(db_connection_string)
        self.kafka_config = kafka_config
        self.tables_config = self.load_config()
        self.consumer = self.setup_kafka_consumer()

    def load_config(self):
        """Загружает конфигурационный файл YAML."""
        with open(self.config_path, 'r') as file:
            return yaml.safe_load(file)

    def setup_kafka_consumer(self):
        """Настраивает Kafka Consumer."""
        consumer = Consumer(self.kafka_config)
        topics = list(self.tables_config['tables'].keys())
        consumer.subscribe(topics)
        print(f"Subscribed to topics: {topics}")
        return consumer

    def process_kafka_messages(self):
        """Обрабатывает сообщения из Kafka."""
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Kafka error: {msg.error()}")
                    continue

                topic = msg.topic()
                message = msg.value().decode('utf-8')
                print(f"Processing message from topic '{topic}': {message}")
                self.process_message(topic, message)
        finally:
            self.consumer.close()

    def process_message(self, topic, message):
        """Обрабатывает сообщение из Kafka."""
        table_config = self.get_table_config(topic)
        if not table_config:
            print(f"No configuration found for topic: {topic}")
            return

        data = json.loads(message)
        self.insert_data(data, table_config)

    def get_table_config(self, topic):
        """Возвращает конфигурацию таблицы для топика."""
        return self.tables_config['tables'].get(topic)

    def create_hash(self, value):
        """Создает хэш из значения."""
        if isinstance(value, (list, tuple)):
            value = ''.join(map(str, value))
        return hashlib.sha256(str(value).encode()).hexdigest()

    def insert_data(self, data, table_config):
        """Вставляет данные в таблицу."""
        schema = table_config['schema']
        table = table_config['table']
        technical_fields = table_config.get('technical_fields', [])

        # Обогащаем данные техническими полями
        data['source_system_id'] = 1
        data['created_at'] = datetime.utcnow()
        data['version'] = 1

        # Создаем DataFrame
        df = pd.DataFrame([data])

        # Вставляем в базу данных
        df.to_sql(
            table,
            self.db_engine,
            schema=schema,
            if_exists='append',
            index=False
        )
        print(f"Data inserted into {schema}.{table}")

# Пример использования
if __name__ == "__main__":
    # Конфигурация подключения к Kafka
    # kafka_config = {
    #     'bootstrap.servers': 'localhost:9092',
    #     'group.id': 'dmp-consumer',
    #     'auto.offset.reset': 'earliest'
    # }

    kafka_config = {
        'bootstrap.servers': 'broker:29092',
        'group.id': 'dmp-consumer',
        'auto.offset.reset': 'earliest'
    }
    # kafka_config = {
    #     'bootstrap.servers': 'localhost:9092',
    #     'group.id': 'dmp-consumer',
    #     'auto.offset.reset': 'earliest'
    # }

    # Инициализация процессора
    processor = DMPProcessor(
        config_path="dmp_config.yaml",
        db_connection_string="postgresql://username:password@localhost:5432/postgres",
        kafka_config=kafka_config
    )

    # Запуск обработки сообщений
    processor.process_kafka_messages()
