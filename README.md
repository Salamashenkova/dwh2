## Авторы
1. **Полина Кияшко**, [shinshiiila](https://t.me/shinshiiila)
3. **Саламашенкова Дарья**, [salamashenkovadasha](https://t.me/salamashenkovadasha)

ФТиАД

## Как запустить проект

Клонирование проекта из этого репо:
```
git clone https://github.com/Salamashenkova/dwh2
```
Запуск инициализации БД:
```
docker-compose up -d
```
Параметры для подключения:
Мастер
```
docker exec -it postgres_master psql -U postgres
```
DWH:
```
docker exec -it postgres_dwh psql -U postgres
```
Примеры вывода таблиц для Мастер:
```
docker exec -it postgres_master psql -U postgres -d user_service_db -c "SELECT * FROM user_service_db.USERS LIMIT 3;"

docker exec -it postgres_master psql -U postgres -d order_service_db -c "SELECT * FROM order_service_db.ORDERS LIMIT 3;"

docker exec -it postgres_master psql -U postgres -d logistics_service_db -c "SELECT * FROM logistics_service_db.warehouses LIMIT 5;"
 
```
Вывод списка всех таблиц в DWH:
```
docker exec -it postgres_dwh psql -U postgres -d dwh_main -c "\dt dwh_detailed.*"
```

Пример вывода таблицы в DWH:
```
docker exec -it postgres_dwh psql -U postgres -d dwh_main -c "SELECT * FROM dwh_detailed.hub_user;"
```
Подкючение к debezium
```
curl -X POST -H "Content-Type: application/json" http://localhost:8083/connectors \
  -d '{
    "name": "user-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres_master",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "postgres",
      "database.dbname": "user_service_db",
      "topic.prefix": "dbserver1",
      "plugin.name": "pgoutput",
      "publication.name": "user_publication",
      "slot.name": "user_slot"
    }
  }'
```
```
curl -X POST -H "Content-Type: application/json" http://localhost:8083/connectors \
  -d '{
    "name": "order-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres_master",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "postgres",
      "database.dbname": "order_service_db",
      "topic.prefix": "dbserver1",
      "plugin.name": "pgoutput",
      "publication.name": "order_publication",
      "slot.name": "order_slot",
      "table.include.list": "order_service_db.ORDERS,order_service_db.ORDER_ITEMS"
    }
  }'
```
```
curl -X POST -H "Content-Type: application/json" http://localhost:8083/connectors \
  -d '{
    "name": "logistics-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres_master",
      "database.port": "5432", 
      "database.user": "postgres",
      "database.password": "postgres",
      "database.dbname": "logistics_service_db",
      "topic.prefix": "dbserver1",
      "plugin.name": "pgoutput",
      "publication.name": "logistics_publication",
      "slot.name": "logistics_slot",
      "table.include.list": "logistics_service_db.WAREHOUSES,logistics_service_db.PICKUP_POINTS"
    }
  }'
```
Топики Kafka
```
 docker exec broker kafka-topics --bootstrap-server localhost:29092 --list
```
## Результаты:

![alt text](photo_1.jpg)

![alt text](photo_2.jpg)

![alt text](photo_3.jpg)

![alt text](photo_4.png)

![alt text](photo_5.png)
<img width="1280" height="720" alt="image" src="https://github.com/user-attachments/assets/90989bf7-0640-4494-998b-1f829a365c2c" />
<img width="1920" height="1080" alt="Снимок экрана (270)" src="https://github.com/user-attachments/assets/fcd3ac0b-ad6d-4ea4-a43f-aa3803b7a3e7" />
