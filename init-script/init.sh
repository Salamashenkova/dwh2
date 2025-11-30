#!/bin/bash
set -euo pipefail

# Docker PostgreSQL репликация (postgres_master → postgres_slave)
MASTER_HOST="postgres_master"
MASTER_PORT="5432"
REPLICATOR_USER="replicator"
REPLICATOR_PASSWORD="replicator_pass"
REPLICATION_SLOT="debezium_replication_slot"
POSTGRES_USER="postgres"
POSTGRES_DB="postgres"

echo "=== Docker PostgreSQL Репликация MASTER → SLAVE ==="

# 1.  НАСТРОЙКА MASTER (docker exec!)
echo "1. Настройка репликации на postgres_master..."
docker exec postgres_master psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<-EOSQL
    --  WAL_LEVEL для Debezium + репликации
    ALTER SYSTEM SET wal_level = 'logical';
    ALTER SYSTEM SET max_replication_slots = 10;
    ALTER SYSTEM SET max_wal_senders = 10;
    SELECT pg_reload_conf();
    
    --  Пользователь репликации
    DROP ROLE IF EXISTS $REPLICATOR_USER;
    CREATE USER $REPLICATOR_USER WITH REPLICATION ENCRYPTED PASSWORD '$REPLICATOR_PASSWORD';
    
    --  Replication Slot (Debezium + Slave)
    SELECT pg_create_physical_replication_slot('$REPLICATION_SLOT') 
    WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '$REPLICATION_SLOT');
EOSQL

# 2.  pg_hba.conf (разрешить репликацию)
docker exec postgres_master bash -c "
cat >> /var/lib/postgresql/data/pg_hba.conf <<EOF
# Репликация + Debezium
host replication $REPLICATOR_USER 0.0.0.0/0 md5
local replication $REPLICATOR_USER peer
EOF
pg_ctl reload -D /var/lib/postgresql/data
"

# 3.  Basebackup → Slave
echo "2. Basebackup master → slave..."
docker exec postgres_master pg_basebackup -D /tmp/slave_data \
    -h localhost -p 5432 -U "$REPLICATOR_USER" \
    -Fp -Xs -P -R \
    --wal-method=stream

# 4.  Копирование в Slave volume
docker run --rm -v postgres_slave_data:/slave_data -v $(pwd):/backup \
    postgres:13 bash -c "cp -r /tmp/slave_data/* /slave_data/"

# 5.  Slave postgresql.conf (PG12+ формат!)
docker run --rm -v postgres_slave_data:/var/lib/postgresql/data postgres:13 bash -c "
cat > /var/lib/postgresql/data/postgresql.conf <<EOF
primary_conninfo = 'host=postgres_master port=5432 user=$REPLICATOR_USER passfile=/var/lib/postgresql/data/.replication_pass'
primary_slot_name = '$REPLICATION_SLOT'
hot_standby = on
wal_receiver_timeout = 60s
EOF
echo '$REPLICATOR_PASSWORD' > /var/lib/postgresql/data/.replication_pass
chmod 600 /var/lib/postgresql/data/.replication_pass
"

echo " Репликация настроена!"
echo "  Master: postgres_master:5432 (slot: $REPLICATION_SLOT)"
echo "  Slave: postgres_slave:5433"
echo "  User: $REPLICATOR_USER"

echo " Запуск: docker-compose up -d postgres_slave"
