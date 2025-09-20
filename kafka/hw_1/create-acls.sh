#!/usr/bin/env bash
set -e

echo "Waiting for kafka to be ready..."
sleep 8

# Используем bootstrap-server 127.0.0.1:9092 и ssl client config,
# но внутри контейнера запустим команды с переменной окружения, чтобы kafka утилиты использовали ssl
# Скопируем клиентский truststore внутрь /tmp для команды в контейнере

echo "Добавляем ACL'ы внутри контейнера..."

# 1) создаём в контейнере конфиг для admin, который содержит keystore (admin.p12) и truststore
docker-compose exec kafka bash -lc "cat > /tmp/admin-ssl.properties <<'EOF'
security.protocol=SSL
ssl.keystore.location=/etc/kafka/secrets/admin.p12
ssl.keystore.password=client1
ssl.keystore.type=PKCS12
ssl.key.password=client1
ssl.truststore.location=/etc/kafka/secrets/client.truststore.jks
ssl.truststore.password=123456
# отключить hostname verification если понадобится (обычно не нужно, у нас есть SAN)
# ssl.endpoint.identification.algorithm=
EOF
"

# 2) проверим, что admin может подключиться (список ACL как тест)
docker-compose exec kafka bash -lc "kafka-acls --bootstrap-server 127.0.0.1:9092 --list --command-config /tmp/admin-ssl.properties 2>&1 | sed -n '1,200p'"

# 3) если предыдущая команда прошла — добавляем ACL'ы (пример для logger и consumer)
docker-compose exec kafka bash -lc "
kafka-acls --bootstrap-server 127.0.0.1:9092 \
  --add --allow-principal User:CN=logger --operation Write --operation Read \
  --topic 'log-' --resource-pattern-type prefixed \
  --command-config /tmp/admin-ssl.properties

kafka-acls --bootstrap-server 127.0.0.1:9092 \
  --add --allow-principal User:CN=consumer --operation Read \
  --topic '*' --resource-pattern-type literal \
  --command-config /tmp/admin-ssl.properties

kafka-acls --bootstrap-server 127.0.0.1:9092 \
  --add --allow-principal User:CN=consumer --operation Read --group '*' \
  --command-config /tmp/admin-ssl.properties

echo 'ACLs created (via admin.p12)'
"

# 4) проверка: покажем ACL'ы
docker-compose exec kafka bash -lc "kafka-acls --bootstrap-server 127.0.0.1:9092 --list --command-config /tmp/admin-ssl.properties"


echo "ACL creation finished."
