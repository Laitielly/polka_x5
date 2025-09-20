#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

echo "==> Очищаю ./certs (будь осторожен)"
rm -f ./server.* ./domain.* ./server.keystore.p12 ./client*.p12 ./client.truststore.jks ./server1.creds

echo "==> Создаём CA (self-signed)"
openssl req -new -x509 -nodes -days 3650 \
  -subj "/CN=MyLocalCA" \
  -keyout ca.key -out ca.crt

echo "==> Создаём server cert (CN=localhost) с SAN (localhost,127.0.0.1)"
openssl req -new -nodes -subj "/CN=localhost" -newkey rsa:2048 -keyout server.key -out server.csr
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 365 \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1") -sha256 -out server.crt

echo "==> Экспортируем server в PKCS12 (пароль server1)"
openssl pkcs12 -export -in server.crt -inkey server.key -name kafka -out server.keystore.p12 -passout pass:server1

echo "==> Создаём client keystore для admin/logger/consumer (PKCS12), CN соответствует principal User:CN=<name>"
for name in admin logger consumer; do
  echo " -> генерируем ключ и сертификат для ${name}"
  openssl req -new -nodes -subj "/CN=${name}" -newkey rsa:2048 -keyout ${name}.key -out ${name}.csr
  openssl x509 -req -in ${name}.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 365 -sha256 -out ${name}.crt
  openssl pkcs12 -export -in ${name}.crt -inkey ${name}.key -name ${name} -out ${name}.p12 -passout pass:client1
done

echo "==> Создаём client.truststore.jks и импортируем CA (пароль 123456)"
keytool -keystore client.truststore.jks -alias CARoot -import -file ca.crt -storepass 123456 -noprompt

echo "==> Создаём файл creds для server keystore (содержит пароль 'server1')"
printf "server1" > server1.creds
chmod 600 server1.creds

echo "==> Создаём файл truststore для server truststore (содержит пароль '123456')"
printf "123456" > truststore.creds
chmod 600 truststore.creds

echo "==> Переместим/сохраним некоторые файлы (в корне ./certs):"
ls -l

echo "==> ФАЙЛЫ: (переместить все созданные сюда, они уже в ./certs если запускали внутри папки)"
echo "server.keystore.p12, client.truststore.jks, admin.p12, logger.p12, consumer.p12, ca.crt, server1.creds"

echo "Генерация завершена."
