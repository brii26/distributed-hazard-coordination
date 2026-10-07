#!/bin/sh
# Renders definitions.json from the template with the broker user from the environment, then starts RabbitMQ.
set -eu

sed -e "s|__RABBITMQ_USER__|${RABBITMQ_USER}|g" -e "s|__RABBITMQ_PASSWORD__|${RABBITMQ_PASSWORD}|g" \
  /etc/rabbitmq/definitions.json.tpl > /tmp/definitions.json

exec docker-entrypoint.sh rabbitmq-server
