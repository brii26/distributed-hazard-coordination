# RabbitMQ topology

`definitions.json.tpl` is rendered at container start by `render-definitions.sh` (the user and password come from `RABBITMQ_USER` and `RABBITMQ_PASSWORD`, so nothing is committed), loaded from `/tmp/definitions.json` (`rabbitmq.conf`) and declares:

- the broker user (loading definitions suppresses RabbitMQ's default user, so it is declared here and every service connects with it), and
- one durable **fanout** exchange, `hazard.events` (TRD section 7), and
- one durable queue, `critical-alerts`, for critical Sensor Pantai alerts. The Aggregator sends to it directly (persistent, with publisher confirms) and only the Notifier consumes it, so a slow consumer of any other queue cannot delay it. Both sides declare it without arguments, so keep it that way.
- one durable queue, `field-reports`, the inbox behind `POST /field-reports`. The Client-Facing API publishes to it (persistent, with publisher confirms, 202 only after the confirm) and the field report consumer reads it, so declare it without arguments everywhere.

`RABBITMQ_EXCHANGE` must equal that name. The template is otherwise static, so renaming the exchange means editing it too. Use a password without `|` or `&`, which the `sed` render would misread.

## Conventions

- The Aggregator is the only producer. It publishes to `hazard.events` with `delivery_mode=2` (persistent) and never knows who is subscribed.
- Every consumer declares its **own durable queue** and binds it to `hazard.events` (e.g. `dashboard-updater`, `notifier`, `audit-logger`, from `QUEUE_NAME`). Never share a queue between consumers: a shared queue round-robins instead of fanning out, and a dead consumer would starve the others.
- Consumers use manual acknowledgement.
- Declarations are idempotent, so producer and consumers may re-declare the exchange (same type, durable) on startup.

Definitions apply on broker boot, including on an existing volume.
