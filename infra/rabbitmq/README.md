# RabbitMQ topology

`definitions.json` is loaded at broker start (`rabbitmq.conf`) and declares:

- the `guest` user (loading definitions suppresses RabbitMQ's default user, and every service connects as `guest:guest`), and
- one durable **fanout** exchange, `hazard.events` (TRD section 7).

`RABBITMQ_EXCHANGE` must equal that name. The definitions file is static, so renaming the exchange means editing it too.

## Conventions

- The Aggregator is the only producer. It publishes to `hazard.events` with `delivery_mode=2` (persistent) and never knows who is subscribed.
- Every consumer declares its **own durable queue** and binds it to `hazard.events` (e.g. `dashboard-updater`, `notifier`, `audit-logger`, from `QUEUE_NAME`). Never share a queue between consumers: a shared queue round-robins instead of fanning out, and a dead consumer would starve the others.
- Consumers use manual acknowledgement.
- Declarations are idempotent, so producer and consumers may re-declare the exchange (same type, durable) on startup.

Definitions apply on broker boot, including on an existing volume.
