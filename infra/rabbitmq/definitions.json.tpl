{
  "users": [
    {
      "name": "__RABBITMQ_USER__",
      "password": "__RABBITMQ_PASSWORD__",
      "tags": [
        "administrator"
      ]
    }
  ],
  "vhosts": [
    {
      "name": "/"
    }
  ],
  "permissions": [
    {
      "user": "__RABBITMQ_USER__",
      "vhost": "/",
      "configure": ".*",
      "write": ".*",
      "read": ".*"
    }
  ],
  "exchanges": [
    {
      "name": "hazard.events",
      "vhost": "/",
      "type": "fanout",
      "durable": true,
      "auto_delete": false,
      "internal": false,
      "arguments": {}
    }
  ]
}
