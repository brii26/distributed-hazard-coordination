// Command fieldpub is a test helper for the scenario script: publish a field report or print the queue depth.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const queue = "field-reports"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: fieldpub depth | pub ID [BODY|BIG|HUGE|NUL]")
		os.Exit(2)
	}

	conn, err := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	ch, err := conn.Channel()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if os.Args[1] == "depth" {
		q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		fmt.Println(q.Messages)

		return
	}

	id := os.Args[2]
	body := fmt.Sprintf(`{"report_id":%q,"description":"banjir","latitude":-6.2,"longitude":106.8,"reporter_id":"a@b.c","reporter_role":"warga","received_at":"2026-11-18T10:00:00Z"}`, id)

	if len(os.Args) > 3 {
		body = os.Args[3]
		switch body {
		case "BIG": // 1 MiB: sent whole, malformed
			body = strings.Repeat("x", 1<<20)
		case "HUGE": // 5 MiB: over the 4 MiB gRPC limit of the Aggregator
			body = strings.Repeat("x", 5<<20)
		case "NUL": // valid JSON and UTF-8, refused by Postgres
			body = fmt.Sprintf(`{"report_id":%q,"description":"banjir \u0000 x","reporter_id":"a@b.c","reporter_role":"warga","received_at":"2026-11-18T10:00:00Z"}`, id)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	err = ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		MessageId: id, CorrelationId: "c-" + id, DeliveryMode: amqp.Persistent, Body: []byte(body),
	})

	cancel()

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
