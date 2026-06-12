package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// CheckKafkaConnection dials the Kafka broker and verifies it responds confirming the broker is reachable before the service starts.
func CheckKafkaConnection(broker string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return fmt.Errorf("kafka dial failed: %w", err)
	}
	defer conn.Close()

	// ApiVersions confirms the broker is responsive
	if _, err := conn.ApiVersions(); err != nil {
		return fmt.Errorf("kafka broker not responding: %w", err)
	}

	return nil
}
