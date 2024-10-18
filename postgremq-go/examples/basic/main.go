package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	postgremq "github.com/slavakl/postgremq/postgremq-go"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	// Get database URL from environment or use default
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}

	ctx := context.Background()

	// Parse connection config
	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	// Connect to PostgreMQ
	conn, err := postgremq.Dial(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	log.Println("Connected to PostgreMQ")

	// Create topic and queue
	topicName := "orders"
	queueName := "order-processor"

	if err := conn.CreateTopic(ctx, topicName); err != nil {
		log.Printf("Topic might already exist: %v", err)
	}

	if err := conn.CreateQueue(ctx, queueName, topicName, false); err != nil {
		log.Printf("Queue might already exist: %v", err)
	}

	log.Printf("Topic '%s' and queue '%s' are ready", topicName, queueName)

	// Publish some messages
	for i := 1; i <= 5; i++ {
		payload := json.RawMessage(fmt.Sprintf(`{"order_id": %d, "total": %d.99}`, i, i*10))
		messageID, err := conn.Publish(ctx, topicName, payload)
		if err != nil {
			log.Printf("Failed to publish message: %v", err)
			continue
		}
		log.Printf("Published message %d with ID: %d", i, messageID)
	}

	// Create consumer
	consumer, err := conn.Consume(ctx, queueName,
		postgremq.WithBatchSize(5),
		postgremq.WithVT(30))
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Stop()

	log.Println("Started consuming messages...")

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Process messages
	go func() {
		for msg := range consumer.Messages() {
			log.Printf("Received message ID %d: %s", msg.ID, string(msg.Payload))

			// Simulate processing
			time.Sleep(1 * time.Second)

			// Acknowledge the message
			if err := msg.Ack(ctx); err != nil {
				log.Printf("Failed to ack message %d: %v", msg.ID, err)
			} else {
				log.Printf("Acknowledged message %d", msg.ID)
			}
		}
	}()

	// Wait for signal
	<-sigChan
	log.Println("Shutting down gracefully...")
}
