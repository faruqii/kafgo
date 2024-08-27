package main

import (
	"context"
	"fmt"
	"log"

	"github.com/gofiber/fiber/v2"
	"github.com/segmentio/kafka-go"
)

const (
	kafkaTopic   = "orders"
	kafkaBroker  = "localhost:9092"
	kafkaGroupID = "order-processor"
)

func main() {
	app := fiber.New()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   kafkaTopic,
		GroupID: kafkaGroupID,
	})

	app.Get("/process-orders", func(c *fiber.Ctx) error {
		ctx := context.Background()
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}

		orderDetails := string(m.Value)
		fmt.Printf("Processing order: %s\n", orderDetails)

		return c.JSON(fiber.Map{
			"message": "Order processed",
			"order":   orderDetails,
			"offset":  m.Offset,
		})
	})

	log.Fatal(app.Listen(":3002"))
}
