package main

import (
	"context"
	"fmt"
	"log"

	"github.com/gofiber/fiber/v2"
	"github.com/segmentio/kafka-go"
)

const (
	kafkaTopic  = "orders"
	kafkaBroker = "localhost:9092"
)

func main() {
	app := fiber.New()

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	})

	app.Post("/order", func(c *fiber.Ctx) error {
		type OrderRequest struct {
			OrderID    string  `json:"order_id"`
			CustomerID string  `json:"customer_id"`
			Amount     float64 `json:"amount"`
		}

		var req OrderRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot parse JSON"})
		}

		message := fmt.Sprintf("OrderID: %s, CustomerID: %s, Amount: %f", req.OrderID, req.CustomerID, req.Amount)
		err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(req.OrderID),
				Value: []byte(message),
			},
		)

		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}

		return c.JSON(fiber.Map{"status": "order placed", "order_id": req.OrderID})
	})

	log.Fatal(app.Listen(":3001"))
}
