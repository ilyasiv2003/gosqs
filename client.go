package gosqs

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const maxMessages = 10

type Client struct {
	client *sqs.Client
	log    *logrus.Logger
}

func NewClient(client *sqs.Client, log *logrus.Logger) *Client {
	return &Client{client: client, log: log}
}

type Handler func(message []byte) error

// StartConsumers doesn't have ctx for now, assuming it should consume for the whole program lifespan
func (c Client) StartConsumers(consumers map[string]Handler) error {
	for qName, handler := range consumers {
		qUrlOut, err := c.client.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: aws.String(qName)})
		if err != nil {
			return err
		}

		go c.startConsumer(qName, qUrlOut.QueueUrl, handler)
	}

	return nil
}

func (c Client) startConsumer(qName string, qUrl *string, handler Handler) {
	log := c.log.WithField("queue_name", qName)

	for {
		output, err := c.client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl:            qUrl,
			MaxNumberOfMessages: maxMessages,
		})
		if err != nil {
			log.WithError(err).Error("failed to receive message from sqs")
			continue
		}

		var wg sync.WaitGroup
		for _, message := range output.Messages {
			wg.Add(1)

			go func(m types.Message) {
				defer wg.Done()

				err = handler([]byte(*m.Body))
				if err != nil {
					log.WithError(err).Error("handler returned error")
					return
				}

				_, err = c.client.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
					QueueUrl:      qUrl,
					ReceiptHandle: m.ReceiptHandle,
				})
				if err != nil {
					log.WithError(err).Error("failed to delete message from sqs")
				}
			}(message)
		}

		wg.Wait()
	}
}
