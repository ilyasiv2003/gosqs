package gosqs

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"

	"github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func TestClient_StartConsumers(t *testing.T) {
	msg := "lol"
	qName := "img-moderation-results"

	newSqs := sqs.New(sqs.Options{Credentials: credentials.NewStaticCredentialsProvider(
		"",
		"",
		"",
	)}, func(options *sqs.Options) {
		options.Region = "us-east-1"
	})

	cli := NewClient(newSqs, logrus.New())

	testHandler := onceHandlerHelper{t: t, expectedMsg: msg}

	err := cli.StartConsumers(map[string]Handler{
		qName: testHandler.Handle,
	})
	require.NoError(t, err)

	qUrlOut, err := newSqs.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: aws.String(qName)})
	require.NoError(t, err)

	_, err = newSqs.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:    qUrlOut.QueueUrl,
		MessageBody: aws.String(msg),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return testHandler.Invoked() }, 10*time.Second, 300*time.Microsecond)

	// Ensure handler not called twice, might be fluky if queue is not fifo
	time.Sleep(5 * time.Second)
}

type onceHandlerHelper struct {
	t *testing.T
	sync.Mutex
	invoked     bool
	expectedMsg string
}

func (h *onceHandlerHelper) Handle(message []byte) error {
	h.Lock()
	defer h.Unlock()

	require.False(h.t, h.invoked)
	require.Equal(h.t, h.expectedMsg, string(message))

	h.invoked = true

	return nil
}

func (h *onceHandlerHelper) Invoked() bool {
	h.Lock()
	defer h.Unlock()

	return h.invoked
}
