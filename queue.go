package makeless_go_queue_amazon_sqs

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/makeless/makeless-go/queue"
	"sync"
)

type Queue struct {
	Context context.Context
	Queue   string
	Config  aws.Config

	client   *sqs.SQS
	queueUrl *string
	*sync.RWMutex
}

func (queue *Queue) GetQueue() string {
	queue.RLock()
	defer queue.RUnlock()

	return queue.Queue
}

func (queue *Queue) GetConfig() aws.Config {
	queue.RLock()
	defer queue.RUnlock()

	return queue.Config
}

func (queue *Queue) getClient() *sqs.SQS {
	queue.RLock()
	defer queue.RUnlock()

	return queue.client
}

func (queue *Queue) setClient(client *sqs.SQS) {
	queue.Lock()
	defer queue.Unlock()

	queue.client = client
}

func (queue *Queue) getQueueUrl() *string {
	queue.RLock()
	defer queue.RUnlock()

	return queue.queueUrl
}

func (queue *Queue) setQueueUrl(queueUrl *string) {
	queue.Lock()
	defer queue.Unlock()

	queue.queueUrl = queueUrl
}

func (queue *Queue) Init() error {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: queue.GetConfig(),
	}))

	client := sqs.New(sess)

	name := queue.GetQueue()
	result, err := client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &name,
	})

	if err != nil {
		return err
	}

	queue.setClient(sqs.New(sess))
	queue.setQueueUrl(result.QueueUrl)
	return nil
}

func (queue *Queue) GetContext() context.Context {
	queue.RLock()
	defer queue.RUnlock()

	return queue.Context
}

func (queue *Queue) Add(node makeless_go_queue.Node) error {
	_, err := queue.getClient().SendMessage(&sqs.SendMessageInput{
		DelaySeconds:      aws.Int64(0),
		MessageAttributes: nil,
		MessageBody:       aws.String(string(node.GetData())),
		QueueUrl:          queue.getQueueUrl(),
	})

	return err
}

func (queue *Queue) Remove() (makeless_go_queue.Node, error) {
	result, err := queue.getClient().ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            queue.getQueueUrl(),
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(1),
	})

	if err != nil {
		return nil, err
	}

	if len(result.Messages) == 0 {
		return nil, nil
	}

	return &Node{
		Data:          []byte(*result.Messages[0].Body),
		ReceiptHandle: *result.Messages[0].ReceiptHandle,
		RWMutex:       new(sync.RWMutex),
	}, nil
}

func (queue *Queue) Empty() (bool, error) {
	return false, fmt.Errorf("empty not supported by aws sqs")
}

func (queue *Queue) Delete(node *Node) error {
	var receiptHandle = node.GetReceiptHandle()
	_, err := queue.getClient().DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      queue.getQueueUrl(),
		ReceiptHandle: &receiptHandle,
	})

	return err
}
