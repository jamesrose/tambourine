package tambourine

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type SNSSQSAdapter struct {
	SQSConn *sqs.SQS
	SNSConn *sns.SNS
	Config  SNSSQSConfig
}

type SNSSQSConfig struct {
	Region          string
	QueueNamePrefix string
}

func NewSNSSQSAdapter(config SNSSQSConfig) SNSSQSAdapter {
	session := session.New()
	cfg := &aws.Config{Region: aws.String(config.Region)}
	return SNSSQSAdapter{
		SQSConn: sqs.New(session, cfg),
		SNSConn: sns.New(session, cfg),
		Config:  config,
	}
}

func (adapter SNSSQSAdapter) publish(queue Queue, message Message) error {
	name := queue.PrefixedName(adapter.Config.QueueNamePrefix)
	q, err := adapter.createPublishQueue(name)
	if err != nil {
		return err
	}
	input := &sns.PublishInput{
		Message:  aws.String(message.Body),
		TopicArn: q.TopicArn,
	}

	_, err = adapter.SNSConn.Publish(input)
	if err != nil {
		return err
	}
	return nil
}

func (adapter SNSSQSAdapter) createPublishQueue(name string) (*sns.CreateTopicOutput, error) {
	cti := &sns.CreateTopicInput{Name: aws.String(name)}
	return adapter.SNSConn.CreateTopic(cti)
}

func (adapter SNSSQSAdapter) createConsumeQueue(name string) (*sqs.CreateQueueOutput, error) {
	cqi := &sqs.CreateQueueInput{QueueName: aws.String(name)}
	return adapter.SQSConn.CreateQueue(cqi)
}

func (adapter SNSSQSAdapter) consume(queue Queue, worker string) ([]Message, error) {
	name := queue.PrefixedNameAndWorker(adapter.Config.QueueNamePrefix, worker)

	q, err := adapter.createConsumeQueue(name)
	if err != nil {
		return nil, err
	}

	rmi := &sqs.ReceiveMessageInput{
		QueueUrl: q.QueueUrl,
	}
	msgs, err := adapter.SQSConn.ReceiveMessage(rmi)
	if err != nil {
		return nil, err
	}
	messages := make([]Message, len(msgs.Messages))
	for i, m := range msgs.Messages {
		messages[i] = Message{Body: *m.Body}
	}
	return messages, nil
}
