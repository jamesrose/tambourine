package tambourine

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sts"
)

type SNSSQSAdapter struct {
	SQSConn *sqs.SQS
	SNSConn *sns.SNS
	STSConn *sts.STS
	Config  SNSSQSConfig
}

type SNSSQSConfig struct {
	Region          string
	QueueNamePrefix string
}

type Policy struct {
	Version   string
	ID        string `json:"Id"`
	Statement Statement
}

type Statement struct {
	Sid       string
	Effect    string
	Principal string
	Action    string
	Resource  string
	Condition Condition
}

type Condition struct {
	ArnEquals ArnEquals
}

type ArnEquals struct {
	SourceArn string `json:"aws:SourceArn"`
}

func NewSNSSQSAdapter(config SNSSQSConfig) SNSSQSAdapter {
	session := session.New()
	cfg := &aws.Config{Region: aws.String(config.Region)}
	return SNSSQSAdapter{
		SQSConn: sqs.New(session, cfg),
		SNSConn: sns.New(session, cfg),
		STSConn: sts.New(session, cfg),
		Config:  config,
	}
}

func (adapter SNSSQSAdapter) QueueNamePrefix() string {
	return adapter.Config.QueueNamePrefix
}

func (adapter SNSSQSAdapter) publish(queue Queue, message Message) error {
	name := queue.PrefixedName(adapter.QueueNamePrefix())
	pubQueue, err := adapter.createPublishQueue(name)
	if err != nil {
		return err
	}
	input := &sns.PublishInput{
		Message:  aws.String(message.Body),
		TopicArn: pubQueue.TopicArn,
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
	name := queue.PrefixedNameAndWorker(adapter.QueueNamePrefix(), worker)

	// Create consumer queue
	conQueue, err := adapter.createConsumeQueue(name)
	if err != nil {
		return nil, err
	}

	// Get consumer queue attributes
	attrInput := &sqs.GetQueueAttributesInput{
		QueueUrl: conQueue.QueueUrl,
		AttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameQueueArn),
		},
	}
	conQueueAttr, err := adapter.SQSConn.GetQueueAttributes(attrInput)
	if err != nil {
		return nil, err
	}
	conQueueARN := conQueueAttr.Attributes[sqs.QueueAttributeNameQueueArn]

	// Subscribe
	pubName := queue.PrefixedName(adapter.QueueNamePrefix())
	pubQueue, err := adapter.createPublishQueue(pubName)
	if err != nil {
		return nil, err
	}
	subReq := &sns.SubscribeInput{
		TopicArn: pubQueue.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: conQueueARN,
	}
	_, err = adapter.SNSConn.Subscribe(subReq)
	if err != nil {
		return nil, err
	}

	policy := Policy{
		ID:      name,
		Version: "2008-10-17",
		Statement: Statement{
			Sid:       "SendMessage",
			Effect:    "Allow",
			Principal: "*",
			Action:    "SQS:SendMessage",
			Resource:  *conQueueARN,
			Condition: Condition{
				ArnEquals: ArnEquals{
					SourceArn: *pubQueue.TopicArn,
				},
			},
		},
	}

	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return nil, err
	}

	// Add permission
	setAttrInput := &sqs.SetQueueAttributesInput{
		QueueUrl: conQueue.QueueUrl,
		Attributes: map[string]*string{
			sqs.QueueAttributeNamePolicy: aws.String(string(policyJSON)),
		},
	}
	_, err = adapter.SQSConn.SetQueueAttributes(setAttrInput)
	if err != nil {
		return nil, err
	}

	// Receive message
	rmi := &sqs.ReceiveMessageInput{
		QueueUrl: conQueue.QueueUrl,
	}
	msgs, err := adapter.SQSConn.ReceiveMessage(rmi)
	if err != nil {
		return nil, err
	}

	var message Message
	messages := make([]Message, len(msgs.Messages))
	for i, m := range msgs.Messages {
		if err := json.Unmarshal([]byte(*m.Body), &message); err != nil {
			return nil, err
		}
		messages[i] = message
	}
	return messages, nil
}
