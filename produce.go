package kafka

import (
	"github.com/Shopify/sarama"
	"strings"
)

type Produce struct {
	brokerList []string
	config     *sarama.Config
	producer   sarama.SyncProducer
}

func NewKafkaProduce(bootstrapServers string) *Produce {
	produce := &Produce{
		config: sarama.NewConfig(),
	}

	produce.config.Producer.RequiredAcks = sarama.WaitForAll
	produce.config.Producer.Retry.Max = 5
	produce.config.Producer.Return.Successes = true

	produce.brokerList = strings.Split(bootstrapServers, ",")
	return produce
}

func (k *Produce) Send(mess *sarama.ProducerMessage) error {
	_, _, err := k.producer.SendMessage(mess)
	if err != nil {
		return err
	}
	return nil
}

func (k *Produce) Sends(mess []*sarama.ProducerMessage) error {
	_, _, err := k.producer.SendMessages(mess)
	if err != nil {
		return err
	}
	return nil
}

func (k *Produce) Stop() error {
	return k.producer.Close()
}

func (k *Produce) Run() error {
	var err error
	k.producer, err = sarama.NewSyncProducer(k.brokerList, k.config)
	if err != nil {
		return err
	}
	return nil
}
