package pulsar

import (
	"errors"
	"fmt"
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

type PulsarSource struct {
	clientCenter *PulsarClientCenter
	pulsarClient pulsar.Client
	consumer     pulsar.Consumer
	msgId        *pulsar.MessageID
	sc           *PulsarSourceConf
	sch          api.StatusChangeHandler
	connected    bool
}

type PulsarSourceConf struct {
	PulsarClientConf
	Topic            string `json:"topic"`
	RegexTopic       string `json:"regex_topic"`
	ConsumerName     string `json:"consumer_name"`
	SubscriptionName string `json:"subscription_name"`
	SubType          string `json:"subscription_type"`
}

var valid_subscription_type = []string{"exclusive", "shared", "failover", "keyshared"}

func StringToSubType(subType string) pulsar.SubscriptionType {
	switch subType {
	case "exclusive":
		return pulsar.Exclusive
	case "shared":
		return pulsar.Shared
	case "failover":
		return pulsar.Failover
	case "keyshared":
		return pulsar.KeyShared
	default:
		return pulsar.Exclusive
	}
}

func (c *PulsarSourceConf) validate() error {
	if c.Topic == "" && c.RegexTopic == "" {
		return fmt.Errorf("either topic or regex_topic in PulsarSourceConf must exist")
	}
	sub_type_validated := false
	for _, typ := range valid_subscription_type {
		if typ == c.SubType {
			sub_type_validated = true
			break
		}
	}
	if !sub_type_validated {
		return fmt.Errorf("subscription type can only be one of: %v", valid_subscription_type)
	}
	return c.PulsarClientConf.validate()
}

func (c *PulsarSourceConf) GetConsumerConfig() (pulsar.ConsumerOptions, error) {
	options := pulsar.ConsumerOptions{}
	if c.Topic != "" {
		if !strings.Contains(c.Topic, ";") {
			options.Topic = c.Topic
		} else {
			topics := make([]string, 0, 4)
			for _, topic := range strings.Split(c.Topic, ";") {
				topic = strings.TrimSpace(topic)
				if topic == "" {
					continue
				}
				topics = append(topics, topic)
			}
			if len(topics) > 0 {
				options.Topics = topics
			}
		}
	} else {
		options.TopicsPattern = c.RegexTopic
	}
	options.SubscriptionName = c.ConsumerName
	options.Type = StringToSubType(c.SubType)
	options.SubscriptionMode = pulsar.Durable
	return options, nil
}

func getSourceConf(props map[string]interface{}) (*PulsarSourceConf, error) {
	c := &PulsarSourceConf{}
	err := MapConfigToStruct(props, c)
	if err != nil {
		return nil, fmt.Errorf("read properties %v failed with error: %v", props, err)
	}
	return c, nil
}

func (p *PulsarSource) Provision(ctx api.StreamContext, props map[string]any) error {
	sconf, err := getSourceConf(props)
	if err != nil {
		conf.Log.Errorf("pulsar source config error:%v", err)
		return err
	}
	if err := sconf.validate(); err != nil {
		conf.Log.Errorf("pulsar source config error:%v", err)
		return err
	}
	clientConf, err := sconf.GetClientConfig()
	if err != nil {
		conf.Log.Errorf("failed to create client config from source config due to: %v", err)
		return err
	}
	p.pulsarClient, err = p.clientCenter.CreateClient(clientConf)
	if err != nil {
		conf.Log.Errorf("failed to create pulsar client due to : %v", err)
		return err
	}
	p.sc = sconf
	return nil
}

func (p *PulsarSource) Ping(ctx api.StreamContext, props map[string]any) error {
	if err := p.Provision(ctx, props); err != nil {
		return err
	}
	p.Close(nil)
	return nil
}

func (p *PulsarSource) Close(ctx api.StreamContext) error {
	if p.consumer != nil {
		p.consumer.Close()
	}
	if p.pulsarClient != nil {
		p.pulsarClient.Close()
	}
	if p.sc != nil {
		p.sc = nil
	}
	return nil
}

func (p *PulsarSource) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	if p.pulsarClient == nil {
		conf.Log.Errorf("provision pulsar before connect to broker and subscribe topic")
		return fmt.Errorf("provision pulsar before connect to broker and subscribe topic")
	}
	p.connected = false
	consumerConf, err := p.sc.GetConsumerConfig()
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		conf.Log.Errorf("failed to get consumer config from source config due to: %v", err)
		return err
	}
	p.consumer, err = p.pulsarClient.Subscribe(consumerConf)
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		conf.Log.Errorf("failed to subscribe topic due to: %v", err)
		return err
	}
	if p.msgId != nil {
		err = p.consumer.Seek(*p.msgId)
		if err != nil {
			sch(api.ConnectionDisconnected, err.Error())
			conf.Log.Errorf("failed to seek to message id: %v due to: %v", p.msgId, err)
			return err
		} else {
			conf.Log.Infof("seek to message id: %v", p.msgId)
		}
	}
	sch(api.ConnectionConnected, "")
	p.sch = sch
	p.connected = true
	return nil
}

func (p *PulsarSource) Subscribe(ctx api.StreamContext, ingest api.BytesIngest, errIngest api.ErrorIngest) error {
	if !p.connected {
		return fmt.Errorf("pulsar source not connected")
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		msg, err := p.consumer.Receive(ctx)
		p.handleConnectedSch(err)
		if err != nil {
			errIngest(ctx, err)
			return err
		}
		props := make(map[string]any)
		for k, v := range msg.Properties() {
			props[k] = v
		}
		ingest(ctx, msg.Payload(), props, msg.PublishTime())
	}
}

func (p *PulsarSource) handleConnectedSch(err error) {
	if p.connected && err != nil {
		p.connected = false
		p.sch(api.ConnectionDisconnected, err.Error())
	} else if !p.connected && err == nil {
		p.sch(api.ConnectionConnected, "")
	}
}

func (p *PulsarSource) Rewind(offset interface{}) error {
	conf.Log.Infof("rewind pulsar source to offset: %v", offset)
	msgId, ok := offset.(pulsar.MessageID)
	if !ok {
		return fmt.Errorf("invalid offset type: %v", offset)
	}
	err := p.consumer.Seek(msgId)
	if err != nil {
		conf.Log.Errorf("failed to seek to message id: %v due to: %v", msgId, err)
		return err
	}
	return nil
}

func (p *PulsarSource) ResetOffset(input map[string]interface{}) error {
	return errors.New("kafka source not support reset offset")
}

func (p *PulsarSource) GetOffset() (interface{}, error) {
	return p.msgId, nil
}

func GetSource() api.Source {
	return &PulsarSource{
		clientCenter: GetClientCenter(),
	}
}

var (
	_ api.BytesSource = &PulsarSource{
		clientCenter: GetClientCenter(),
	}
)
