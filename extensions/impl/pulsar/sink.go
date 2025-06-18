package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/lf-edge/ekuiper/contract/v2/api"
)

type PulsarSink struct {
	producer pulsar.Producer
}

func (p *PulsarSink) Provision(ctx api.StreamContext, props map[string]any) error {
	return nil
}
