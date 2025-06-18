package main

import (
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/pulsar"
)

func Pulsar() api.Source {
	return pulsar.GetSource()
}

func PulsarLookup() api.Source {
	return pulsar.GetSource()
}
