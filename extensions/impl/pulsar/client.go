package pulsar

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
)

type PulsarClientCenter struct {
	mu sync.Mutex
}

func (c *PulsarClientCenter) CreateClient(conf pulsar.ClientOptions) (pulsar.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	client, err := pulsar.NewClient(conf)
	if err != nil {
		return nil, err
	}
	return client, nil
}

type PulsarClientConf struct {
	URL                 string `json:"url"`
	AccessToken         string `json:"token"`
	TLSKey              string `json:"tls_key"`
	TLSCert             string `json:"tls_cert"`
	TLSCA               string `json:"tls_ca"`
	TLSAllowInsecure    bool   `json:"tls_allow_insecure"`
	TLSValidateHostname bool   `json:"tls_validate_hostname"`
	ListenerName        string `json:"pulsar_listener_name"`
}

func (c *PulsarClientConf) validate() error {
	if c.URL == "" {
		return fmt.Errorf("PulsarClientConf URL is required")
	}
	return nil
}

func (c *PulsarClientConf) GetClientConfig() (pulsar.ClientOptions, error) {
	clientOptions := pulsar.ClientOptions{}
	clientOptions.URL = c.URL
	if c.AccessToken != "" {
		clientOptions.Authentication = pulsar.NewAuthenticationToken(c.AccessToken)
	}
	if c.TLSKey != "" && c.TLSCert != "" && c.TLSCA != "" {
		cert, err := tls.LoadX509KeyPair(c.TLSCert, c.TLSKey)
		if err != nil {
			return clientOptions, err
		}
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM([]byte(c.TLSCA))

		clientOptions.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientCAs:    certPool,
		}
		clientOptions.TLSAllowInsecureConnection = c.TLSAllowInsecure
		clientOptions.TLSValidateHostname = c.TLSValidateHostname
	}
	return clientOptions, nil
}

var _clientCenter = &PulsarClientCenter{}

func GetClientCenter() *PulsarClientCenter {
	return _clientCenter
}
