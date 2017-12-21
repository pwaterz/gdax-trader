package main

import (
	"flag"
	"io/ioutil"

	"github.com/pkg/errors"
	yaml "gopkg.in/yaml.v2"
)

// Configuration file
var configLocation = flag.String("config", "./config.yml", "Location of yaml configuration file")

// Configuration store application configuration information
type Configuration struct {
	ElasticClientBatchSize     int      `yaml:"elastic-client-batch-size"`
	ElasticClientWorkers       int      `yaml:"elastic-client-workers"`
	ElasticClientFlushInterval int      `yaml:"elastic-client-flush-interval"`
	ElasticClientStatsEnabled  bool     `yaml:"elastic-client-stats-enabled"`
	Elastic                    []string `yaml:"elastic-hosts"`
	ElasticUser                string   `yaml:"elastic-user,omitempty"`
	ElasticPassword            string   `yaml:"elastic-password,omitempty"`
	ElasticSniff               bool     `yaml:"elastic-sniff-discovery"`
	ElasticIndexName           string   `yaml:"elastic-index"`
	GDAXMarkets                []string `yaml:"gdax-markets"`
	LogLevel                   string   `yaml:"log-level,omitempty"` // Valid values are info or debug. Defaults to info.
}

// NewConfiguration creates a new Configuration from the given file path
func NewConfiguration(configLocation string) (*Configuration, error) {
	yml, err := ioutil.ReadFile(configLocation)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable read configuration file %v", configLocation)
	}

	config := Configuration{}
	err = yaml.Unmarshal(yml, &config)
	if err != nil {
		return nil, errors.Wrapf(err, "Error parsing configuration file %v", configLocation)
	}

	if config.LogLevel == "" {
		config.LogLevel = "info"
	}

	return &config, nil
}
