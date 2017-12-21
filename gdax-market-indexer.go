package main

import (
	"context"
	"flag"
	"io/ioutil"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	elastic "gopkg.in/olivere/elastic.v5"

	"github.com/Sirupsen/logrus"
	ws "github.com/gorilla/websocket"
	"github.com/phayes/errors"
	gdax "github.com/preichenberger/go-gdax"
)

var (
	wg                   sync.WaitGroup // WaitGroup for clean shutdown
	ctx                  context.Context
	elasticClient        *elastic.Client        // Elastic client
	elasticBulkProcessor *elastic.BulkProcessor // Elastic BulkProcessor
	config               *Configuration         // Global configuration object
	tickerTime           = time.Millisecond * 500
)

func main() {
	flag.Parse()

	// Create our common context
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(context.Background())
	// Parse configuration
	var configErr error
	config, configErr = NewConfiguration(*configLocation)
	if configErr != nil {
		logMain.Fatal(errors.Wraps(configErr, "Error loading configuration"))
	}

	switch config.LogLevel {
	case "debug":
		log.Level = logrus.DebugLevel
	default:
		log.Level = logrus.InfoLevel
	}

	initializeElasticClient()

	// Create the index if it doesn't exist
	initializeIndex()

	wg.Add(1)
	go indexCoin("BTC-USD")
	wg.Add(1)
	go indexCoin("BCH-USD")
	wg.Add(1)
	go indexCoin("LTC-USD")
	wg.Add(1)
	go indexCoin("ETH-USD")

	// Run forever until getting interrupt signal
	waitForSignal()

	// After getting interrupted, cancel all consumers and jobs
	cancel()

	// Wait until all jobs are done before exiting
	wg.Wait()
}

func waitForSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	logMain.Info("Got interrupt signal. Initiating Shutdown.")
}

// initializeIndex create the index in the configuration if it doesn't exist
func initializeIndex() {
	logElastic.Info("Initializing index " + config.ElasticIndexName)
	exists, existError := elasticClient.IndexExists(config.ElasticIndexName).Do(ctx)
	if existError != nil {
		logElastic.Fatal(existError)
	}

	if !exists {
		// Create the index
		templateFile := "elastic-template.json"
		json, jsonErr := ioutil.ReadFile(templateFile)
		if jsonErr != nil {
			jsonErr = errors.Wrapf(jsonErr, "Unable to refresh index. Error opening %v", templateFile)
			logElastic.Fatal(jsonErr)
		}

		_, createIndexErr := elasticClient.CreateIndex(config.ElasticIndexName).BodyString(string(json[:])).Do(ctx)

		if createIndexErr != nil {
			createIndexErr = errors.Wrapf(createIndexErr, "Unable to create index. Error PUTing index definition for %v", config.ElasticIndexName)
			logElastic.Fatal(createIndexErr)
		}

		logElastic.Info("Created index " + config.ElasticIndexName)
	} else {
		logElastic.Info("Index " + config.ElasticIndexName + " exists, nothing to do")
	}
}

func initializeElasticClient() {
	var elasticErr error
	elasticClient, elasticErr = elastic.NewClient(elastic.SetURL(config.Elastic...), elastic.SetSniff(config.ElasticSniff))
	if elasticErr != nil {
		logElastic.Fatal(errors.Wraps(elasticErr, "Could not connect to elastic cluster"))
	}
	logElastic.Info("Elastic client successfully initialized")

	var processorErr error
	elasticBulkProcessor, processorErr = elasticClient.BulkProcessor().
		Name("embargod-indexer").
		After(elasticBulkProcessorFinished).
		Workers(config.ElasticClientWorkers).
		BulkActions(config.ElasticClientBatchSize).
		FlushInterval(time.Duration(config.ElasticClientFlushInterval) * time.Second).
		Stats(config.ElasticClientStatsEnabled).
		Do(ctx)

	if processorErr != nil {
		logElastic.Fatal(errors.Wraps(processorErr, "Failed to start elastic bulk processor"))
	}

	logElastic.Info("Elastic bulk processor successfully initialized")

	// Gracefully shutdown bulk procesor
	go func() {
		defer wg.Done()
		<-ctx.Done()
		logElastic.Info("Shutting down elastic bulk processor")
		if err := elasticBulkProcessor.Close(); err != nil {
			logElastic.Error(errors.Wraps(err, "Error shutting down elastic bulk processor"))
		}
		logElastic.Info("Successfully shut down elastic bulk processor")
		logElastic.Info("Shutting elastic client")
		elasticClient.Stop()
	}()
}

// elasticBulkProcessorFinished is called when the elastic bulk processor finishes trying to send requests to eleastic
func elasticBulkProcessorFinished(id int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if len(response.Succeeded()) > 0 {
		logElastic.Info("Successfully sent " + strconv.Itoa(len(response.Succeeded())) + " documents to elastic")
	}
	if len(response.Failed()) > 0 {
		logElastic.Info("Errors recieved " + strconv.Itoa(len(response.Failed())))
	}
}

func indexCoin(coin string) {
	var wsDialer ws.Dialer
	wsConn, _, err := wsDialer.Dial("wss://ws-feed.gdax.com", nil)
	if err != nil {
		logStream.Error(err.Error())
	}
	defer wsConn.Close()

	subscribe := gdax.Message{
		Type: "subscribe",
		Channels: []gdax.MessageChannel{
			gdax.MessageChannel{
				Name: "level2",
				ProductIds: []string{
					coin,
				},
			},
		},
	}
	if err := wsConn.WriteJSON(subscribe); err != nil {
		println(err.Error())
	}

	go func() {
		<-ctx.Done()
		os.Exit(1)
	}()

	message := gdax.Message{}
	ticker := time.NewTicker(tickerTime)
	defer ticker.Stop()

	for _ = range ticker.C {
		if err := wsConn.ReadJSON(&message); err != nil {
			logElastic.Error(err.Error())
			continue
		}

		if message.Time.Time().IsZero() {
			continue
		}

		r := elastic.NewBulkIndexRequest().
			Index(config.ElasticIndexName).
			Type("snap-shot").
			Doc(message)
		elasticBulkProcessor.Add(r)
	}
}
