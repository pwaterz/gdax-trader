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

	"github.com/Sirupsen/logrus"
	ws "github.com/gorilla/websocket"
	"github.com/phayes/errors"
	gdax "github.com/preichenberger/go-gdax"
	elastic "gopkg.in/olivere/elastic.v5"
)

var (
	wg         sync.WaitGroup // WaitGroup for clean shutdown
	tickerTime = time.Millisecond * 500
)

func main() {
	flag.Parse()

	// Create our common context
	ctx, cancel := context.WithCancel(context.Background())
	// Parse configuration
	var configErr error
	config, configErr := NewConfiguration(*configLocation)
	if configErr != nil {
		logMain.Fatal(errors.Wraps(configErr, "Error loading configuration"))
	}

	switch config.LogLevel {
	case "debug":
		log.Level = logrus.DebugLevel
	default:
		log.Level = logrus.InfoLevel
	}

	elasticClient, err := initializeElasticClient(ctx, config)
	if err != nil {
		logElastic.Fatal(errors.Wraps(err, "Could not connect to elastic cluster"))
	}

	elasticBulkProcessor, err := initializeElasticBulkProcessor(ctx, elasticClient, config)
	if err != nil {
		logElastic.Fatal(errors.Wraps(err, "Could not start bulk processor"))
	}

	// Create the index if it doesn't exist
	initializeIndex(ctx, elasticClient, config)

	for _, market := range config.GDAXMarkets {
		logMain.Info("Starting indexer for order book " + market)
		wg.Add(1)
		go indexOrderBook(ctx, config.ElasticIndexName, market, elasticBulkProcessor)
		logMain.Info("Starting indexer for ticket " + market)
		wg.Add(1)
		go indexTicker(ctx, config.ElasticIndexName, market, elasticBulkProcessor)
	}

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
func initializeIndex(ctx context.Context, elasticClient *elastic.Client, config *Configuration) {
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

// initializeElasticClient creates the configured index if it doesn't exist
func initializeElasticClient(ctx context.Context, config *Configuration) (*elastic.Client, error) {
	elasticClient, elasticErr := elastic.NewClient(
		elastic.SetURL(config.Elastic...),
		elastic.SetSniff(config.ElasticSniff),
		elastic.SetBasicAuth(config.ElasticUser,
			config.ElasticPassword),
	)

	if elasticErr != nil {
		return nil, elasticErr
	}

	logElastic.Info("Elastic client successfully initialized")

	// Gracefully shutdown bulk procesor
	// todo do this better
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		logElastic.Info("Shutting elastic client")
		elasticClient.Stop()
	}()

	return elasticClient, nil
}

// initializeElasticBulkProcessor creates an elastic bulk processor based on a configuration object
func initializeElasticBulkProcessor(ctx context.Context, elasticClient *elastic.Client, config *Configuration) (*elastic.BulkProcessor, error) {
	elasticBulkProcessor, processorErr := elasticClient.BulkProcessor().
		Name("embargod-indexer").
		After(elasticBulkProcessorFinished).
		Workers(config.ElasticClientWorkers).
		BulkActions(config.ElasticClientBatchSize).
		FlushInterval(time.Duration(config.ElasticClientFlushInterval) * time.Second).
		Stats(config.ElasticClientStatsEnabled).
		Do(ctx)

	if processorErr != nil {
		return nil, processorErr
	}

	logElastic.Info("Elastic bulk processor successfully initialized")
	// Gracefully shutdown bulk procesor
	// todo do this better
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		logElastic.Info("Shutting down elastic bulk processor")
		if err := elasticBulkProcessor.Close(); err != nil {
			logElastic.Error(errors.Wraps(err, "Error shutting down elastic bulk processor"))
		}
		logElastic.Info("Successfully shut down elastic bulk processor")
	}()

	return elasticBulkProcessor, nil
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

// restartTicker will restart the ticker indexer if there is a panic
func restartTicker(ctx context.Context, indexName, coin string, elasticBulkProcessor *elastic.BulkProcessor) {
	if err := recover(); err != nil {
		logStream.Error("Ticker stream failed, waiting 10 seconds and restatrting")
		time.Sleep(10 * time.Second)
		wg.Add(1)
		indexTicker(ctx, indexName, coin, elasticBulkProcessor)
	}
}

// indexTicker indexes ticket data from gdax
func indexTicker(ctx context.Context, indexName, coin string, elasticBulkProcessor *elastic.BulkProcessor) {
	defer restartTicker(ctx, indexName, coin, elasticBulkProcessor)
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
				Name: "ticker",
				ProductIds: []string{
					coin,
				},
			},
		},
	}
	if err := wsConn.WriteJSON(subscribe); err != nil {
		logStream.Error(err)
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
			logStream.Error(err.Error())
			continue
		}

		if message.Time.Time().IsZero() {
			continue
		}

		r := elastic.NewBulkIndexRequest().
			Index(indexName).
			Type("ticker").
			Doc(message)
		elasticBulkProcessor.Add(r)
	}
}

// restartOrderBook auto restarts the index order book indexer in the event of a panic
func restartOrderBook(ctx context.Context, indexName, coin string, elasticBulkProcessor *elastic.BulkProcessor) {
	if err := recover(); err != nil {
		logStream.Error("Order book stream failed, waiting 10 seconds and restatrting")
		time.Sleep(10 * time.Second)
		wg.Add(1)
		indexOrderBook(ctx, coin, indexName, elasticBulkProcessor)
	}
}

// indexOrderBook index order book results from gdax
func indexOrderBook(ctx context.Context, indexName, coin string, elasticBulkProcessor *elastic.BulkProcessor) {
	defer restartOrderBook(ctx, indexName, coin, elasticBulkProcessor)
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
		logStream.Error(err)
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
			logStream.Error(err.Error())
			continue
		}

		if message.Time.Time().IsZero() {
			continue
		}

		r := elastic.NewBulkIndexRequest().
			Index(indexName).
			Type("snap-shot").
			Doc(message)
		elasticBulkProcessor.Add(r)
	}
}
