# GDAX market indexer
This program is designed to read data from GDAX's streaming api and index the order book and ticker data into elastic search.

## Build
Install golang
```
git clone git@github.com:pwaterz/gdax-market-indexer.git
cd gdax-market-indexer
go get -d ./...
go build .
./gdax-market-indexer -config /path/to/config.yml
```
## Configure
Modify the yaml file in the repo or create a yaml configuration file
```
# ElasticSearch servers
elastic-hosts:
  - http://localhost:9200

# If password protected, set username and password
elastic-user: elastic

elastic-password: changeme

# Elastic client bulk processor batchsize
elastic-client-batch-size: 5

# Elastic client bulk processor number of workers
elastic-client-workers: 10

# Elastic client flush interval in seconds
elastic-client-flush-interval: 5

# Enable the elastic client stat collector
elastic-client-stats-enabled: true

# Set to true to enable sniffing of elastic hosts
elastic-sniff-discovery: false

# The name of the index to store the data
elastic-index: gdax-market

# Markets to indexe
gdax-markets:
  - BTC-USD
  - BCH-USD
  - LTC-USD
  - ETH-USD

# Log level. Can be info, debug, or be omitted. If ommitted will default to info.
log-level: debug
```
## Development
Use the docker-compose in the repository to get development going quickly. Leave the configuration file defaults, and execute docker-compose.

```
docker-compose up -d
go build . && ./gdax-market-indexer
```
