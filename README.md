# Prometheus Graphite Bridge [![Build Status](https://travis-ci.org/stuart-c/prometheus-graphite-bridge.svg)]()
Scrapes Prometheus metrics and pushes them into Graphite.

## Running

```bash
pip install -r requirements.txt
./prometheus-graphite-bridge.py options...
```

## Configuration

Configuration can be preformed by both command line options and environment variables.

Command Line | Environment | Default | Description
-------------|-------------|---------|------------
--single | `SINGLE` | False | When run only perform a single scrape before quitting. Useful for testing
--debug | `DEBUG` | False | Enable debug logging
--metrics-port | `METRICS_PORT` | 9215 | Port to use for publishing internal metrics
--scrape-interval, --interval | `SCRAPE_INTERVAL` | 30s | How often to scrape target and send metrics to Graphite server. In addition to an number in seconds, suffixes of s (seconds), m (minutes) and h (hours) are also accepted
--scrape-target, --target | `SCRAPE_TARGET` | | The full URL to scrape. You will need to include a path such as `/metrics`. Defaults to http if scheme is not specified
--graphite-host, --graphite | `GRAPHITE_HOST` | | Host to publish Graphite metrics to. Can also specify a port as `HOST:PORT`
--graphite-port | `GRAPHITE_PORT` | 2003 | Port of Graphite server
--graphite-prefix, --prefix | `GRAPHITE_PREFIX` | | Prefix to add to all metrics sent to Graphite server


## Internal Metrics

The Prometheus Graphite bridge publishes a number of internal metrics on the port specified in the configuration. In addition to the standard Python metrics added by the Prometheus client libraries the following are also published:

Metric | Type | Description
-------|------|------------
graphite_bridge_scrape_interval_seconds | Gauge | Configured scrape interval
graphite_bridge_scrape_total | Counter | Number of scrapes performed
graphite_bridge_scrape_duration_seconds | Counter | Sum of actual scrape durations

## Using Docker

[![Docker Build Status](https://img.shields.io/docker/build/stuartc/prometheus-graphite-bridge.svg)]()

```bash
docker run -d -p 9215:9215 \
  stuartc/prometheus-graphite-bridge \
    --scrape-target http://localhost:9100/metrics \
    --graphite-host graphite.example.com
```
