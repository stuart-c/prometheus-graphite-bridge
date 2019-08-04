#!/usr/bin/python

import os
import time
import logging
import requests
import argparse

from timeit import default_timer
from humanfriendly import parse_timespan
from prometheus_client import start_http_server
from prometheus_client import CollectorRegistry, Counter, Gauge
from prometheus_client.parser import text_string_to_metric_families
from prometheus_client.bridge.graphite import GraphiteBridge

SCRAPE_INTERVAL = Gauge(
    'graphite_bridge_scrape_interval_seconds',
    'Configured time between scrapes'
)
SCRAPE_COUNT = Counter(
    'graphite_bridge_scrape_total',
    'Number of scrapes'
)
SCRAPE_DURATION = Counter(
    'graphite_bridge_scrape_duration_seconds',
    'Scrape duration'
)


class ScrapeCollector(object):

    def __init__(self, target, retry):
        self._target = target
        self._retry = retry

    def collect(self):
        metrics = None
        for i in range(0, self._retry + 1):
            try:
                metrics = requests.get(self._target).content.decode('utf-8')
            except requests.exceptions.ConnectionError as ce:
                time.sleep(1)
                logging.warning(f"got {ce}, retrying {self._retry - i} more times.. ")
                if i == self._retry:
                    raise ce
                continue
            break
        return text_string_to_metric_families(metrics)


def setup_bridge(prom, retry, graphite):
    registry = CollectorRegistry()
    registry.register(ScrapeCollector(prom, retry))

    logging.info('Scrape target: %s', prom)
    logging.info('Graphite server: %s:%i', *graphite)

    bridge = GraphiteBridge(graphite, registry=registry)

    return bridge


def scrape_and_wait(bridge, interval, prefix=''):
    start = default_timer()

    logging.debug('Scrape started')
    bridge.push(prefix)

    duration = default_timer() - start

    SCRAPE_COUNT.inc()
    SCRAPE_DURATION.inc(duration)
    logging.debug('Scrape duration: %f', duration)

    wait = interval - duration

    if wait > 0:
        logging.debug('Sleeping for %f seconds', wait)
        time.sleep(wait)


def cmdline_parse():
    defaults = {}

    defaults['single'] = os.environ.get('SINGLE')
    defaults['debug'] = os.environ.get('DEBUG')
    defaults['metrics_port'] = os.environ.get('METRICS_PORT')
    defaults['scrape_interval'] = os.environ.get('SCRAPE_INTERVAL')
    defaults['scrape_target'] = os.environ.get('SCRAPE_TARGET')
    defaults['graphite_host'] = os.environ.get('GRAPHITE_HOST')
    defaults['graphite_port'] = os.environ.get('GRAPHITE_PORT')
    defaults['graphite_prefix'] = os.environ.get('GRAPHITE_PREFIX')
    defaults['retry_count'] = os.environ.get('RETRY_COUNT')

    defaults = dict((k, v) for k, v in defaults.items() if v is not None)

    parser = argparse.ArgumentParser(
        description='Scrapes Prometheus metrics and pushes them into Graphite',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('--single',
                        action='store_true',
                        help='only perform a single scrape')
    parser.add_argument('--debug',
                        action='store_true',
                        help='enable debug logging')
    parser.add_argument('--metrics-port',
                        type=int, default=9215, metavar='PORT',
                        help='port for publishing internal metrics')
    parser.add_argument('--scrape-interval', '--interval',
                        default='30s', metavar='INTERVAL',
                        help='scrape interval')
    parser.add_argument('--scrape-target', '--target',
                        required=True, metavar='URL',
                        help='URL to scrape')
    parser.add_argument('--graphite-host', '--graphite',
                        required=True, metavar='HOST[:PORT]',
                        help='host name of Graphite server')
    parser.add_argument('--graphite-port',
                        type=int, default=2003, metavar='PORT',
                        help='port number of Graphite servier')
    parser.add_argument('--graphite-prefix', '--prefix',
                        metavar='PREFIX',
                        help='prefix to add to all metrics sent to Graphite')
    parser.add_argument('--retry-count',
                        type=int, default=10, metavar='NUM',
                        help='times to retry connecting to target')

    parser.set_defaults(**defaults)

    config = parser.parse_args()

    try:
        config.scrape_interval = int(config.scrape_interval)
    except ValueError:
        config.scrape_interval = parse_timespan(config.scrape_interval)

    if config.single:
        config.scrape_interval = 0

    if not config.scrape_target.startswith('http'):
        config.scrape_target = 'http://' + config.scrape_target

    if ':' in config.graphite_host:
        (host, port) = config.graphite_host.split(':', 2)

        config.graphite_host = (host, int(port))
    else:
        config.graphite_host = (config.graphite_host, config.graphite_port)

    return config


if __name__ == '__main__':
    CONFIG = cmdline_parse()

    if CONFIG.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    if not CONFIG.single:
        logging.info('Scrape interval set to %i seconds',
                     CONFIG.scrape_interval)
        SCRAPE_INTERVAL.set(CONFIG.scrape_interval)

    logging.info('Publishing internal metrics on port %s', CONFIG.metrics_port)
    start_http_server(CONFIG.metrics_port)

    BRIDGE = setup_bridge(CONFIG.scrape_target, CONFIG.retry_count, CONFIG.graphite_host)

    while True:
        scrape_and_wait(BRIDGE, CONFIG.scrape_interval, CONFIG.graphite_prefix)

        if CONFIG.single:
            break
