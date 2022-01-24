"""Connector script for Alpha Vantage API."""

import configparser
import requests


def alphavantage_connector(symbol='ESTC', series='TIME_SERIES_INTRADAY', interval='60min'):
    """Connector for free alphavantage stock ticker API."""
    cfg = configparser.ConfigParser()
    try:
        cfg.read('keys_local.cfg')
        av_key = cfg.get('finance', 'alphavantage_apikey')
    except configparser.NoSectionError:
        raise FileNotFoundError('It looks like you need to create a keys_local.cfg file.')
        
    url = 'https://www.alphavantage.co/query?function=%s&symbol=%s&interval=%s&apikey=%s' % (series, symbol, interval, av_key)
    r = requests.get(url)
    data = r.json()

    print(data)


if __name__ == '__main__':
    alphavantage_connector()
