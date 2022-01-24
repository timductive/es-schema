"""Connector script for Alpha Vantage API."""

import sys
sys.path.append('connectors')

import configparser
import json
import requests

from elastic.sync import sync_document



def alphavantage_connector(symbol='ESTC', series='TIME_SERIES_INTRADAY', interval='60min'):
    """Connector for free alphavantage stock ticker API."""
    SCHEMA = './schema/finance/stock_ticker.schema.json'

    cfg = configparser.ConfigParser()
    try:
        cfg.read('keys_local.cfg')
        av_key = cfg.get('finance', 'alphavantage_apikey')
    except configparser.NoSectionError:
        raise FileNotFoundError('It looks like you need to create a keys_local.cfg file.')
        
    url = 'https://www.alphavantage.co/query?function=%s&symbol=%s&interval=%s&apikey=%s' % (series, symbol, interval, av_key)
    r = requests.get(url)
    data = r.json()

    metadata = data.get('Meta Data')

    # Load schema
    f = open(SCHEMA)
    schema = json.load(f)
    f.close()

    for dockey, document in data.get('Time Series (%s)' % interval, {}).items():
        valid_document = {}
        for schema_key, schema_value in schema.get('properties').items():
            # Fuzzy match as many keys as possible
            for dkey, value in document.items():
                if schema_key in dkey:
                    if schema_value.get('type') == 'number':
                        try:
                            valid_document[schema_key] = float(value)
                        except:
                            valid_document[schema_key] = value
                    elif schema_value.get('type') == 'integer':
                        try:
                            valid_document[schema_key] = int(value)
                        except:
                            valid_document[schema_key] = value
                    elif schema_value.get('type') == 'string':
                        try:
                            valid_document[schema_key] = str(value)
                        except:
                            valid_document[schema_key] = value
            # Match remaining fields
            valid_document['symbol'] = metadata.get('2. Symbol')
            valid_document['timezone'] = metadata.get('6. Time Zone')
            valid_document['@timestamp'] = dockey
            valid_document['id'] = str(valid_document['symbol'] + valid_document['@timestamp'])

        sync_document(schema, valid_document)


if __name__ == '__main__':
    alphavantage_connector()
