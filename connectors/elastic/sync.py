"""Use these functions to sync Elasticsearch and Kibana documents."""

import configparser
import jsonschema

from elasticsearch import Elasticsearch


def connect(url='', user='', password=''):
    """Establishes connection to Elasticsearch."""
    # If creds not passed, check config file
    if not url:
        cfg = configparser.ConfigParser()
        try:
            cfg.read('keys_local.cfg')
            url = cfg.get('elastic', 'elasticsearch_url')
            api_key = cfg.get('elastic', 'elasticsearch_api_key')
        except configparser.NoSectionError:
            raise FileNotFoundError('It looks like you need to create a keys_local.cfg file.')

    _es = Elasticsearch([
            'https://{}'.format(url)
        ],
        api_key=api_key,
        use_ssl=True,
        verify_certs=True
    )

    if _es.ping():
        print('Yay Connect')
    else:
        print('Awww it could not connect!')
        raise ConnectionError('Elasticsearch could not connect.')

    return _es


def sync_document(schema, document):
    """Validates schema and then syncs to elasticsearch cluster."""
    es = ES_SESSION
    # Validate schema
    jsonschema.validate(instance=document, schema=schema)

    # Derive elasticsearch index name from schema
    es_index = schema.get('es_index')

    # Sync to elasticsearch
    create_index(es, es_index)

    try:
        resp = es.index(index=es_index, body=document, id=document.get('id'))
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
    finally:
        return resp


def delete_index(es, index):
    """
    Delete Elasticsearch index.
    """
    return es.indices.delete(index)


def create_index(es, index):
    """
    Create Elasticsearch index.
    """
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
    try:
        if not es.indices.exists(index):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es.indices.create(index=index, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


ES_SESSION = connect()
    