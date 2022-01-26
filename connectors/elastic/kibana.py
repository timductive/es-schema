"""Use these functions to save objects to Kibana via the Saved Objects API."""

import os
import requests
import configparser


def create_saved_objects(type='finance', version='7.14', kibana_url=''):
    """Given a schema type (finance, git, ecommerce, etc...) install the relevant objects."""

    if not kibana_url:
        cfg = configparser.ConfigParser()
        try:
            cfg.read('keys_local.cfg')
            kibana_url = cfg.get('elastic', 'kibana_url')
            api_key = cfg.get('elastic', 'elasticsearch_api_key')
        except configparser.NoSectionError:
            raise FileNotFoundError('It looks like you need to create a keys_local.cfg file.')

    directory = './assets/%s/%s' % (type, version)
    headers = {
        'kbn-xsrf': 'true',
        'Authorization': 'ApiKey %s' % api_key
    }

    multi_resp = {
        'success': False,
        'warnings': [],
    }
    for filename in os.listdir(directory):
        files = {}
        f = os.path.join(directory, filename)

        if os.path.isfile(f):
            files['file'] = (filename, open('%s/%s' % (directory, filename), 'rb'))

            req = requests.post('https://%s/api/saved_objects/_import?overwrite=true' % kibana_url, headers=headers, files=files)
            req.raise_for_status()

            resp = req.json()
            multi_resp['success'] = resp.get('success')
            if resp.get('warnings'):
                multi_resp['warnings'] += resp.get('warnings')

    return multi_resp
