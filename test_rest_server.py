import logging.config
import os
import threading
from datetime import datetime

import requests
from rpi.custom_logging import get_dict_config
from rpi.encryption import encrypt

from rest_server import get_server, SOURCE_CSV, LOG_PATH

config = get_dict_config(name='')
config['loggers']['']['handlers'] = ['console']
del config['handlers']['file']
logging.config.dictConfig(config)


def _run_server_with_threading(server):
    try:
        server.serve_forever()
    except OSError:
        pass


def _simulate_csv():
    with open(SOURCE_CSV, 'w') as f:
        f.write('peter friend')


def _create_token():
    today = datetime.today()
    anything = (today.year, today.month, today.day)

    token = encrypt(repr(anything))
    return token


def test_server():
    token = _create_token()
    server = get_server()

    t = threading.Thread(target=_run_server_with_threading, args=(server,), name='RestServer',
                         daemon=True)
    t.start()

    r = requests.get('http://127.0.0.1:5415')
    assert r.status_code in (404, 200)
    if r.status_code == 404:
        assert 'File has just been deleted, please wait about 2 min' in r.text

    r = requests.get('http://127.0.0.1:5415/this_does_not_exist')
    assert r.status_code == 400

    _simulate_csv()
    r = requests.get('http://127.0.0.1:5415')
    assert r.text == 'peter friend'

    r = requests.delete('http://127.0.0.1:5415')
    assert r.status_code == 403

    r = requests.delete('http://127.0.0.1:5415/this_does_not_exist')
    assert r.status_code == 403

    r = requests.delete('http://127.0.0.1:5415', data={'token': token})
    assert r.text == 'Result: True'

    r = requests.delete('http://127.0.0.1:5415', data={'token': token})
    assert r.text == 'Result: False'

    r = requests.get('http://127.0.0.1:5415/favicon.ico')
    assert r.headers['Content-type'] == 'image/png'
    assert len(r.content) > 0

    server.server_close()

    os.remove(LOG_PATH)
