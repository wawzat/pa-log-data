#

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import config

session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.headers.update({'X-API-Key': config.PURPLEAIR_READ_KEY})
session.mount('http://', adapter)
session.mount('https://', adapter)

root_url = 'https://api.purpleair.com/v1/sensors/{sensor_id}'

sensor_id = '9208'

params = {
    'sensor_id': sensor_id,
}

url = root_url.format(**params)

response = session.get(url)
url_data = response.content

print(url_data)