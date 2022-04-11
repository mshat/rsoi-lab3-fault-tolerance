import sys
import os
sys.path.append("..")
from my_modules.uri import Uri

if 'MODE' in os.environ:
    MODE = os.environ["MODE"]
else:
    MODE = 'dev'


URIS_DEV_PROD = {
    "dev": {
        "gateway": Uri(host='gateway-service', port="8000", path="api/v1/"),
    },
    "prod": {
        "gateway": Uri(host='lab2-gateway.herokuapp.com', path="api/v1/"),
    }
}


def get_uri(name):
    return Uri(other=URIS_DEV_PROD[MODE][name])
