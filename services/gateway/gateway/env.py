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
        "reservation": Uri(host='reservation-service', port="8001", path="reservation"),
        "loyalty": Uri(host='loyalty-service', port="8003", path="loyalty"),
        "payment": Uri(host='payment-service', port="8002", path="payments"),

    },
    "prod": {
        "reservation": Uri(host="lab2-reservation-2.herokuapp.com", path="reservation"),
        "loyalty": Uri(host='lab2-loyalty.herokuapp.com', path="loyalty"),
        "payment": Uri(host='lab2-payment.herokuapp.com', path="payments"),
    }
}


def get_uri(name) -> Uri:
    return Uri(other=URIS_DEV_PROD[MODE][name])
