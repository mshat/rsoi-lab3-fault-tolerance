import sys
sys.path.append("..")
from my_modules.uri import Uri
import unittest


class UriTest(unittest.TestCase):
    SCHEME = 'http'
    HOST = '127.0.0.1'
    PORT = '8000'
    PATH = 'api/v1/hotels'
    QUERY = 'page=1&size=10'

    def test_create_uri(self):

        uri = Uri(scheme=self.SCHEME, host=self.HOST, port=self.PORT, path=self.PATH, query=self.QUERY)

        self.assertEqual('http://127.0.0.1:8000/api/v1/hotels?page=1&size=10', str(uri))

    def test_create_uri_bad_scheme(self):
        scheme = 'http://'

        uri = Uri(scheme=self.SCHEME, host=self.HOST, port=self.PORT, path=self.PATH, query=self.QUERY)

        self.assertEqual('http://127.0.0.1:8000/api/v1/hotels?page=1&size=10', str(uri))

    def test_create_uri_bad_host(self):
        host = '://127.0.0.1/'

        uri = Uri(scheme=self.SCHEME, host=self.HOST, port=self.PORT, path=self.PATH, query=self.QUERY)

        self.assertEqual('http://127.0.0.1:8000/api/v1/hotels?page=1&size=10', str(uri))

    def test_create_uri_bad_port(self):
        port = ':8000/'

        uri = Uri(scheme=self.SCHEME, host=self.HOST, port=self.PORT, path=self.PATH, query=self.QUERY)

        self.assertEqual('http://127.0.0.1:8000/api/v1/hotels?page=1&size=10', str(uri))

    def test_create_uri_bad_path(self):
        path = '/api/v1/hotels?'

        uri = Uri(scheme=self.SCHEME, host=self.HOST, port=self.PORT, path=self.PATH, query=self.QUERY)

        self.assertEqual('http://127.0.0.1:8000/api/v1/hotels?page=1&size=10', str(uri))

    def test_create_uri_bad_query(self):
        query = '?page=1&size=10'

        uri = Uri(scheme=self.SCHEME, host=self.HOST, port=self.PORT, path=self.PATH, query=self.QUERY)

        self.assertEqual('http://127.0.0.1:8000/api/v1/hotels?page=1&size=10', str(uri))
