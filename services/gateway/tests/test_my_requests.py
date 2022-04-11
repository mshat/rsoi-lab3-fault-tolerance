import sys
import unittest
from unittest import mock
sys.path.append("..")
from my_modules.uri import Uri
from my_modules.my_requests import GetRequest, PostRequest, PatchRequest, DeleteRequest


HEADERS = {'header1': 'value'}
DATA = {'data_key1': 'data_value'}


class GetRequestTest(unittest.TestCase):
    @mock.patch('my_modules.my_requests.requests')
    def test_send_request(self, requests):
        uri = Uri()
        get_request = GetRequest(uri, headers=HEADERS, data=DATA)

        get_request.send()

        requests.get.assert_called_with(str(uri), headers=HEADERS, data=DATA)


class PostRequestTest(unittest.TestCase):
    @mock.patch('my_modules.my_requests.requests')
    def test_send_request(self, requests):
        uri = Uri()
        get_request = PostRequest(uri, headers=HEADERS, data=DATA)

        get_request.send()

        requests.post.assert_called_with(str(uri), headers=HEADERS, data=DATA)


class PatchRequestTest(unittest.TestCase):
    @mock.patch('my_modules.my_requests.requests')
    def test_send_request(self, requests):
        uri = Uri()
        get_request = PatchRequest(uri, headers=HEADERS, data=DATA)

        get_request.send()

        requests.patch.assert_called_with(str(uri), headers=HEADERS, data=DATA)


class DeleteRequestTest(unittest.TestCase):
    @mock.patch('my_modules.my_requests.requests')
    def test_send_request(self, requests):
        uri = Uri()
        get_request = DeleteRequest(uri, headers=HEADERS, data=DATA)

        get_request.send()

        requests.delete.assert_called_with(str(uri), headers=HEADERS, data=DATA)