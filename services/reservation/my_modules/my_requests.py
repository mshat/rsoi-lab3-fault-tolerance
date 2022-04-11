import requests
import sys
from abc import ABC, abstractmethod
sys.path.append("..")
from my_modules.uri import Uri


class BaseRequest(ABC):
    def __init__(self, uri, headers=None, data=None):
        self._uri = uri
        self._headers = headers
        self._data = data

    @abstractmethod
    def send(self):
        pass

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, val: dict):
        self._data = val

    @property
    def headers(self):
        return self._headers

    @headers.setter
    def headers(self, val: dict):
        self._headers = val

    @property
    def uri(self):
        return str(self._uri)

    @uri.setter
    def uri(self, val):
        if isinstance(val, Uri):
            self._uri = val
        else:
            raise Exception('Val is not Uri instance')


class GetRequest(BaseRequest):
    def __init__(self, uri, **kwargs):
        super().__init__(uri, **kwargs)

    def send(self):
        return requests.get(self.uri, headers=self.headers, data=self.data)


class PostRequest(BaseRequest):
    def __init__(self, uri, **kwargs):
        super().__init__(uri, **kwargs)

    def send(self):
        return requests.post(self.uri, headers=self.headers, data=self.data)


class PatchRequest(BaseRequest):
    def __init__(self, uri, **kwargs):
        super().__init__(uri, **kwargs)

    def send(self):
        return requests.patch(self.uri, headers=self.headers, data=self.data)


class DeleteRequest(BaseRequest):
    def __init__(self, uri, **kwargs):
        super().__init__(uri, **kwargs)

    def send(self):
        return requests.delete(self.uri, headers=self.headers, data=self.data)
