import requests
from requests.models import Response
from time import time, sleep
from .uri import Url, Uri
from rest_framework import status
from requests.exceptions import ConnectionError
from .logger import Logger

OPEN, CLOSED = 0, 1
logger = Logger(__name__, print_start_message=True)


class Fallback:
    def __init__(self, data: dict = None, status: int = None):
        self._status = status
        self._data = data

    def __str__(self):
        return f'Fallback: {self._status} {self._data}'

    @property
    def status_code(self):
        return self._status

    @property
    def data(self):
        return self._data

    def content(self):
        if self._data:
            return self.text().encode('utf-8')
        else:
            return b""

    def text(self):
        if self._data:
            return str(self._data)
        else:
            ''

    def json(self):
        return self.data


class CircuitBreakerItem:
    def __init__(self, url: Url, fallback: Fallback = None):
        self._url = url
        self._fallback = fallback if fallback else Fallback(
            {'message': 'Service unavailable'},
            status.HTTP_503_SERVICE_UNAVAILABLE
        )
        self._state = CLOSED
        self._opening_time = 0
        self._error_counter = 0
        self._logger = logger.child(f'CircuitBreakerItem {url}')
        self._logger.info(f'CircuitBreakerItem for {url} initialized url: {url} fallback: {fallback}')

    def __str__(self):
        return f'CircuitBreakerItem: ' \
               f'{self._url} {self._fallback} {self._state} {self._opening_time} {self._error_counter}'

    @property
    def state(self) -> int:
        return self._state

    @state.setter
    def state(self, state) -> None:
        assert state in (OPEN, CLOSED)
        self._state = state
        if state == OPEN:
            self._opening_time = time()
        elif state == CLOSED:
            self._opening_time = 0

    @property
    def opening_time(self) -> float:
        return self._opening_time

    @opening_time.setter
    def opening_time(self, val) -> None:
        self._opening_time = val

    @property
    def error_counter(self) -> int:
        return self._error_counter

    @error_counter.setter
    def error_counter(self, val) -> None:
        self._error_counter = val

    @property
    def fallback(self) -> Fallback:
        return self._fallback

    @property
    def url(self) -> Url:
        return self._url


class CircuitBreaker:
    def __init__(self, redis_instance, errors_num_before_opening=1, health_check_timeout=5, polling_sleep_time=1):
        self._redis_instance = redis_instance
        self._errors_num_before_opening = errors_num_before_opening
        self._health_check_timeout = health_check_timeout
        self._polling_sleep_time = polling_sleep_time
        self._tracked_items = {}
        self._poll_items = True
        self._logger = logger.child(f'CircuitBreaker')
        self._logger.info(f'CircuitBreaker initialized')

    def _add_item_to_db(self, url: Url) -> None:
        self._logger.child('Method _add_item_to_db').info(f'adding item {url} to db')
        self._redis_instance.rpush('str:circuit_breaker_items:1', url.str)
        self._redis_instance.set(f'int:circuit_breaker_state:{url}', CLOSED)
        self._redis_instance.set(f'str:circuit_breaker_time:{url}', str(time()))

    def _update_item_in_db(self, url: Url, state: int = None, time_: float = None) -> None:
        self._logger.child('Method _update_item_in_db').info(f'update item {url} with: state {state} time {time_}')
        if url.str not in self._tracked_items.keys():
            raise Exception(f'Url {url} not in tracked items: {self._tracked_items.keys()}')

        if state != None:
            self._redis_instance.set(f'int:circuit_breaker_state:{url}', state)
        if time_:
            self._redis_instance.set(f'str:circuit_breaker_time:{url}', str(time_))

    def add_url_for_tracking(self, url: Url, fallback: Fallback = None) -> None:
        url = Url(url)  # because uri can be passed instead of url
        l = self._logger.child('Method add_url_for_tracking')
        l.info(f'Adding {url} for tracking with fallback: {fallback}')
        redis_keys = list(map(bytes.decode, self._redis_instance.keys()))

        if f'int:circuit_breaker_state:{url}' not in redis_keys:
            self._add_item_to_db(url)
        else:
            l.info(f'Item {url} - uri is already in the DB')

        if url.str not in self._tracked_items.keys():
            item = CircuitBreakerItem(url, fallback)
            self._tracked_items.update({url.str: item})
        else:
            l.info(f'Item {url} - uri is already in the tracking list')

    def _reset_error_counter(self, url: Url) -> None:
        item = self._tracked_items[url.str]
        item.error_counter = 0

    def _get_item_from_db(self, url: Url) -> CircuitBreakerItem:
        actual_state = self._redis_instance.get(f'int:circuit_breaker_state:{url}').decode('UTF-8')
        actual_time_ = self._redis_instance.get(f'str:circuit_breaker_time:{url}').decode('UTF-8')
        actual_state = int(actual_state)
        actual_time_ = float(actual_time_)

        item = self._tracked_items[url.str]
        if item.state == OPEN and actual_state == CLOSED:
            self._reset_error_counter(url)
        item.state = actual_state
        item.opening_time = actual_time_
        self._logger.child('Method _get_item_from_db').info(f'Method got url {url} and returned item: {item}')
        return item

    def _get_tracked_item(self, url: Url) -> CircuitBreakerItem:
        return self._get_item_from_db(url)

    def _handle_response(self, url: Url, response: Response) -> None:
        self._logger.child('Method _handle_response').info(
            f'Handle response from {url} with status code {response.status_code}')
        if response.status_code // 100 == 5:
            self._inc_error_counter(url)

    def _inc_error_counter(self, url: Url):
        l = self._logger.child('Method _inc_error_counter')
        l.info(f'Got url {url}')
        item = self._get_tracked_item(url)
        item.error_counter += 1

        if item.error_counter > self._errors_num_before_opening:
            self._update_item_in_db(url, state=OPEN)
            item.state = OPEN

        l.info(f'New error counter value for {url}: {item.error_counter}')
        l.info(f'Item {item} state: {item.state}')

    def _send_request(self, request_method, uri: Uri, headers=None, data=None, **kwargs) -> Response | Fallback:
        l = self._logger.child('Method _send_request')
        l.info(f'send_request {request_method.__name__} to {uri} with {headers=} {data=}')
        url = Url(uri)

        self.add_url_for_tracking(url)

        item = self._get_tracked_item(url)
        if item.state == OPEN:
            l.info(f'Returns fallback: {item.fallback}')
            return item.fallback

        try:
            response = request_method(uri.str, headers=headers, data=data, **kwargs)
        except ConnectionError:
            self._inc_error_counter(url)
            l.info(f'ConnectionError. Returns fallback: {item.fallback}')
            return item.fallback

        self._handle_response(url, response)
        l.info(f'Returns response {response.status_code}')
        return response

    def get(self, uri: Uri, headers=None, data=None, **kwargs) -> Response | Fallback:
        return self._send_request(request_method=requests.get, uri=uri, headers=headers, data=data, **kwargs)

    def post(self, uri: Uri, headers=None, data=None, **kwargs) -> Response | Fallback:
        return self._send_request(request_method=requests.post, uri=uri, headers=headers, data=data, **kwargs)

    def patch(self, uri: Uri, headers=None, data=None, **kwargs) -> Response | Fallback:
        return self._send_request(request_method=requests.patch, uri=uri, headers=headers, data=data, **kwargs)

    def delete(self, uri: Uri, headers=None, data=None, **kwargs) -> Response | Fallback:
        return self._send_request(request_method=requests.delete, uri=uri, headers=headers, data=data, **kwargs)

