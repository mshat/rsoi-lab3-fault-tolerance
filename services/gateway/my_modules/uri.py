import requests


class UriPart:
    EXTRA_SUBSTRINGS = ('://', '/', ':', '?')

    def __init__(self, part, extra_substrings=EXTRA_SUBSTRINGS, left_service_substring='', right_service_substring=''):
        self.part = part
        self.clear_part(extra_substrings)
        if part and left_service_substring:
            self.left_add_substring(left_service_substring)
        if part and right_service_substring:
            self.right_add_substring(right_service_substring)

    def clear_part(self, extra_substrings):
        self.part = self.part.strip()
        if not extra_substrings:
            return
        for substring in extra_substrings:
            self.my_strip(substring)

    def left_add_substring(self, substring):
        self.part = substring + self.part

    def right_add_substring(self, substring):
        self.part += substring

    def my_strip(self, extra_substring):
        if self.part.startswith(extra_substring):
            self.part = self.part.replace(extra_substring, '', 1)
        if self.part.endswith(extra_substring):
            self.part = self.part[:-(len(extra_substring))]

    def __str__(self):
        return self.part


class Scheme(UriPart):
    def __init__(self, part, right_service_substring='://', **kwargs):
        super().__init__(part, right_service_substring=right_service_substring, **kwargs)


class Host(UriPart):
    def __init__(self, part, **kwargs):
        super().__init__(part, **kwargs)


class Port(UriPart):
    def __init__(self, part, left_service_substring=':', **kwargs):
        super().__init__(part, left_service_substring=left_service_substring, **kwargs)


class Path(UriPart):
    def __init__(self, part, left_service_substring='/', **kwargs):
        super().__init__(part, left_service_substring=left_service_substring, **kwargs)


class Query(UriPart):
    def __init__(self, part, left_service_substring='?', **kwargs):
        super().__init__(part, left_service_substring=left_service_substring, **kwargs)


class Url:
    def __init__(self, other=None, scheme='http', host='127.0.0.1', port=''):
        if other:
            self._scheme = other.scheme
            self._host = other.host
            self._port = other.port
        else:
            self._scheme = Scheme(scheme)
            self._host = Host(host)
            self._port = Port(port)

    @property
    def scheme(self):
        return self._scheme

    @scheme.setter
    def scheme(self, val):
        self._scheme = Scheme(val)

    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, val):
        self._host = Host(val)

    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, val):
        self._port = Port(val)

    @property
    def str(self):
        return self.__str__()

    def __str__(self):
        return f'{self.scheme}{self._host}{self._port}'


class Uri(Url):
    def __init__(self, other=None, scheme='http', host='127.0.0.1', port='', path='', query=''):
        super().__init__(other, scheme, host, port)
        if other:
            self._path = other.path
            self._query = other.query
        else:
            self._path = Path(path)
            self._query = Query(query)

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, val):
        self._path = Path(val)

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, val):
        self._query = Query(val)

    def __str__(self):
        return f'{self.scheme}{self._host}{self._port}{self._path}{self._query}'



