import json
import typing as t
from typing import Iterable

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError, TransportError

DEFAULT_SIZE = 10_000
SCROLL_TIMEOUT = '15m'


class ElasticsearchClientException(Exception):
    pass


class ElasticsearchClient(Elasticsearch):

    def __init__(self, hosts: list, scheme: str = None, http_auth: tuple = None, api_key: tuple = None):
        options = {"hosts": hosts, "timeout": 30, "retry_on_timeout": True, "max_retries": 5}

        if scheme == "https":
            options.update({"verify_certs": False, "ssl_show_warn": False})

        if http_auth:
            options.update({"http_auth": http_auth})
        elif api_key:
            options.update({"api_key": api_key})

        super().__init__(**options)

    def extract_data(self, index_name: str, query: str) -> Iterable:
        """
        Extracts data from the specified Elasticsearch index based on the given query.

        Parameters:
            index_name (str): Name of the Elasticsearch index.
            query (dict): Elasticsearch DSL query.

        Yields:
            dict
        """
        response = self.search(index=index_name, size=DEFAULT_SIZE, scroll=SCROLL_TIMEOUT, body=query)
        for r in self._process_response(response):
            yield r

        while len(response['hits']['hits']):
            response = self.scroll(scroll_id=response["_scroll_id"], scroll=SCROLL_TIMEOUT)
            for r in self._process_response(response):
                yield r

    def _process_response(self, response: dict) -> Iterable:
        results = [hit["_source"] for hit in response['hits']['hits']]
        for result in results:
            yield self.flatten_json(result)

    def ping(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[t.List[str], str]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> bool:
        """
        Returns True if a successful response returns from the info() API,
        otherwise returns False. This API call can fail either at the transport
        layer (due to connection errors or timeouts) or from a non-2XX HTTP response
        (due to authentication or authorization issues).

        If you want to discover why the request failed you should use the ``info()`` API.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html>`_
        """
        __path = "/"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        try:
            self.perform_request("HEAD", __path, params=__query, headers=__headers)
            return True
        except (ApiError, TransportError) as e:
            raise ElasticsearchClientException(e)

    def flatten_json(self, x, out=None, name=''):
        if out is None:
            out = dict()
        if type(x) is dict:
            for a in x:
                self.flatten_json(x[a], out, name + a + '.')

        elif type(x) is list:
            out[name[:-1]] = json.dumps(x)

        else:
            out[name[:-1]] = x

        return out
