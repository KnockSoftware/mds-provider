import unittest, re, uuid
from contextlib import contextmanager
import requests_mock
from urllib3.util import parse_url

from mds.fake.server import make_static_server_app
from mds.providers import Provider
from mds.api import ProviderClient


def requests_mock_with_app(app, netloc='testserver'):
    client = app.test_client()
    def get_app_response(request, response_context):
        url_object = parse_url(request.url)
        app_response = client.get(url_object.request_uri, base_url='https://testserver/')
        response_context.status_code = app_response.status_code
        response_context.headers = app_response.headers
        return app_response.data

    mock = requests_mock.Mocker()
    matcher = re.compile(f'^https://{netloc}/')
    mock.register_uri('GET', matcher, content=get_app_response)
    return mock

@contextmanager
def mock_provider(app):
    with requests_mock_with_app(app, netloc='testserver') as mock:
        provider = Provider(
            'test',
            uuid.uuid4(),
            url='',
            auth_type='Bearer',
            token='', # enable simple token auth
            mds_api_url='https://testserver')
        yield provider


class APITest(unittest.TestCase):
    def setUp(self):
        self.empty_app = make_static_server_app(
            trips=[],
            status_changes=[],
            version='0.2.0',
            page_size=20,
        )

        self.bogus_data_app = make_static_server_app(
            trips=list(range(100)),
            status_changes=list(range(100)),
            version='0.2.0',
            page_size=20,
        )


    def _items_from_app(self, app, endpoint='trips', **kwargs):
        with mock_provider(app) as provider:
            client = ProviderClient(provider)
            return list(client.iterate_items(endpoint, **kwargs))

    def test_single_provider_paging_enabled(self):
        # empty provider should return zero trips
        trips = self._items_from_app(self.empty_app, 'trips')
        self.assertEqual(len(trips), 0)

        # 100-trip provider should return all trips
        trips = self._items_from_app(self.bogus_data_app, 'trips')
        self.assertEqual(len(trips), 100)

    def test_single_provider_disable_paging(self):
        # Turn off paging; should get just first 20 trips
        trips = self._items_from_app(self.bogus_data_app, 'trips', paging=False)
        self.assertEqual(len(trips), 20)
