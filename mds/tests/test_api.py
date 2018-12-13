import unittest, re, uuid, os, json
from contextlib import contextmanager
import requests_mock
from urllib3.util import parse_url

from mds.fake.server import make_static_server_app
from mds.providers import Provider
from mds.api import ProviderClient


def requests_mock_with_app(app, netloc='testserver', scheme='https'):
    client = app.test_client()
    def get_app_response(request, response_context):
        url_object = parse_url(request.url)
        app_response = client.get(url_object.request_uri, base_url=f'{scheme}://{netloc}/')
        response_context.status_code = app_response.status_code
        response_context.headers = app_response.headers
        return app_response.data

    mock = requests_mock.Mocker()
    matcher = re.compile(f'^{scheme}://{netloc}/')
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


def get_fixture_json(filename):
    filename = os.path.join(os.path.dirname(__file__), 'fixtures', filename)
    with open(filename) as f:
        return json.load(f)

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

        self.small_app = make_static_server_app(
            trips=get_fixture_json('mds_tiny_0.2.0_trips_only.json'),
            status_changes=get_fixture_json('mds_tiny_0.2.0_status_changes_only.json'),
            version='0.2.0',
            page_size=20,
            next_page_shortness=1,
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

    def test_nonoverlapping_trip_query_window_misses_trips(self):
        """Verify spec-compliant trip filtering on fake server

        The MDS Provider spec requires that `start_time` parameter apply to
        `start_time` property and `end_time` parameter apply to the `end_time`
        property. This test verifies that our fake server follows the spec, and
        that the client issues request parameters correctly.

        c.f. https://github.com/CityOfLosAngeles/mobility-data-specification/blob/0.2.x/provider/README.md#trips-query-parameters
        """
        def get_trips(**api_params):
            return self._items_from_app(self.small_app, 'trips', **api_params)

        # this timestamp chosen so that it's during a trip from the fixture
        pivot = 1544512585
        two_hours = get_trips(start_time=pivot - 3600, end_time=pivot + 3600)
        hour1 = get_trips(start_time=pivot - 3600, end_time=pivot)
        hour2 = get_trips(start_time=pivot, end_time=pivot + 3600)

        self.assertLess(len(hour1) + len(hour2), len(two_hours))

        # just for good measure, this is a trip id from the fixture that we
        # expect to miss.
        self.assertIn('801e3dbc-d47b-4e20-9862-eca76f379526', [t['trip_id'] for t in two_hours])
        self.assertNotIn('801e3dbc-d47b-4e20-9862-eca76f379526', [t['trip_id'] for t in hour1])
        self.assertNotIn('801e3dbc-d47b-4e20-9862-eca76f379526', [t['trip_id'] for t in hour2])
