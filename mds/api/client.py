"""
MDS Provider API client implementation.
"""

from datetime import datetime
import json
import requests
import mds
from mds.api.auth import OAuthClientCredentialsAuth
from mds.providers import get_registry, Provider

class ProviderClientBase(OAuthClientCredentialsAuth):

    def _auth_session(self, provider):
        """
        Internal helper to establish an authenticated session with the :provider:.
        """
        if hasattr(provider, "token") and not hasattr(provider, "token_url"):
            # auth token defined by provider
            return self.auth_token_session(provider)
        else:
            # OAuth 2.0 client_credentials grant flow
            return self.oauth_session(provider)

    def _build_url(self, provider, endpoint):
        """
        Internal helper for building API urls.
        """
        url = provider.mds_api_url

        if hasattr(provider, "mds_api_suffix"):
            url += "/" + getattr(provider, "mds_api_suffix").rstrip("/")

        url += "/" + endpoint

        return url

    def _date_format(self, dt):
        """
        Internal helper to format datetimes for querystrings.
        """
        return int(dt.timestamp()) if isinstance(dt, datetime) else int(dt)

    def _prepare_status_changes_params(
        self,
        start_time=None,
        end_time=None,
        bbox=None,
        **kwargs):

        # convert datetimes to querystring friendly format
        if start_time is not None:
            start_time = self._date_format(start_time)
        if end_time is not None:
            end_time = self._date_format(end_time)

        # gather all the params together
        return {
            **dict(start_time=start_time, end_time=end_time, bbox=bbox),
            **kwargs
        }

    def _prepare_trips_params(
        self,
        device_id=None,
        vehicle_id=None,
        start_time=None,
        end_time=None,
        bbox=None,
        **kwargs):

        # convert datetimes to querystring friendly format
        if start_time is not None:
            start_time = self._date_format(start_time)
        if end_time is not None:
            end_time = self._date_format(end_time)

        # gather all the params togethers
        return {
            **dict(device_id=device_id, vehicle_id=vehicle_id, start_time=start_time, end_time=end_time, bbox=bbox),
            **kwargs
        }


class ProviderClient(ProviderClientBase):
    def __init__(self, provider):
        self.provider = provider

    def get_trips(self, **kwargs):
        return list(self.iterate_trips_pages(**kwargs))

    def get_status_changes(self, **kwargs):
        return list(self.iterate_status_change_pages(**kwargs))

    def iterate_trips_pages(self, paging=True, **kwargs):
        params = self._prepare_trips_params(**kwargs)
        return self.request(mds.TRIPS, params, paging)

    def iterate_status_change_pages(self, paging=True, **kwargs):
        params = self._prepare_status_changes_params(**kwargs)
        return self.request(mds.STATUS_CHANGES, params, paging)

    def request(self, endpoint, params, paging):
        url = self._build_url(self.provider, endpoint)
        session = self._auth_session(self.provider)
        for page in self._iterate_pages_from_session(session, endpoint, url, params):
            yield page
            if not paging:
                break

    def _iterate_pages_from_session(self, session, endpoint, url, params):
        """
        Request items from endpoint, following pages
        """

        def __has_data(page):
            """
            Checks if this :page: has a "data" property with a non-empty payload
            """
            data = page["data"] if "data" in page else {"__payload__": []}
            payload = data[endpoint] if endpoint in data else []
            print(f"Got payload with {len(payload)} {endpoint}")
            return len(payload) > 0

        def __next_url(page):
            """
            Gets the next URL or None from :page:
            """
            return page["links"].get("next") if "links" in page else None

        response = session.get(url, params=params)
        response.raise_for_status()

        this_page = response.json()
        if __has_data(this_page):
            yield this_page

            next_url = __next_url(this_page)
            while next_url is not None:
                response = session.get(next_url)
                response.raise_for_status()
                this_page = response.json()
                if __has_data(this_page):
                    yield this_page
                    next_url = __next_url(this_page)
                else:
                    break


class MultipleProviderClient(ProviderClientBase):
    """
    Client for MDS Provider APIs
    """
    def __init__(self, providers=None, ref=None):
        """
        Initialize a new MultipleProviderClient object.

        :providers: is a list of Providers this client tracks by default. If None is given, downloads and uses the official Provider registry.

        When using the official Providers registry, :ref: could be any of:
            - git branch name
            - commit hash (long or short)
            - git tag
        """
        self.providers = providers if providers is not None else get_registry(ref)

    def _request_from_providers(self, providers, endpoint, params, paging):
        """
        Internal helper for sending requests.

        Returns a dict of provider => payload(s).
        """
        def __describe(res):
            """
            Prints details about the given response.
            """
            print(f"Requested {res.url}, Response Code: {res.status_code}")
            print("Response Headers:")
            for k,v in res.headers.items():
                print(f"{k}: {v}")

            if r.status_code is not 200:
                print(r.text)

        results = {}
        for provider in providers:
            client = ProviderClient(provider)
            try:
                results[provider] = list(client.request(endpoint, params, paging))
            except requests.RequestException as exc:
                __describe(exc.response)

        return results

    def get_status_changes(
        self,
        providers=None,
        paging=True,
        **kwargs):
        """
        Request Status Changes data. Returns a dict of provider => list of status_changes payload(s)

        Supported keyword args:

            - `providers`: One or more Providers to issue this request to.
                           The default is to issue the request to all Providers.

            - `start_time`: Filters for status changes where `event_time` occurs at or after the given time
                            Should be a datetime object or numeric representation of UNIX seconds

            - `end_time`: Filters for status changes where `event_time` occurs at or before the given time
                          Should be a datetime object or numeric representation of UNIX seconds

            - `bbox`: Filters for status changes where `event_location` is within defined bounding-box.
                      The order is defined as: southwest longitude, southwest latitude,
                      northeast longitude, northeast latitude (separated by commas).

                      e.g.

                      bbox=-122.4183,37.7758,-122.4120,37.7858

            - `paging`: True (default) to follow paging and request all available data.
                        False to request only the first page.
        """
        if providers is None:
            providers = self.providers

        params = self._prepare_status_changes_params(**kwargs)

        # make the request(s)
        status_changes = self._request_from_providers(providers, mds.STATUS_CHANGES, params, paging)

        return status_changes

    def get_trips(
        self,
        providers=None,
        paging=True,
        **kwargs):
        """
        Request Trips data. Returns a dict of provider => list of trips payload(s).

        Supported keyword args:

            - `providers`: One or more Providers to issue this request to.
                           The default is to issue the request to all Providers.

            - `device_id`: Filters for trips taken by the given device.

            - `vehicle_id`: Filters for trips taken by the given vehicle.

            - `start_time`: Filters for trips where `start_time` occurs at or after the given time
                            Should be a datetime object or numeric representation of UNIX seconds

            - `end_time`: Filters for trips where `end_time` occurs at or before the given time
                          Should be a datetime object or numeric representation of UNIX seconds

            - `bbox`: Filters for trips where and point within `route` is within defined bounding-box.
                      The order is defined as: southwest longitude, southwest latitude,
                      northeast longitude, northeast latitude (separated by commas).

                      e.g.

                      bbox=-122.4183,37.7758,-122.4120,37.7858

            - `paging`: True (default) to follow paging and request all available data.
                        False to request only the first page.
        """
        if providers is None:
            providers = self.providers

        params = self._prepare_trips_params(**kwargs)

        # make the request(s)
        trips = self._request_from_providers(providers, mds.TRIPS, params, paging)

        return trips
