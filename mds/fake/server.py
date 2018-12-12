
from flask import Flask, request, jsonify, url_for
import mds.json
import json, base64
from datetime import datetime
import pytz

epoch = pytz.utc.localize(datetime(1970, 1, 1, 0, 0, 0))

def ensure_unixtime(dt_or_float):
    if isinstance(dt_or_float, datetime):
        return (dt_or_float - epoch).total_seconds()
    return dt_or_float

class UnixtimeJSONEncoder(mds.json.CustomJsonEncoder):
    date_format = 'unix'

class PaginationCursor(object):
    def __init__(self, serialized_cursor=None, offset=None):
        if offset is not None and serialized_cursor is not None:
            raise RuntimeError('Cannot initialize with non-None offset AND non-None cursor')

        if serialized_cursor is not None:
            data = json.loads(base64.b64decode(serialized_cursor).decode('utf-8'))
            self.offset = data['o']
        else:
            self.offset = 0 if offset is None else offset

    def serialize(self):
        return base64.b64encode(json.dumps({ 'o': self.offset }).encode('utf-8'))

class InMemoryPaginator(object):
    def __init__(self, all_items, serialized_cursor=None, page_size=20, next_page_shortness=0):
        self.items = all_items
        self.cursor = PaginationCursor(serialized_cursor)
        self.page_size = page_size
        self.next_page_shortness = next_page_shortness

    def next_cursor_serialized(self):
        offset = self.cursor.offset
        return PaginationCursor(offset=offset+self.page_size-self.next_page_shortness).serialize()

    def get_page(self):
        offset = self.cursor.offset
        return self.items[offset:offset+self.page_size]

def make_mds_response_data(version, resource_name, paginator, **params):
    return {
        'version': version,
        'links': {
            'next': url_for(resource_name,
                            cursor=paginator.next_cursor_serialized(),
                            _external=True,
                            **params),
        },
        'data': {
            resource_name: paginator.get_page(),
        }
    }

def params_match_trip(params, trip):
    vehicle_id = params.get('vehicle_id')
    if vehicle_id and trip['vehicle_id'] != vehicle_id:
        return False

    device_id = params.get('device_id')
    if device_id and trip['device_id'] != device_id:
        return False

    start_time = params.get('start_time')
    if start_time and ensure_unixtime(trip['start_time']) < float(start_time):
        return False

    end_time = params.get('end_time')
    if end_time and ensure_unixtime(trip['end_time']) > float(end_time):
        return False

    bbox = params.get('bbox')
    if bbox is not None:
        raise NotImplementedError('fake server does not support bbox queries')

    return True

def params_match_status_change(params, sc):
    start_time = params.get('start_time')
    if start_time and ensure_unixtime(sc['event_time']) < float(start_time):
        return False

    end_time = params.get('end_time')
    if end_time and ensure_unixtime(sc['event_time']) > float(end_time):
        return False

    bbox = params.get('bbox')
    if bbox is not None:
        raise NotImplementedError('fake server does not support bbox queries')

    return True

def make_static_server_app(trips=[],
                           status_changes=[],
                           version='0.2.0',
                           page_size=20,
                           next_page_shortness=0):
    app = Flask('mds_static')

    options = {
        'next_page_shortness': next_page_shortness,
        'page_size': page_size,
    }

    store = {
        'trips': trips,
        'status_changes': status_changes,
    }
    app.config['STATIC_MDS_DATA'] = store
    app.config['STATIC_MDS_OPTIONS'] = options
    app.json_encoder = UnixtimeJSONEncoder

    @app.route('/trips')
    def trips():
        supported_param_keys =  ('device_id', 'vehicle_id', 'start_time', 'end_time', 'bbox')
        params = { k: request.args.get(k) for k in request.args if k in supported_param_keys}
        selected_trips = [t for t in store['trips'] if params_match_trip(params, t)]
        paginator = InMemoryPaginator(selected_trips,
                                      serialized_cursor=request.args.get('cursor'),
                                      page_size=page_size,
                                      next_page_shortness=next_page_shortness)
        return jsonify(make_mds_response_data(
            version, 'trips', paginator, **params
        ))

    @app.route('/status_changes')
    def status_changes():
        supported_param_keys = ('start_time', 'end_time', 'bbox')
        params = { k: request.args.get(k) for k in request.args if k in supported_param_keys}
        selected_items = [sc for sc in store['status_changes'] if params_match_status_change(params, sc)]
        paginator = InMemoryPaginator(selected_items,
                                      serialized_cursor=request.args.get('cursor'),
                                      page_size=page_size)
        return jsonify(make_mds_response_data(
            version, 'status_changes', paginator, **params
        ))

    return app
