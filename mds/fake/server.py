
from flask import Flask, request, jsonify, url_for
import json, base64

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
    def __init__(self, all_items, serialized_cursor=None, page_size=20):
        self.items = all_items
        self.cursor = PaginationCursor(serialized_cursor)
        self.page_size = page_size

    def next_cursor_serialized(self):
        offset = self.cursor.offset
        return PaginationCursor(offset=offset+self.page_size).serialize()

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

    return True

def params_match_status_change(params, sc):
    return True

def make_static_server_app(trips=[],
                           status_changes=[],
                           version='0.2.0',
                           page_size=20):
    app = Flask('mds_static')
    store = {
        'trips': trips,
        'status_changes': status_changes,
    }

    @app.route('/trips')
    def trips():
        params = {
            'vehicle_id': request.args.get('vehicle_id'),
            # TODO: support other params
        }
        selected_trips = [t for t in store['trips'] if params_match_trip(params, t)]
        paginator = InMemoryPaginator(selected_trips,
                                      serialized_cursor=request.args.get('cursor'),
                                      page_size=page_size)
        return jsonify(make_mds_response_data(
            version, 'trips', paginator, **params
        ))

    @app.route('/status_changes')
    def status_changes():
        params = {
            # TODO
        }
        selected_items = [sc for sc in store['status_changes'] if params_match_status_change(params, sc)]
        paginator = InMemoryPaginator(selected_items,
                                      serialized_cursor=request.args.get('cursor'),
                                      page_size=page_size)
        return jsonify(make_mds_response_data(
            version, 'status_changes', paginator, **params
        ))

    return app
