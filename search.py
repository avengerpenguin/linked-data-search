from bottle import route, run, request
from bottle.ext.pystache import view
from elasticsearch import Elasticsearch


@route('/')
@view('home')
def index():
    return {}

@route('/search')
@view('results')
def search():
    query = request.query.q or '*'

    es = Elasticsearch()
    results = es.search(index='search', q=query)

    return {
        'hits': {
            'total': results['hits']['total'],
            'hits': [
                {
                    'title': try_props(hit['_source'], ['http://ogp.me/ns#title', 'http://schema.org/headline']),
                    'synopsis': try_props(hit['_source'], ['http://schema.org/description']),
                    'url': try_props(hit['_source'], ['http://schema.org/url']),
                    'image': try_props(hit['_source'], ['http://ogp.me/ns#image']),
                } for hit in results['hits']['hits']
            ]
        }
    }

def try_props(data, prop_list):
    for prop in prop_list:
        if prop in data:
            return data[prop]

run(host='localhost', port=8080, debug=True, reloader=True)
