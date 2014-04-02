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
                    'title': hit['_source']['http://schema.org/headline'],
                    'synopsis': hit['_source']['http://schema.org/description'],
                    'url': hit['_source']['http://schema.org/url']
                } for hit in results['hits']['hits']
            ]
        }
    }


run(host='localhost', port=8080, debug=True, reloader=True)
