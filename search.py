import json
from bottle import route, run, request
from bottle.ext.pystache import view
from elasticsearch import Elasticsearch
from rdflib import Graph, URIRef


QUERY = """
SELECT ?title
WHERE {
    OPTIONAL {
        ?entity <http://ogp.me/ns#title>|<http://schema.org/headline> ?title .
    }
}
"""


@route('/')
@view('home')
def index():
    return {}


@route('/search')
@view('results')
def search():

    query = request.query.q or '*'
    response = Elasticsearch().search(index='search', q=query)


    for hit in response['hits']['hits']:
        print Graph().parse(data=json.dumps(hit['_source']['jsonld']), format='json-ld').serialize(format="turtle")

    for hit in response['hits']['hits']:
        for row in Graph().parse(
                    data=json.dumps(hit['_source']['jsonld']),
                    format='json-ld'
                ).query(QUERY, initBindings={'entity': URIRef(hit['_id'])}):
            print row

    results = {
        'hits': {
            'total': response['hits']['total'],
            'hits': [
                Graph().parse(
                    data=json.dumps(hit['_source']['jsonld']),
                    format='json-ld'
                ).query(QUERY, initBindings={'entity': URIRef(hit['_id'])}).bindings[0]
                for hit in response['hits']['hits']
            ]
        }
    }
    print results
    return results


    #{
    #    'title': try_props(hit['_source'], ['http://ogp.me/ns#title', 'http://schema.org/headline']),
    #    'synopsis': try_props(hit['_source'], ['http://schema.org/description']),
    #    'url': try_props(hit['_source'], ['http://schema.org/url']),
    #    'image': try_props(hit['_source'], ['http://ogp.me/ns#image']),
    #} for hit in results['hits']['hits']


def try_props(data, prop_list):
    for prop in prop_list:
        if prop in data:
            if type(data[prop]) is list:
                for value in data[prop]:
                    if '@language' not in value or value[
                        '@language'] == 'en-gb':
                        prop_value = value['@value']
                        break
            else:
                prop_value = data[prop]
            return prop_value


run(host='localhost', port=8080, debug=True, reloader=True)
