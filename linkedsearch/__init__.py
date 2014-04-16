from StringIO import StringIO
from RDFClosure import DeductiveClosure, OWLRL_Semantics
from elasticsearch import Elasticsearch
import feedparser
from rdflib import Graph, URIRef, Literal
import requests
from lxml.cssselect import CSSSelector
from lxml.etree import fromstring
import json
import pyld
from celery import Celery


app = Celery('linkedsearch', broker='amqp://guest@localhost//')


def create_graph_from_rss_entry(uri, entry):
    graph = Graph()
    graph.add((URIRef(uri), URIRef('http://schema.org/headline'), Literal(entry.title)))
    graph.add((URIRef(uri), URIRef('http://schema.org/url'), Literal(entry.link)))
    graph.add((URIRef(uri), URIRef('http://schema.org/description'), Literal(entry.description)))

    return graph


def convert_graph_to_json(graph):
    return json.loads(graph.serialize(format='json-ld'))


def index_graph(uri, graph):
    print graph.serialize(format='turtle')
    print
    print '=' * 80
    print

    json_ld = convert_graph_to_json(graph)
    body = {'jsonld': json_ld}
    es = Elasticsearch()
    es.index(index='search', id=uri, body=body, doc_type='item')


def expand_graph_via_lookup(uri, graph):
    print "Fetching " + uri
    graph.parse(uri, format='rdfa')

    for title_uri in ['http://iptc.org/std/rNews/2011-10-07#headline', 'http://ogp.me/ns#title']:
        graph.add((URIRef(title_uri), URIRef(u'http://www.w3.org/2002/07/owl#sameAs'), URIRef('http://schema.org/headline')))

    for description_uri in ['http://iptc.org/std/rNews/2011-10-07#description']:
        graph.add((URIRef(description_uri), URIRef(u'http://www.w3.org/2002/07/owl#sameAs'), URIRef('http://schema.org/description')))

    for url_uri in ['http://opengraphprotocol.org/schema/url', 'http://ogp.me/ns#url']:
        graph.add((URIRef(url_uri), URIRef(u'http://www.w3.org/2002/07/owl#sameAs'), URIRef('http://schema.org/url')))

    for thumbnail_uri in ['http://iptc.org/std/rNews/2011-10-07#thumbnailUrl']:
        graph.add((URIRef(thumbnail_uri), URIRef(u'http://www.w3.org/2002/07/owl#sameAs'), URIRef('http://schema.org/thumbnailUrl')))

    DeductiveClosure(OWLRL_Semantics).expand(graph)

    return graph


def index_feed(feed_url):
    news = feedparser.parse(feed_url)
    for entry in news.entries:
        uri = unicode(entry.link).split('#')[0].strip()
        #graph = create_graph_from_rss_entry(uri, entry)
        graph = Graph()
        expand_and_index.delay(uri, graph)


def index_news():
    index_feed('http://feeds.bbci.co.uk/news/rss.xml')


def index_sport():
    index_feed('http://feeds.bbci.co.uk/sport/0/rss.xml?edition=uk')

@app.task
def expand_and_index(uri, graph):
    graph = expand_graph_via_lookup(uri, graph)
    index_graph(uri, graph)


def index_programmes():
    response = requests.get('https://api.live.bbc.co.uk/pips/api/v1/change/?rows=100',
                            cert='/etc/pki/certificate.pem', verify=False,
                            headers={'Accept': 'application/xml'})

    change_doc = fromstring(response.content)
    sel = CSSSelector('pips|change[rel="pips-meta:episode"]', namespaces={'pips': 'http://ns.webservices.bbc.co.uk/2006/02/pips'})
    for episode in sel(change_doc):
        pid = episode.get('pid')

        programmes_url = u'http://www.bbc.co.uk/programmes/{0}#programme'.format(pid)
        rdf_url = u'http://www.bbc.co.uk/programmes/{0}.rdf'.format(pid)

        graph = Graph()
        graph.parse(rdf_url)

        expand_and_index.delay(programmes_url, graph)
