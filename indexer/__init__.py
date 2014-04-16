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


#def index_item(uri, item):
#    if uri.startswith('http://'):
#        for p, o in read_page_for_metadata(uri):
#            item[p] = o
#
#        # We know programmes have extra stuff as RDF
#        if uri.startswith('http://www.bbc.co.uk/programmes/'):
#            item['#programme'] = {}
#            for p, o in read_page_for_metadata(uri + '#programme'):
#                item['#programme'][p] = o
#            for p, o in read_page_for_metadata(uri + '.rdf'):
#                item[p] = o
#
#
#    es = Elasticsearch()
#    print u'Pushing: {0}'.format(item)
#    es.index(index='search', id=uri, doc_type='item', body=item)
#

#def read_page_for_metadata(url):
#    g = Graph()
#    g.parse(url)
#    return g.predicate_objects(URIRef(url))


def create_graph_from_rss_entry(uri, entry):
    graph = Graph()
    graph.add((URIRef(uri), URIRef('http://schema.org/headline'), Literal(entry.title)))
    graph.add((URIRef(uri), URIRef('http://schema.org/url'), Literal(entry.link)))
    graph.add((URIRef(uri), URIRef('http://schema.org/description'), Literal(entry.description)))

    return graph


def index_graph(uri, graph):
    json_ld = json.loads(graph.serialize(format='json-ld'))
    #print json_ld
    #if len(json_ld) != 1:
    #    raise Exception('multi!')
    body = {'jsonld': json_ld}
    es = Elasticsearch()
    #print u'Pushing: {0}'.format(body)
    es.index(index='search', id=uri, body=body, doc_type='item')


def expand_graph_via_lookup(uri, graph):
    graph.parse(uri)
    return graph


def index_feed(feed_url):
    news = feedparser.parse(feed_url)
    for entry in news.entries:
        uri = unicode(entry.link).split('#')[0].strip()
        graph = create_graph_from_rss_entry(uri, entry)
        graph = expand_graph_via_lookup(uri, graph)
        index_graph(uri, graph)


def index_news():
    index_feed('http://feeds.bbci.co.uk/news/rss.xml')


def index_sport():
    index_feed('http://feeds.bbci.co.uk/sport/0/rss.xml?edition=uk')


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

        print programmes_url

        #rdfa_data_file = StringIO()
        #rdfa_data = requests.get(programmes_url).text
        #rdfa_data = rdfa_data.replace(u'xsd:datetime', u'xsd:dateTime')
        #rdfa_data_file.write(rdfa_data)
        #
        graph = Graph()
        print len(graph.serialize(format='turtle'))
        graph.parse(programmes_url)
        print len(graph.serialize(format='turtle'))
        graph.parse(rdf_url, format='xml')
        print len(graph.serialize(format='turtle'))
        graph.add((URIRef(programmes_url), URIRef(u'http://www.w3.org/2002/07/owl#sameAs'), URIRef(u'http://www.bbc.co.uk/programmes/{0}'.format(pid))))

        print len(graph.serialize(format='turtle'))
        DeductiveClosure(OWLRL_Semantics).expand(graph)

        print len(graph.serialize(format='turtle'))

        index_graph(programmes_url, graph)
