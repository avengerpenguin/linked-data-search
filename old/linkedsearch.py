from StringIO import StringIO
from RDFClosure import DeductiveClosure, OWLRL_Semantics
from elasticsearch import Elasticsearch
import feedparser
import rdflib
from rdflib import Graph, URIRef, Literal
import requests
from lxml.cssselect import CSSSelector
from lxml.etree import fromstring
import json
from celery import Celery
from FuXi.Rete.RuleStore import SetupRuleStore
from FuXi.Rete.Util import generateTokenSet
from FuXi.Horn.HornRules import HornFromN3
from datetime import timedelta
import lxml.etree as ET


CELERYBEAT_SCHEDULE = {
    'poll-every-five-minutes': {
        'task': 'poll',
        'schedule': timedelta(minutes=5)
    },
}

app = Celery('linkedsearch', broker='amqp://guest@localhost//')

def combine_pipeline(source, pipeline):
    """Combine source and pipeline and return a generator."""
    generator = source
    for step in pipeline:
        generator = step(generator)
    return generator


def run_pipeline(iter):
    for _ in iter:
        pass


def fetch_feeds(feeds):
    for feed in feeds:
        yield feedparser.parse(feed)


def break_feeds_into_entries(feeds):
    for feed in feeds:
        for entry in feed.entries:
            yield entry


def convert_rss_entries_to_graphs(entries):
    for entry in entries:
        graph = rdflib.Graph()
        graph.bind('rss', 'http://purl.org/rss/1.0/')
        RSS_NAMESPACE = rdflib.Namespace('http://purl.org/rss/1.0/')

        for prop in entry.keys():
            subject = rdflib.URIRef(entry.id)
            predicate = RSS_NAMESPACE[prop]
            object = rdflib.Literal(entry[prop])

            statement = (subject, predicate, object)
            graph.add(statement)

        yield graph


def enrich_graphs_with_metadata_in_page(graphs):
    for graph in graphs:
        for subject in graph.subjects():
            graph.parse(str(subject))
        yield graph


def apply_inference_rules(graph, ruleset):
    rule_store, rule_graph, network = SetupRuleStore(makeNetwork=True)
    closure_delta = Graph()
    network.inferredFacts = closure_delta
    for rule in HornFromN3(ruleset):
        network.buildNetworkFromClause(rule)

    network.feedFactsToAdd(generateTokenSet(graph))

    return graph + closure_delta


def infer_required_fields(graphs):
    for graph in graphs:
        yield apply_inference_rules(graph, 'rules.n3')


def serialise_graphs(format):
    def graphs_serialiser(graphs):
        for graph in graphs:
            yield graph.serialize(format=format)
    return graphs_serialiser


def parse_json_strings(strings):
    for string in strings:
        yield json.loads(string)


def index_jsonld_in_elasticsearch(elastic_search):
    def indexer(jsonld_strings):
        for jsonld_string in jsonld_strings:
            body = {'jsonld': json.loads(jsonld_string)}
            elastic_search.index(index='search', body=body, doc_type='item')
    return indexer


def print_items(items):
    for item in items:
        print item


if __name__ == '__main__':
    feeds = ['http://feeds.bbci.co.uk/news/rss.xml', 'http://feeds.bbci.co.uk/sport/0/rss.xml?edition=uk']
    rss_pipeline_steps = [
        fetch_feeds,
        break_feeds_into_entries,
        convert_rss_entries_to_graphs,
        enrich_graphs_with_metadata_in_page,
        infer_required_fields,
        serialise_graphs('json-ld'),
        index_jsonld_in_elasticsearch(Elasticsearch())
    ]
    rss_pipeline = combine_pipeline(feeds, rss_pipeline_steps)
    run_pipeline(rss_pipeline)
















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

    #for title_uri in ['http://iptc.org/std/rNews/2011-10-07#headline', 'http://ogp.me/ns#title']:
    #    graph.add((URIRef(title_uri), URIRef(u'http://www.w3.org/2002/07/owl#sameAs'), URIRef('http://schema.org/headline')))
    #
    #for description_uri in ['http://iptc.org/std/rNews/2011-10-07#description']:
    #    graph.add((URIRef(description_uri), URIRef(u'http://www.w3.org/2002/07/owl#sameAs'), URIRef('http://schema.org/description')))
    #
    #for url_uri in ['http://opengraphprotocol.org/schema/url', 'http://ogp.me/ns#url']:
    #    graph.add((URIRef(url_uri), URIRef(u'http://www.w3.org/2002/07/owl#sameAs'), URIRef('http://schema.org/url')))
    #
    #for thumbnail_uri in ['http://iptc.org/std/rNews/2011-10-07#thumbnailUrl']:
    #    graph.add((URIRef(thumbnail_uri), URIRef(u'http://www.w3.org/2002/07/owl#sameAs'), URIRef('http://schema.org/thumbnailUrl')))

    #DeductiveClosure(OWLRL_Semantics).expand(graph)

    rule_store, rule_graph, network = SetupRuleStore(makeNetwork=True)
    closureDeltaGraph=Graph()
    network.inferredFacts = closureDeltaGraph
    for rule in HornFromN3('rules.n3'):
        network.buildNetworkFromClause(rule)

    network.feedFactsToAdd(generateTokenSet(graph))

    return graph + closureDeltaGraph


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
        graph.add((URIRef(programmes_url.split('#')[0]), URIRef('http://xmlns.com/foaf/0.1/primaryTopic'), URIRef(programmes_url)))


        expand_and_index.delay(programmes_url, graph)


