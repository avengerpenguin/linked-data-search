import json
import datetime
from celery import Celery
import os
from FuXi.Horn.HornRules import HornFromN3
from FuXi.Rete.RuleStore import SetupRuleStore
from FuXi.Rete.Util import generateTokenSet
from ldsearch.enrichers import programmes_rdf, dbpedia_spotlight
from rdflib import Graph, URIRef, RDF, Literal
from elasticsearch import Elasticsearch
from pyld import jsonld
import logging
import feedparser


celery = Celery('ldsearch', broker=os.getenv('BROKER_URL', None))
celery.conf.CELERY_TIMEZONE = 'Europe/London'
celery.conf.CELERYBEAT_SCHEDULE = {
    'fetch_news': {
        'task': 'ldsearch.tasks.news_poll',
        'schedule': datetime.timedelta(minutes=5),
    },
}


enrichers = [programmes_rdf, dbpedia_spotlight]
es = Elasticsearch(hosts=os.getenv('ELASTICSEARCH_HOST', 'localhost:9200'))


@celery.task(queue='notify')
def news_poll():
    feed = feedparser.parse('http://feeds.bbci.co.uk/news/rss.xml')
    for item in feed['entries']:
        uri = item['id']
        (notify.s(uri) | enrich.s() | infer.s() | ingest.s()).apply_async()


@celery.task(queue='notify')
def notify(uri):
    g = Graph()
    g.add((URIRef(uri), RDF.type, URIRef('http://www.bbc.co.uk/search/schema/ContentItem')))
    g.add((URIRef(uri), URIRef('http://www.bbc.co.uk/search/schema/url'), Literal(uri)))
    g.parse(uri)

    return g.serialize(format='nt').decode('utf-8')


@celery.task(queue='enrich')
def enrich(ntriples):
    graph = Graph()
    graph.parse(data=ntriples, format='nt')
    for enricher in enrichers:
        try:
            enricher.enrich(graph)
        except:
            # Allow things to fail and we will pass through unenriched (for now)
            continue

    return graph.serialize(format='nt').decode('utf-8')


@celery.task(queue='infer')
def infer(ntriples):
    graph = Graph()
    graph.parse(data=ntriples, format='nt')
    rule_store, rule_graph, network = SetupRuleStore(makeNetwork=True)
    rules = HornFromN3(os.path.join(
        os.path.dirname(os.path.realpath(__file__)), 'rules.n3'))

    closure_delta = Graph()
    network.inferredFacts = closure_delta
    for rule in rules:
        network.buildNetworkFromClause(rule)

    network.feedFactsToAdd(generateTokenSet(graph))

    new_graph = graph + closure_delta

    return new_graph.serialize(format='nt').decode('utf-8')


@celery.task(queue='ingest')
def ingest(ntriples):

    graph = Graph()
    graph.parse(data=ntriples, format='nt')

    expanded = jsonld.expand(
        json.loads(
            graph.serialize(format='json-ld').decode('utf-8')))

    mandatory_props = [
        'http://www.bbc.co.uk/search/schema/title',
        'http://www.bbc.co.uk/search/schema/url'
    ]

    for json_object in expanded:

        uri = json_object['@id']

        for prop in mandatory_props:
            if prop not in json_object:
                logging.warning(
                    "Not indexing %s due to missing property: %s", uri, prop)

        es.index(index='bbc',
                 body=jsonld.expand(json_object)[0],
                 doc_type='item',
                 id=uri)
