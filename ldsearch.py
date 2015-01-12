#!/usr/bin/env python

import os

from rdflib import URIRef, RDF
from enrichers import programmes_rdf, dbpedia_spotlight
from flask import Flask, request
from FuXi.Rete.RuleStore import SetupRuleStore
from FuXi.Rete.Util import generateTokenSet
from FuXi.Horn.HornRules import HornFromN3
from rdflib import Graph
from celery import Celery
from pyld import jsonld
import json
from elasticsearch import Elasticsearch
from celery import chain


def make_celery(app):
    celery = Celery(app.import_name, broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task
    class ContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery


app = Flask(__name__)
app.config.update(
    CELERY_BROKER_URL = 'redis://guest@localhost:6379/0',
    CELERY_RESULT_BACKEND = 'redis://guest@localhost:6379/0'
)
celery = make_celery(app)
es = Elasticsearch(hosts=os.getenv('ELASTICSEARCH_HOST', 'localhost:9200'))


@app.route('/', methods=['POST'])
def index():
    uris = [line.strip() # trim any whitespace
            for line in request.get_data().decode('utf-8').splitlines()
            if not line.startswith('#')]

    for uri in uris:
        (notify.s(uri) | enrich.s() | infer.s() | ingest.s()).apply_async()

    return 'Accepted!\n', 202, {'Content-Type': 'text/plain'}


@celery.task(queue='notify')
def notify(uri):
    g = Graph()
    g.add((URIRef(uri), RDF.type, URIRef('http://www.bbc.co.uk/search/schema/ContentItem')))
    g.parse(uri)

    return g.serialize(format='nt')


enrichers = [programmes_rdf]


@celery.task(queue='enrich')
def enrich(ntriples):
    graph = Graph()
    graph.parse(ntriples, format='nt')
    for enricher in enrichers:
        try:
            enricher.enrich(graph)
        except:
            # Allow things to fail and we will pass through unenriched (for now)
            continue

    return graph.serialize(format='nt')


@celery.task(queue='infer')
def infer(ntriples):
    graph = Graph()
    graph.parse(ntriples, format='nt')
    rule_store, rule_graph, network = SetupRuleStore(makeNetwork=True)
    rules = HornFromN3(os.path.join(
        os.path.dirname(os.path.realpath(__file__)), 'rules.n3'))

    closure_delta = Graph()
    network.inferredFacts = closure_delta
    for rule in rules:
        network.buildNetworkFromClause(rule)

    network.feedFactsToAdd(generateTokenSet(graph))

    new_graph = closure_delta

    return new_graph.serialize(format='nt')


@celery.task(queue='ingest')
def ingest(ntriples):
    graph = Graph()
    graph.parse(ntriples, format='nt')
    body = {'jsonld': jsonld.expand(json.loads(graph.serialize(format='json-ld').decode('utf-8')))}

    for uri in graph.subjects(predicate=RDF.type, object=URIRef('http://www.bbc.co.uk/search/schema/ContentItem')):
        es.index(index='bbc', body=body, doc_type='item', id=str(uri))


if __name__ == '__main__':
    app.run(debug=True, port=int(os.getenv('PORT', 5000)))
