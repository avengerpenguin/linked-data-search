import json
import os
from FuXi.Horn.HornRules import HornFromN3
from FuXi.Rete.RuleStore import SetupRuleStore
from FuXi.Rete.Util import generateTokenSet
from ldsearch.enrichers import programmes_rdf, dbpedia_spotlight
from rdflib import Graph, URIRef, RDF, Literal
from elasticsearch import Elasticsearch
from pyld import jsonld


enrichers = [programmes_rdf, dbpedia_spotlight]
es = Elasticsearch(hosts=os.getenv('ELASTICSEARCH_HOST', 'localhost:9200'))


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


def ingest(ntriples):
    graph = Graph()
    graph.parse(data=ntriples, format='nt')
    body = {
        'jsonld': jsonld.expand(
            json.loads(graph.serialize(format='json-ld').decode('utf-8')),
            {
                'expandContext': {
                    '@vocab': 'http://www.bbc.co.uk/search/schema/'
                }
            }
        )
    }

    mandatory_props = [
        URIRef('http://www.bbc.co.uk/search/schema/title'),
        URIRef('http://www.bbc.co.uk/search/schema/url')
    ]

    for uri in graph.subjects(
            predicate=RDF.type,
            object=URIRef('http://www.bbc.co.uk/search/schema/ContentItem')):

        for prop in mandatory_props:
            if not (uri, prop) in graph.subject_predicates():
                raise Exception(
                    'Item {} missing mandatory field {}'.format(str(uri), str(prop)))

        es.index(index='bbc', body=body, doc_type='item', id=str(uri))


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


def notify(uri):
    g = Graph()
    g.add((URIRef(uri), RDF.type, URIRef('http://www.bbc.co.uk/search/schema/ContentItem')))
    g.add((URIRef(uri), URIRef('http://www.bbc.co.uk/search/schema/url'), Literal(uri)))
    g.parse(uri)

    return g.serialize(format='nt').decode('utf-8')
