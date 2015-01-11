from rdflib import URIRef, RDF
import re


def enrich(graph):
    for uri in set(list(graph.subjects()) + list(graph.objects())):
        if re.match('^http://www.bbc.co.uk/programmes/[a-z0-9]+$', uri):
            graph.parse(str(uri) + '.rdf')
            graph.add((uri, URIRef('http://xmlns.com/foaf/0.1/primaryTopic'), uri + '#programme'))



