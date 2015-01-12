from rdflib import URIRef, RDF
import re


def enrich(graph):
    for uri in graph.subjects(predicate=RDF.type, object=URIRef('http://www.bbc.co.uk/search/schema/ContentItem')):
        if re.match('^http://www.bbc.co.uk/programmes/[a-z0-9]+$', uri):
            graph.parse(str(uri) + '.rdf')
            graph.add((uri, URIRef('http://xmlns.com/foaf/0.1/primaryTopic'), uri + '#programme'))
