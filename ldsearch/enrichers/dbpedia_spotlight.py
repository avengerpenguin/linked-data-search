import spotlight
from rdflib import URIRef, RDF

def enrich(graph):
    for uri in graph.subjects(predicate=RDF.type, object=URIRef('http://www.bbc.co.uk/search/schema/ContentItem')):
        for desc in graph.objects(predicate=URIRef('http://schema.org/description')):
            try:
                annotations = spotlight.annotate('http://spotlight.dbpedia.org/rest/annotate', str(desc), confidence=0.4, support=20)
                for tag in annotations:
                    graph.add((URIRef(uri), URIRef('http://www.bbc.co.uk/search/schema/tag'), URIRef(tag['URI'])))
            except spotlight.SpotlightException:
                continue
