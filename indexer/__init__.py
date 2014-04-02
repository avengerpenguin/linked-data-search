from elasticsearch import Elasticsearch
import feedparser
from rdflib import Graph, URIRef

def index_item(item, uri):

    if uri.startswith('http://'):
        for p, o in read_page_for_metadata(uri):
            item[p] = o

    es = Elasticsearch()
    print u'Pushing: {0}'.format(item['http://schema.org/headline'])
    es.index(index='search', id=uri, doc_type='item', body=item)


def read_page_for_metadata(url):
    g = Graph()
    g.parse(url, format="rdfa1.1")
    return g.predicate_objects(URIRef(url))


def convert_entry_to_item(entry):
    item = {
        'http://schema.org/headline': entry.title,
        'http://schema.org/url': entry.link,
        'http://schema.org/description': entry.summary
    }

    uri = unicode(entry.link).rsplit('#')[0].strip()
    return uri, item


def index_feed(feed_url):
    news = feedparser.parse(feed_url)
    for entry in news.entries:
        item, uri = convert_entry_to_item(entry)
        uri = unicode(entry.link).rsplit('#')[0].strip()
        index_item(uri, item)


def index_news():
    index_feed('http://feeds.bbci.co.uk/news/rss.xml')

def index_sport():
    index_feed('http://feeds.bbci.co.uk/sport/0/rss.xml?edition=uk')
