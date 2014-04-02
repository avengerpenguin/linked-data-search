from elasticsearch import Elasticsearch
import feedparser


def index_item(item, uri):
    es = Elasticsearch()
    print u'Pushing: {0}'.format(item['http://schema.org/headline'])
    es.index(index='search', id=uri, doc_type='item', body=item)


def convert_entry_to_item(entry):
    item = {
        'http://schema.org/headline': entry.title,
        'http://schema.org/url': entry.link,
        'http://schema.org/description': entry.summary
    }
    # Use the URL as the key
    uri = entry.link
    return uri, item


def index_feed(feed_url):
    news = feedparser.parse(feed_url)
    for entry in news.entries:
        item, uri = convert_entry_to_item(entry)
        index_item(uri, item)


def index_news():
    index_feed('http://feeds.bbci.co.uk/news/rss.xml')

def index_sport():
    index_feed('http://feeds.bbci.co.uk/sport/0/rss.xml?edition=uk')
