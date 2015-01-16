import mock
import pytest
from ldsearch import tasks
import logging


logging.basicConfig(level=logging.DEBUG)


@pytest.fixture(autouse=True)
def index(request):
    patcher = mock.patch('ldsearch.tasks.es')
    mock_es = patcher.start()
    request.addfinalizer(patcher.stop)
    return mock_es.index


@pytest.fixture()
def news_item():
    return """
    <http://example.com/item/1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.bbc.co.uk/search/schema/ContentItem> .
    <http://example.com/item/1> <http://www.bbc.co.uk/search/schema/title> "My test page"              .
    <http://example.com/item/1> <http://www.bbc.co.uk/search/schema/url>   "http://example.com/item/1" .
    <http://example.com/irrelevant/thing> <http://example.com/uninteresting/fact> "foobar" .
    """


@pytest.fixture()
def welsh_programme():
    return """
<http://www.bbc.co.uk/programmes/b04d8wyp> <http://ogp.me/ns#description> "Cyfle i chi sgwrsio gydag Aled Hughes am yr hyn sy'n digwydd yng Nghymru a thu hwnt."@cy-gb .
_:Ncfd7ae1472d541afbd14e5c202dff21e <http://www.w3.org/1999/xhtml/vocab#role> <http://www.w3.org/1999/xhtml/vocab#presentation> .
_:N79f1c731aa0f46aa8c626bff28184284 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/BroadcastEvent> .
<http://www.bbc.co.uk/programmes/b03xsp7f> <http://schema.org/url> "http://www.bbc.co.uk/programmes/b03xsp7f"@cy-gb .
<http://www.bbc.co.uk/programmes/b04d8wyp#orb-search-form> <http://www.w3.org/1999/xhtml/vocab#role> <http://www.w3.org/1999/xhtml/vocab#search> .
<http://www.bbc.co.uk/programmes/b04d8wyp.rdf> <http://purl.org/dc/terms/created> "2014-08-05T00:02:35+01:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://www.bbc.co.uk/programmes/b04d8w9l> <http://schema.org/name> "11/08/2014"@cy-gb .
<http://www.bbc.co.uk/programmes/b04d8wyp> <http://ogp.me/ns#url> "http://www.bbc.co.uk/programmes/b04d8wyp"@cy-gb .
<http://www.bbc.co.uk/programmes/b04d8wyp#programme> <http://purl.org/ontology/po/masterbrand> <http://www.bbc.co.uk/radiocymru#service> .
<http://www.bbc.co.uk/programmes/b04d8y8c> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/RadioEpisode> .
<http://www.bbc.co.uk/programmes/b04d8wyp> <http://schema.org/partOfSeries> <http://www.bbc.co.uk/programmes/b03xsp7f> .
<http://www.bbc.co.uk/programmes/b04d8wyp#programme> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/po/Episode> .
<http://www.bbc.co.uk/programmes/b04d8y8c> <http://schema.org/name> "13/08/2014"@cy-gb .
<http://www.bbc.co.uk/programmes/b04d8w9l> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/RadioEpisode> .
<http://www.bbc.co.uk/radiocymru> <http://schema.org/name> "BBC Radio Cymru"@cy-gb .
<http://www.bbc.co.uk/programmes/b04d8wyp> <http://ogp.me/ns#image> "http://ichef.bbci.co.uk/images/ic/368x207/p02d940t.jpg"@cy-gb .
<http://www.bbc.co.uk/programmes/b04d8wyp> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/RadioEpisode> .
<http://www.bbc.co.uk/programmes/b04d8wyp> <http://www.w3.org/ns/md#item> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .
_:N91b57830a8a04473b5b63e92366f9f43 <http://www.w3.org/1999/xhtml/vocab#role> <http://www.w3.org/1999/xhtml/vocab#navigation> .
<http://www.bbc.co.uk/programmes/b04d8wyp> <http://ogp.me/ns#title> "12/08/2014, Rhaglen Dylan Jones - BBC Radio Cymru"@cy-gb .
<http://www.bbc.co.uk/programmes/b04d8wyp#programme> <http://purl.org/ontology/po/pid> "b04d8wyp" .
<http://www.bbc.co.uk/radiocymru> <http://schema.org/url> <http://www.bbc.co.uk/radiocymru> .
<http://www.bbc.co.uk/programmes/b03xsp7f#programme> <http://purl.org/ontology/po/episode> <http://www.bbc.co.uk/programmes/b04d8wyp#programme> .
<http://www.bbc.co.uk/programmes/b04d8wyp> <http://schema.org/name> "12/08/2014"@cy-gb .
<http://www.bbc.co.uk/programmes/b04d8wyp> <http://schema.org/description> "Cyfle i chi sgwrsio gydag Aled Hughes am yr hyn sy'n digwydd yng Nghymru a thu hwnt. Cerddoriaeth, cyfarchion a hysbys. Aled Hughes chats about what is happening in Wales."@cy-gb .
<http://www.bbc.co.uk/programmes/b04d8wyp#orb-banner> <http://www.w3.org/1999/xhtml/vocab#role> <http://www.w3.org/1999/xhtml/vocab#banner> .
<http://www.bbc.co.uk/programmes/b04d8wyp> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.bbc.co.uk/search/schema/ContentItem> .
<http://www.bbc.co.uk/programmes/b04d8wyp> <http://www.bbc.co.uk/search/schema/url> "http://www.bbc.co.uk/programmes/b04d8wyp" .
<http://www.bbc.co.uk/programmes/b04d8wyp> <http://ogp.me/ns#type> "website"@cy-gb .
<http://www.bbc.co.uk/radiocymru> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/BroadcastService> .
<http://www.bbc.co.uk/programmes/b04d8wyp> <http://schema.org/publication> _:N79f1c731aa0f46aa8c626bff28184284 .
<http://www.bbc.co.uk/programmes/b04d8wyp.rdf> <http://www.w3.org/2000/01/rdf-schema#label> "Description of the episode 12/08/2014" .
<http://www.bbc.co.uk/programmes/b04d8wyp> <http://ogp.me/ns#site_name> "BBC"@cy-gb .
<http://www.bbc.co.uk/programmes/b03xsp7f> <http://schema.org/name> "Rhaglen Dylan Jones"@cy-gb .
<http://www.bbc.co.uk/programmes/b04d8wyp#programme> <http://purl.org/ontology/po/medium_synopsis> "Cyfle i chi sgwrsio gydag Aled Hughes am yr hyn sy'n digwydd yng Nghymru a thu hwnt. Cerddoriaeth, cyfarchion a hysbys. Aled Hughes chats about what is happening in Wales." .
<http://www.bbc.co.uk/programmes/b04d8wyp> <http://schema.org/timeRequired> "PT7200S"@cy-gb .
<http://www.bbc.co.uk/programmes/b04d8wyp#programme> <http://purl.org/dc/elements/1.1/title> "12/08/2014" .
_:N8d84e6a4365341bbba13e7b05936202e <http://www.w3.org/1999/xhtml/vocab#role> <http://www.w3.org/1999/xhtml/vocab#presentation> .
<http://www.bbc.co.uk/programmes/b04d8wyp#programme> <http://purl.org/ontology/po/image_pid> "p02d940t" .
<http://www.bbc.co.uk/programmes/b04d8wyp> <http://xmlns.com/foaf/0.1/primaryTopic> <http://www.bbc.co.uk/programmes/b04d8wyp#programme> .
_:N183f7a5102b94a4c9cc86d464075ef91 <http://www.w3.org/1999/xhtml/vocab#role> <http://www.w3.org/1999/xhtml/vocab#complementary> .
_:N99863f9285a24334913da63288da0236 <http://www.w3.org/1999/xhtml/vocab#role> <http://www.w3.org/1999/xhtml/vocab#presentation> .
<http://www.bbc.co.uk/programmes/b04d8wyp.rdf> <http://xmlns.com/foaf/0.1/primaryTopic> <http://www.bbc.co.uk/programmes/b04d8wyp#programme> .
<http://www.bbc.co.uk/programmes/b03xsp7f#programme> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/po/Brand> .
_:Naf810b932ce041c8a87deb76dccc99f2 <http://www.w3.org/1999/xhtml/vocab#role> <http://www.w3.org/1999/xhtml/vocab#navigation> .
<http://www.bbc.co.uk/programmes/b04d8wyp> <http://schema.org/image> "http://ichef.bbci.co.uk/images/ic/544x306/p02d940t.jpg"@cy-gb .
<http://www.bbc.co.uk/programmes/b04d8wyp#programme> <http://purl.org/ontology/po/version> <http://www.bbc.co.uk/programmes/b04d8wtj#programme> .
<http://www.bbc.co.uk/programmes/b03xsp7f> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Series> .
_:N79f1c731aa0f46aa8c626bff28184284 <http://schema.org/startDate> "2014-08-12T08:00:00+01:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<http://www.bbc.co.uk/programmes/b04d8wyp.rdf> <http://purl.org/dc/terms/modified> "2014-08-05T00:02:35+01:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
_:N79f1c731aa0f46aa8c626bff28184284 <http://schema.org/publishedOn> <http://www.bbc.co.uk/radiocymru> .
<http://www.bbc.co.uk/programmes/b04d8wyp#programme> <http://purl.org/ontology/po/genre> <http://www.bbc.co.uk/programmes/genres/news#genre> .
<http://www.bbc.co.uk/programmes/b04d8y8c> <http://schema.org/url> <http://www.bbc.co.uk/programmes/programmes/b04d8y8c> .
<http://www.bbc.co.uk/programmes/b04d8w9l> <http://schema.org/url> <http://www.bbc.co.uk/programmes/programmes/b04d8w9l> .
_:Nef427162d1ab4722be842803aa6fd3b6 <http://www.w3.org/1999/xhtml/vocab#role> <http://www.w3.org/1999/xhtml/vocab#contentinfo> .
<http://www.bbc.co.uk/programmes/b04d8wyp> <http://www.w3.org/ns/rdfa#usesVocabulary> <http://schema.org/> .
<http://www.bbc.co.uk/programmes/b04d8wyp#programme> <http://purl.org/ontology/po/short_synopsis> "Cyfle i chi sgwrsio gydag Aled Hughes am yr hyn sy'n digwydd yng Nghymru a thu hwnt." .
<http://www.bbc.co.uk/programmes/b04d8wyp#programme> <http://purl.org/ontology/po/genre> <http://www.bbc.co.uk/programmes/genres/factual#genre> .
    """

def test_news(news_item, index):

    tasks.ingest(news_item)

    expected_doc = {
        '@id': 'http://example.com/item/1',
        '@type': ['http://www.bbc.co.uk/search/schema/ContentItem'],
        'http://www.bbc.co.uk/search/schema/url': [
            {'@value': 'http://example.com/item/1'}
        ],
        'http://www.bbc.co.uk/search/schema/title': [
            {'@value': 'My test page'}
        ],
    }

    index.assert_called_once_with(
        id='http://example.com/item/1', body=expected_doc,
        doc_type='item', index='bbc')


def test_news(welsh_programme, index):

    tasks.ingest(welsh_programme)

    expected_doc = {
        '@id': 'http://example.com/item/1',
        '@type': ['http://www.bbc.co.uk/search/schema/ContentItem'],
        'http://www.bbc.co.uk/search/schema/url': [
            {'@value': 'http://example.com/item/1'}
        ],
        'http://www.bbc.co.uk/search/schema/title': [
            {'@value': 'My test page'}
        ],
    }

    index.assert_called_once_with(
        id='http://example.com/item/1', body=expected_doc,
        doc_type='item', index='bbc')
