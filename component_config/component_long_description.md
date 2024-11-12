# Elasticsearch Extractor

Elasticsearch is a search engine based on the Lucene library. It provides a distributed, multitenant-capable full-text search engine with an HTTP web interface and schema-free JSON documents.

The component allows to download data from indexes in an Elasticseach engine directly to Keboola without complicated setup.


# Notes on functionality

The extractor utilizes Elasticsearch [Search API](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html) to download the data from an index. Users are able to define their own request by specifying a JSON request body, which will be appended to a request. For all allowed request body specifications, please refer to [Request Body in Search API](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html#search-search-api-request-body) documentation.