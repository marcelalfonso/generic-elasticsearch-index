# Generic Elasticsearch index

A proposal for building a generic phonetic and exact search elasticsearch index with Nest .net client. Some tests are performed against the index.

## Build and run

The script is .net 6 console application, so .net 6 framework is required to build and run the code. Also a network-accessible elasticsearch server is required. The code was tested with the official elasticsearch:8.3.3 docker image running in a local environment. For the phonetic searches the elasticsearch plugin analysis-phonetic was used.
