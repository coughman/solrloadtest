SolrLoadTest
============

Utility for updating solr with JSON data.  Used for load testing Solr.  Solr Cloud ready.  Multi-threaded capable.

Prerequisities
--------------
- JSON data file, one record per line
- valid solr schema.xml for JSON data

To build
--------
```mvn package```

To run
------
```java -cp SolrLoadTest.jar com.cloudera.sa.solr.SolrUpdater -c <collection> -f <JSON-data-file> -n <number-of-docs> -s <solr-schema-path> -t <num-threads> -zk <zookeeper-host>```
