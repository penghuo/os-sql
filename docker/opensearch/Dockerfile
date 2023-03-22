FROM opensearchproject/opensearch:2.5.0

RUN rm -rf /usr/share/opensearch/plugins/opensearch-sql
COPY opensearch-sql-2.5.0.0 /usr/share/opensearch/plugins/opensearch-sql
COPY datasources.json /usr/share/opensearch/

RUN /usr/share/opensearch/bin/opensearch-keystore add-file plugins.query.federation.datasources.config /usr/share/opensearch/datasources.json --force
