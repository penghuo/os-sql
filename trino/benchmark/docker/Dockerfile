# Dockerfile for OpenSearch 3.6.0-SNAPSHOT with SQL/Trino plugin
# Builds from the local distro tarball since there's no Docker image for SNAPSHOT versions.
FROM ubuntu:22.04

# Install JDK 21 and basic tools
RUN apt-get update && apt-get install -y openjdk-21-jdk curl && rm -rf /var/lib/apt/lists/*

# Copy OpenSearch distro from local build
COPY opensearch-distro/ /opt/opensearch/

# Copy and install the SQL plugin
COPY opensearch-sql-plugin/ /opt/opensearch/plugins/opensearch-sql/

# Create opensearch user
RUN useradd -m opensearch && chown -R opensearch:opensearch /opt/opensearch

# Set environment
ENV OPENSEARCH_HOME=/opt/opensearch
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
WORKDIR /opt/opensearch

USER opensearch
EXPOSE 9200 9300

ENTRYPOINT ["/opt/opensearch/bin/opensearch"]
