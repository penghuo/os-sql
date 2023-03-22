FROM opensearchproject/opensearch-dashboards:2.5.0

# disable security dashboards plugin
RUN /usr/share/opensearch-dashboards/bin/opensearch-dashboards-plugin remove securityDashboards
COPY --chown=opensearch-dashboards:opensearch-dashboards opensearch_dashboards.yml /usr/share/opensearch-dashboards/config/

# replace observability dashboards plugin
RUN /usr/share/opensearch-dashboards/bin/opensearch-dashboards-plugin remove observabilityDashboards
COPY observabilityDashboards-2.5.0.zip /usr/share/.
RUN /usr/share/opensearch-dashboards/bin/opensearch-dashboards-plugin install file:///usr/share/observabilityDashboards-2.5.0.zip

