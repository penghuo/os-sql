## Summary
Test Calcite join with OpenSearch 2.17, Understand the latency and limitation.

## Prepare Dataset
* load http_logs
```
opensearch-benchmark execute-test \
  --workload http_logs \
  --pipeline benchmark-only \
  --target-hosts http://localhost:9200
```