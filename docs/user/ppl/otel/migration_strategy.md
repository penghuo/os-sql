# OpenTelemetry Logs Migration Strategy

This document outlines the strategy for migrating PPL doctest cases from the `accounts` and `people` indexes to the `otellogs` OpenTelemetry logs index.

## Field Mapping Reference

### Top-Level otellogs Fields

| Field | Type | Sample Values | Use For |
|-------|------|---------------|---------|
| `@timestamp` | date_nanos | 2024-01-15T10:30:00.123456789Z | Time-based queries |
| `severityNumber` | long | 1-24 | Numeric comparisons, ranges |
| `severityText` | keyword | INFO, ERROR, WARN, DEBUG, FATAL, TRACE, etc. | String matching, filtering |
| `body` | text | Log message content | Full-text search |
| `traceId` | keyword | b3cb01a03c846973fd496b973f49be85 | String operations |
| `spanId` | keyword | caf311ef949971cb | String operations |
| `flags` | long | 0, 1 | Binary/boolean-like values |

### Nested Fields

| Field | Type | Sample Values | Use For |
|-------|------|---------------|---------|
| `instrumentationScope.name` | keyword | cart-service, payment-service | Service filtering |
| `instrumentationScope.version` | keyword | 1.0.0 | Version filtering |
| `resource.attributes.service.name` | keyword | cart-service, api-gateway | Service grouping |
| `resource.attributes.service.namespace` | keyword | production | Namespace filtering |
| `resource.attributes.host.name` | keyword | api-server-01, prod-server-01 | Host filtering |

### Attributes Fields

| Field | Type | Sample Values | Use For |
|-------|------|---------------|---------|
| `attributes.user.email` | keyword | user@example.com | Email-related tests |
| `attributes.user.id` | keyword | e1ce63e6-8501-11f0-930d-c2fcbdc05f14 | ID filtering |
| `attributes.error.code` | keyword | INSUFFICIENT_FUNDS | Error filtering |
| `attributes.http.method` | keyword | GET, POST | HTTP method filtering |
| `attributes.http.status_code` | long | 200 | Numeric comparisons |
| `attributes.payment.amount` | double | 1500.00 | Decimal comparisons |
| `attributes.quantity` | long | 4 | Numeric operations |
| `attributes.kafka.topic` | keyword | user-events | String filtering |
| `attributes.kafka.offset` | long | 12345 | Numeric operations |
| `attributes.redis.ttl` | long | 3600 | Numeric operations |
| `attributes.client.ip` | ip | 192.168.1.1 | IP address operations |

## Migration Strategy

### Field Mapping from accounts to otellogs

| accounts Field | otellogs Equivalent | Notes |
|---------------|---------------------|-------|
| `account_number` | `severityNumber` | Both are long/integer types |
| `firstname` | `severityText` | Keyword field for string matching |
| `lastname` | `severityText` | Alternative string field |
| `employer` | `resource.attributes.service.name` | Keyword field with null values |
| `age` | `severityNumber` | Numeric field for ranges |
| `balance` | `attributes.payment.amount` | Numeric field (note: double) |
| `email` | `attributes.user.email` | Keyword field with null values |
| `gender` | `flags` | Binary categorical field |
| `state` | `resource.attributes.service.namespace` | Keyword field |
| `city` | `instrumentationScope.name` | Keyword field |
| `address` | `body` | Text field for full-text search |

### Field Mapping from people to otellogs

For function tests that use the `people` index with single-row results, map to `otellogs` and use `| head 1` to get single-row output.

## Test Data Summary (32 records)

### Severity Distribution
- INFO: 7 records (severityNumber 9, 10, 11, 12)
- ERROR: 4 records (severityNumber 17, 18, 19, 20)
- WARN: 4 records (severityNumber 13, 14, 15, 16)
- DEBUG: 4 records (severityNumber 5, 6, 7, 8)
- TRACE: 4 records (severityNumber 1, 2, 3, 4)
- FATAL: 4 records (severityNumber 21, 22, 23, 24)

### Service Distribution
- cart-service, payment-service, search-service, nginx, notification-service
- api-gateway, user-service, parser-service, security-scanner, auth-service
- cache-service, event-producer, graphql-service, email-service, webhook-service
- cert-monitor, grpc-service, queue-processor, validation-service, batch-processor
- monitoring-service, k8s-monitor, data-integrity-service, health-checker
- analytics-service, transaction-service

## Migration Rules

1. **Migrate ALL test cases** from original files, not just ones that are easy to migrate.

2. **Preserve test semantics**: Use equivalent otellogs fields that maintain the same query behavior.

3. **Use consistent ordering**: Add `| sort @timestamp` when deterministic output order is required.

4. **Limit results appropriately**: Use `| head N` to match expected row counts.

5. **Escape nested field names**: Use backticks for nested fields like `` `resource.attributes.service.name` ``.

6. **Handle null values**: The otellogs data has nulls/missing values in some nested attributes, similar to accounts data.

7. **Test expected results**: Run queries against actual data to verify expected output matches.

## Migrated Commands and Functions

### Commands (in otel/cmd/) - 37 files

| Command | Test Cases | Status |
|---------|------------|--------|
| search.md | 8 | ✅ PASS |
| where.md | 8 | ✅ PASS |
| sort.md | 7 | ✅ PASS |
| stats.md | 16 | ✅ PASS |
| fields.md | 10 | ✅ PASS |
| head.md | 3 | ✅ PASS |
| rename.md | 5 | ✅ PASS |
| eval.md | 5 | ✅ PASS |
| dedup.md | 4 | ✅ PASS |
| top.md | 6 | ✅ PASS |
| rare.md | 5 | ✅ PASS |
| fillnull.md | 6 | ✅ PASS |
| parse.md | 3 | ✅ PASS |
| grok.md | 2 | ✅ PASS |
| join.md | 4 | ✅ PASS |
| lookup.md | 2 | ✅ PASS |
| append.md | 3 | ✅ PASS |
| table.md | 1 | ✅ PASS |
| reverse.md | 3 | ✅ PASS |
| regex.md | 4 | ✅ PASS |
| replace.md | 4 | ✅ PASS |
| rex.md | 4 | ✅ PASS |
| trendline.md | 4 | ✅ PASS |
| eventstats.md | 4 | ✅ PASS |
| bin.md | 6 | ✅ PASS |
| streamstats.md | 6 | ✅ PASS |
| addcoltotals.md | 3 | ✅ PASS |
| addtotals.md | 3 | ✅ PASS |
| appendcol.md | 3 | ✅ PASS |
| appendpipe.md | 2 | ✅ PASS |
| transpose.md | 3 | ✅ PASS |
| timechart.md | 5 | ✅ PASS |
| subquery.md | 4 | ✅ PASS |
| patterns.md | 4 | ✅ PASS |
| multisearch.md | 4 | ✅ PASS |
| chart.md | 5 | ✅ PASS |
| describe.md | 3 | ✅ PASS |
| explain.md | 3 | ✅ PASS |

### Functions (in otel/functions/) - 14 files

| Function | Test Cases | Status |
|----------|------------|--------|
| string.md | 16 | ✅ PASS |
| math.md | 15 | ✅ PASS |
| condition.md | 12 | ✅ PASS |
| datetime.md | 14 | ✅ PASS |
| conversion.md | 13 | ✅ PASS |
| aggregations.md | 11 | ✅ PASS |
| ip.md | 2 | ✅ PASS |
| json.md | 6 | ✅ PASS |
| collection.md | 16 | ✅ PASS |
| cryptographic.md | 4 | ✅ PASS |
| expressions.md | 6 | ✅ PASS |
| relevance.md | 5 | ✅ PASS |
| statistical.md | 4 | ✅ PASS |
| system.md | 2 | ✅ PASS |

## Test Verification Command

Run individual command tests:
```bash
./gradlew doctest -Pdocs=otel/cmd/{command_name} -DignorePrometheus
```

Run individual function tests:
```bash
./gradlew doctest -Pdocs=otel/functions/{function_name} -DignorePrometheus
```

Run all otel tests:
```bash
./gradlew doctest -Pdocs=otel -DignorePrometheus
```

## Test Results Summary

| Category | Files | Test Cases (approx) | Status |
|----------|-------|---------------------|--------|
| Commands | 37 | ~150 | ✅ ALL PASS |
| Functions | 14 | ~130 | ✅ ALL PASS |
| **TOTAL** | **51** | **~280** | **✅ ALL PASS** |

## Bug Tracking

No PPL bugs were discovered during this migration. All tests pass successfully.

## Not Migrated (Special Requirements)

The following files were NOT migrated because they require special data structures or configurations not available in otellogs:

### Commands Not Migrated

| Command | Reason |
|---------|--------|
| ad.md | Deprecated; requires ML Commons plugin with nyc_taxi dataset |
| kmeans.md | Deprecated; requires ML Commons plugin with iris_data dataset |
| ml.md | Requires ML Commons plugin with nyc_taxi/iris_data datasets |
| expand.md | Requires nested array data (migration dataset with address array) |
| flatten.md | Requires nested object data (my-index dataset with message object) |
| mvcombine.md | Requires mvcombine_data dataset with specific multi-value fields |
| spath.md | Requires structured dataset with JSON fields |
| showdatasources.md | Requires Prometheus datasource configured; plugins.calcite.enabled=false |
| syntax.md | Documentation only, all examples marked with `ignore` |
| index.md | Documentation pointer only, no test cases |

### Functions Not Migrated

| Function | Reason |
|----------|--------|
| index.md | Documentation pointer only, no test cases |

## Summary

- **Total files migrated**: 51 (37 commands + 14 functions)
- **Total test cases**: ~280
- **All tests**: PASS
- **PPL bugs found**: 0
- **Files not migrated**: 11 (due to special data/config requirements)
