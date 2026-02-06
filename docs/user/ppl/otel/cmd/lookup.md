# lookup (otellogs)

The `lookup` command enriches search data by adding or replacing values from a lookup index. This example demonstrates using otellogs as both source and lookup index.

## Example 1: Lookup with replace strategy

The following query uses otellogs as a lookup table to enrich data based on severityNumber:

```ppl
source=otellogs
| where severityText = 'INFO'
| sort @timestamp
| head 3
| lookup otellogs severityNumber REPLACE body
| fields severityNumber, severityText, body
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------+--------------+----------------------------------------------------------------------------------+
| severityNumber | severityText | body                                                                             |
|----------------+--------------+----------------------------------------------------------------------------------|
| 9              | INFO         | User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart |
| 9              | INFO         | User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart |
| 9              | INFO         | User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart |
+----------------+--------------+----------------------------------------------------------------------------------+
```


## Example 2: Lookup with field aliasing

The following query looks up data and places matched values into a new field:

```ppl
source=otellogs
| where severityText = 'ERROR'
| sort @timestamp
| head 2
| lookup otellogs severityNumber REPLACE traceId AS lookup_trace
| fields severityNumber, severityText, traceId, lookup_trace
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+----------------+--------------+----------------------------------+----------------------------------+
| severityNumber | severityText | traceId                          | lookup_trace                     |
|----------------+--------------+----------------------------------+----------------------------------|
| 17             | ERROR        | 7475a30207dbef54d29e42c37f09a528 | 7475a30207dbef54d29e42c37f09a528 |
| 17             | ERROR        | 60b30ecb7edcaa4ca3c8c8b8c6ffefbc | 7475a30207dbef54d29e42c37f09a528 |
+----------------+--------------+----------------------------------+----------------------------------+
```
