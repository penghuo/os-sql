# fields (otellogs)

The `fields` command specifies the fields that should be included in or excluded from the search results.

## Example 1: Select specified fields from the search result

The following query shows how to retrieve the `severityNumber`, `severityText`, and `body` fields from the search results:

```ppl
source=otellogs
| sort @timestamp
| fields severityNumber, severityText, body
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------+----------------------------------------------------------------------------------+
| severityNumber | severityText | body                                                                             |
|----------------+--------------+----------------------------------------------------------------------------------|
| 9              | INFO         | User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart |
| 17             | ERROR        | Payment failed: Insufficient funds for user@example.com                          |
| 13             | WARN         | Query contains Lucene special characters: +field:value -excluded AND (grouped OR terms) NOT "exact phrase" wildcard* fuzzy~2 /regex/ [range TO search] |
| 5              | DEBUG        | 192.168.1.1 - - [15/Jan/2024:10:30:03 +0000] "GET /api/products?search=laptop&category=electronics HTTP/1.1" 200 1234 "-" "Mozilla/5.0" |
+----------------+--------------+----------------------------------------------------------------------------------+
```


## Example 2: Remove specified fields from the search results

The following query shows how to remove the `severityNumber` field from the search results:

```ppl
source=otellogs
| sort @timestamp
| fields severityNumber, severityText, traceId
| fields - severityNumber
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+----------------------------------+
| severityText | traceId                          |
|--------------+----------------------------------|
| INFO         | b3cb01a03c846973fd496b973f49be85 |
| ERROR        | 7475a30207dbef54d29e42c37f09a528 |
| WARN         |                                  |
| DEBUG        |                                  |
+--------------+----------------------------------+
```


## Example 3: Space-delimited field selection

Fields can be specified using spaces instead of commas, providing a more concise syntax:

```ppl
source=otellogs
| sort @timestamp
| fields severityText severityNumber flags
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+----------------+-------+
| severityText | severityNumber | flags |
|--------------+----------------+-------|
| INFO         | 9              | 1     |
| ERROR        | 17             | 1     |
| WARN         | 13             | 0     |
| DEBUG        | 5              | 0     |
+--------------+----------------+-------+
```


## Example 4: Prefix wildcard pattern

The following query selects fields starting with a pattern using prefix wildcards:

```ppl
source=otellogs
| sort @timestamp
| fields severity*
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 9              | INFO         |
| 17             | ERROR        |
| 13             | WARN         |
| 5              | DEBUG        |
+----------------+--------------+
```


## Example 5: Suffix wildcard pattern

The following query selects fields ending with a pattern using suffix wildcards:

```ppl
source=otellogs
| sort @timestamp
| fields *Id
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------------------------+------------------+
| traceId                          | spanId           |
|----------------------------------+------------------|
| b3cb01a03c846973fd496b973f49be85 | caf311ef949971cb |
| 7475a30207dbef54d29e42c37f09a528 | 7a35f3b69a2f9a24 |
|                                  |                  |
|                                  |                  |
+----------------------------------+------------------+
```


## Example 6: Wildcard pattern matching

The following query selects fields containing a pattern using `contains` wildcards:

```ppl
source=otellogs
| sort @timestamp
| fields *e*
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+----------------+--------------+----------------------------------+----------------------------------------------------------------------------------+
| severityNumber | severityText | traceId                          | time                                                                             |
|----------------+--------------+----------------------------------+----------------------------------------------------------------------------------|
| 9              | INFO         | b3cb01a03c846973fd496b973f49be85 | 2024-01-15 10:30:00.123456789                                                    |
+----------------+--------------+----------------------------------+----------------------------------------------------------------------------------+
```


## Example 7: Mixed delimiter syntax

The following query combines spaces and commas for flexible field specification:

```ppl
source=otellogs
| sort @timestamp
| fields severityText, severity* *Id
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+----------------+----------------------------------+------------------+
| severityText | severityNumber | traceId                          | spanId           |
|--------------+----------------+----------------------------------+------------------|
| INFO         | 9              | b3cb01a03c846973fd496b973f49be85 | caf311ef949971cb |
| ERROR        | 17             | 7475a30207dbef54d29e42c37f09a528 | 7a35f3b69a2f9a24 |
| WARN         | 13             |                                  |                  |
| DEBUG        | 5              |                                  |                  |
+--------------+----------------+----------------------------------+------------------+
```


## Example 8: Field deduplication

The following query automatically prevents duplicate columns when wildcards expand to already specified fields:

```ppl
source=otellogs
| sort @timestamp
| fields severityText, severity*
| head 4
```

The query returns the following results. Even though `severityText` is explicitly specified and also matches `severity*`, it appears only once because of automatic deduplication:

```text
fetched rows / total rows = 4/4
+--------------+----------------+
| severityText | severityNumber |
|--------------+----------------|
| INFO         | 9              |
| ERROR        | 17             |
| WARN         | 13             |
| DEBUG        | 5              |
+--------------+----------------+
```

## Example 9: Full wildcard selection

The following query selects all available fields using `*` or `` `*` ``:

```ppl
source=otellogs
| sort @timestamp
| fields severityNumber, severityText, traceId
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+----------------+--------------+----------------------------------+
| severityNumber | severityText | traceId                          |
|----------------+--------------+----------------------------------|
| 9              | INFO         | b3cb01a03c846973fd496b973f49be85 |
+----------------+--------------+----------------------------------+
```

## Example 10: Wildcard exclusion

The following query removes fields using wildcard patterns containing the minus (`-`) operator:

```ppl
source=otellogs
| sort @timestamp
| fields severityNumber, severityText, traceId, spanId
| fields - *Id
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 9              | INFO         |
| 17             | ERROR        |
| 13             | WARN         |
| 5              | DEBUG        |
+----------------+--------------+
```
