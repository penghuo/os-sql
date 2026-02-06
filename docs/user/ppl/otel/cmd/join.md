# join (otellogs)

The `join` command combines two datasets. This example demonstrates self-joins on the otellogs index.

## Example 1: Inner join with subsearch

The following query joins otellogs with a filtered subsearch of itself:

```ppl
source=otellogs
| where severityText = 'ERROR'
| inner join left=a right=b ON a.severityNumber = b.severityNumber
  [ source=otellogs | where severityText = 'ERROR' | fields severityNumber, body ]
| fields a.severityNumber, a.severityText, b.body
| head 2
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+------------------+----------------+---------------------------------------------------------+
| a.severityNumber | a.severityText | b.body                                                  |
|------------------+----------------+---------------------------------------------------------|
| 17               | ERROR          | Payment failed: Insufficient funds for user@example.com |
| 17               | ERROR          | Database connection timeout after 30000ms               |
+------------------+----------------+---------------------------------------------------------+
```


## Example 2: Left join with subsearch

The following query performs a left join to find logs with and without trace IDs:

```ppl
source=otellogs
| where severityNumber < 10
| sort @timestamp
| head 4
| left join left=a right=b ON a.severityNumber = b.severityNumber
  [ source=otellogs | where isnotnull(traceId) AND traceId != '' | fields severityNumber, traceId ]
| fields a.severityNumber, a.severityText, b.traceId
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+------------------+----------------+----------------------------------+
| a.severityNumber | a.severityText | b.traceId                        |
|------------------+----------------+----------------------------------|
| 9                | INFO           | b3cb01a03c846973fd496b973f49be85 |
| 5                | DEBUG          | null                             |
| 9                | INFO           | b3cb01a03c846973fd496b973f49be85 |
| 1                | TRACE          | null                             |
+------------------+----------------+----------------------------------+
```


## Example 3: Join using field list

The following query uses the extended syntax with a field list for join criteria:

```ppl
source=otellogs
| where severityText IN ('INFO', 'ERROR')
| sort @timestamp
| head 3
| join severityNumber
  [ source=otellogs | where flags = 1 | fields severityNumber, flags ]
| fields severityNumber, severityText, flags
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------+--------------+-------+
| severityNumber | severityText | flags |
|----------------+--------------+-------|
| 9              | INFO         | 1     |
| 17             | ERROR        | 1     |
| 9              | INFO         | 1     |
+----------------+--------------+-------+
```


## Example 4: Self-join to compare severity levels

The following query performs a self-join to pair INFO and ERROR logs by matching timestamps within the same second:

```ppl
source=otellogs
| where severityText = 'INFO'
| sort @timestamp
| head 2
| inner join left=info right=err ON info.flags = err.flags
  [ source=otellogs | where severityText = 'ERROR' | fields severityNumber, severityText, flags ]
| fields info.severityNumber, info.severityText, err.severityNumber, err.severityText
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+--------------------+------------------+-------------------+-----------------+
| info.severityNumber | info.severityText | err.severityNumber | err.severityText |
|--------------------+------------------+-------------------+-----------------|
| 9                  | INFO             | 17                | ERROR           |
| 9                  | INFO             | 17                | ERROR           |
+--------------------+------------------+-------------------+-----------------+
```
