# append (otellogs)

The `append` command appends the results of a subsearch as additional rows to the end of the input search results.

## Example 1: Append rows from a count aggregation

The following query appends count by severityText to sum by severityText and flags:

```ppl
source=otellogs
| stats sum(severityNumber) by severityText, flags
| sort - `sum(severityNumber)`
| head 3
| append [ source=otellogs | stats count(severityNumber) by severityText | sort - `count(severityNumber)` | head 3 ]
```

The query returns the following results:

```text
fetched rows / total rows = 6/6
+---------------------+--------------+-------+-----------------------+
| sum(severityNumber) | severityText | flags | count(severityNumber) |
|---------------------+--------------+-------+-----------------------|
| 24                  | FATAL4       | 0     | null                  |
| 23                  | FATAL3       | 0     | null                  |
| 22                  | FATAL2       | 0     | null                  |
| null                | INFO         | null  | 7                     |
| null                | ERROR        | null  | 2                     |
| null                | WARN         | null  | 2                     |
+---------------------+--------------+-------+-----------------------+
```


## Example 2: Append rows with merged column names

The following query appends rows from sum by severityText to sum by severityText and flags, merging columns with the same field name:

```ppl
source=otellogs
| stats sum(severityNumber) as total by severityText, flags
| sort - total
| head 3
| append [ source=otellogs | stats sum(severityNumber) as total by severityText | sort - total | head 3 ]
```

The query returns the following results:

```text
fetched rows / total rows = 6/6
+-------+--------------+-------+
| total | severityText | flags |
|-------+--------------+-------|
| 24    | FATAL4       | 0     |
| 23    | FATAL3       | 0     |
| 22    | FATAL2       | 0     |
| 63    | INFO         | null  |
| 34    | ERROR        | null  |
| 26    | WARN         | null  |
+-------+--------------+-------+
```


## Example 3: Append different aggregations

The following query appends average severity numbers to maximum severity numbers:

```ppl
source=otellogs
| stats max(severityNumber) as severity by flags
| append [ source=otellogs | stats avg(severityNumber) as severity by flags ]
| sort flags, severity
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------------+-------+
| severity           | flags |
|--------------------+-------|
| 12.466666666666667 | 0     |
| 24                 | 0     |
| 13.0               | 1     |
| 17                 | 1     |
+--------------------+-------+
```
