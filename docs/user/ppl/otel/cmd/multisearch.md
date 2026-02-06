# multisearch (otellogs)

The `multisearch` command runs multiple subsearches and merges their results.

## Example 1: Combining severity groups

```ppl
| multisearch [search source=otellogs
| where severityNumber < 10
| eval severity_group = "low"
| fields severityText, severityNumber, severity_group] [search source=otellogs
| where severityNumber >= 10
| eval severity_group = "high"
| fields severityText, severityNumber, severity_group]
| stats count() by severity_group
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+----------------+---------+
| severity_group | count() |
|----------------+---------|
| high           | 16      |
| low            | 16      |
+----------------+---------+
```


## Example 2: Segmenting by flags

```ppl
| multisearch [search source=otellogs
| where flags = 1
| eval query_type = "flagged"
| fields severityText, severityNumber, flags, query_type] [search source=otellogs
| where flags = 0
| eval query_type = "unflagged"
| fields severityText, severityNumber, flags, query_type]
| sort severityNumber
| head 6
```

The query returns the following results:

```text
fetched rows / total rows = 6/6
+--------------+----------------+-------+-----------+
| severityText | severityNumber | flags | query_type |
|--------------+----------------+-------+-----------|
| TRACE        | 1              | 0     | unflagged |
| TRACE2       | 2              | 0     | unflagged |
| TRACE3       | 3              | 0     | unflagged |
| TRACE4       | 4              | 0     | unflagged |
| DEBUG        | 5              | 0     | unflagged |
| DEBUG2       | 6              | 0     | unflagged |
+--------------+----------------+-------+-----------+
```


## Example 3: Handling missing fields across subsearches

```ppl
| multisearch [search source=otellogs
| where severityNumber > 20
| eval critical_flag = "yes"
| fields severityText, severityNumber, critical_flag] [search source=otellogs
| where severityNumber <= 20 AND severityNumber > 15
| fields severityText, severityNumber]
| sort severityNumber desc
| head 6
```

The query returns the following results:

```text
fetched rows / total rows = 6/6
+--------------+----------------+---------------+
| severityText | severityNumber | critical_flag |
|--------------+----------------+---------------|
| FATAL4       | 24             | yes           |
| FATAL3       | 23             | yes           |
| FATAL2       | 22             | yes           |
| FATAL        | 21             | yes           |
| ERROR4       | 20             | null          |
| ERROR3       | 19             | null          |
+--------------+----------------+---------------+
```


## Example 4: Merging with different aggregations

```ppl
| multisearch [search source=otellogs
| where flags = 1
| stats count() as cnt, avg(severityNumber) as avg_sev
| eval source = "flagged"] [search source=otellogs
| where flags = 0
| stats count() as cnt, avg(severityNumber) as avg_sev
| eval source = "unflagged"]
| fields source, cnt, avg_sev
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+-----------+-----+--------------------+
| source    | cnt | avg_sev            |
|-----------+-----+--------------------|
| flagged   | 2   | 13.0               |
| unflagged | 30  | 12.466666666666667 |
+-----------+-----+--------------------+
```

