# appendcol (otellogs)

The `appendcol` command appends the result of a subsearch as additional columns to the input search results.

## Example 1: Append count aggregation to existing results

```ppl
source=otellogs
| stats sum(severityNumber) by flags, severityText
| appendcol [ stats count(severityNumber) by flags ]
| head 10
```

The query returns the following results:

```text
fetched rows / total rows = 10/10
+-------+--------------+---------------------+----------------------+
| flags | severityText | sum(severityNumber) | count(severityNumber) |
|-------+--------------+---------------------+----------------------|
| 0     | DEBUG        | 5                   | 30                   |
| 0     | DEBUG2       | 6                   | 2                    |
| 0     | DEBUG3       | 7                   | null                 |
| 0     | DEBUG4       | 8                   | null                 |
| 0     | ERROR        | 17                  | null                 |
| 0     | ERROR2       | 18                  | null                 |
| 0     | ERROR3       | 19                  | null                 |
| 0     | ERROR4       | 20                  | null                 |
| 0     | FATAL        | 21                  | null                 |
| 0     | FATAL2       | 22                  | null                 |
+-------+--------------+---------------------+----------------------+
```


## Example 2: Append count with override

```ppl
source=otellogs
| stats sum(severityNumber) by flags, severityText
| appendcol override=true [ stats count(severityNumber) by flags ]
| head 10
```

The query returns the following results:

```text
fetched rows / total rows = 10/10
+-------+--------------+---------------------+----------------------+
| flags | severityText | sum(severityNumber) | count(severityNumber) |
|-------+--------------+---------------------+----------------------|
| 0     | DEBUG        | 5                   | 30                   |
| 1     | DEBUG2       | 6                   | 2                    |
| 0     | DEBUG3       | 7                   | null                 |
| 0     | DEBUG4       | 8                   | null                 |
| 0     | ERROR        | 17                  | null                 |
| 0     | ERROR2       | 18                  | null                 |
| 0     | ERROR3       | 19                  | null                 |
| 0     | ERROR4       | 20                  | null                 |
| 0     | FATAL        | 21                  | null                 |
| 0     | FATAL2       | 22                  | null                 |
+-------+--------------+---------------------+----------------------+
```


## Example 3: Append multiple subsearch results

```ppl
source=otellogs
| sort @timestamp
| fields severityText, severityNumber, flags
| head 5
| appendcol [ stats avg(severityNumber) as avg_sev ]
| appendcol [ stats max(severityNumber) as max_sev ]
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+--------------+----------------+-------+--------------------+---------+
| severityText | severityNumber | flags | avg_sev            | max_sev |
|--------------+----------------+-------+--------------------+---------|
| INFO         | 9              | 1     | 12.5               | 24      |
| ERROR        | 17             | 1     | null               | null    |
| WARN         | 13             | 0     | null               | null    |
| DEBUG        | 5              | 0     | null               | null    |
| INFO         | 9              | 0     | null               | null    |
+--------------+----------------+-------+--------------------+---------+
```

