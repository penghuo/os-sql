# appendpipe (otellogs)

The `appendpipe` command appends the results of a subpipeline to the search results.

## Example 1: Append rows from total count to existing results

```ppl
source=otellogs
| stats sum(severityNumber) as part by flags, severityText
| sort -part
| head 5
| appendpipe [ stats sum(part) as total by flags ]
```

The query returns the following results:

```text
fetched rows / total rows = 7/7
+------+-------+--------------+-------+
| part | flags | severityText | total |
|------+-------+--------------+-------|
| 24   | 0     | FATAL4       | null  |
| 23   | 0     | FATAL3       | null  |
| 22   | 0     | FATAL2       | null  |
| 21   | 0     | FATAL        | null  |
| 20   | 0     | ERROR4       | null  |
| null | 0     | null         | 110   |
| null | 1     | null         | null  |
+------+-------+--------------+-------+
```


## Example 2: Append rows with merged column names

```ppl
source=otellogs
| stats sum(severityNumber) as total by flags, severityText
| sort -total
| head 5
| appendpipe [ stats sum(total) as total by flags ]
```

The query returns the following results:

```text
fetched rows / total rows = 7/7
+-------+-------+--------------+
| total | flags | severityText |
|-------+-------+--------------|
| 24    | 0     | FATAL4       |
| 23    | 0     | FATAL3       |
| 22    | 0     | FATAL2       |
| 21    | 0     | FATAL        |
| 20    | 0     | ERROR4       |
| 110   | 0     | null         |
| null  | 1     | null         |
+-------+-------+--------------+
```

