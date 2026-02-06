# eventstats (otellogs)

The `eventstats` command enriches event data with calculated summary statistics appended as new fields.

## Example 1: Calculate average, sum, and count by group

```ppl
source=otellogs
| sort @timestamp
| fields severityNumber, severityText, flags
| eventstats avg(severityNumber), sum(severityNumber), count() by flags
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------+-------+---------------------+---------------------+---------+
| severityNumber | severityText | flags | avg(severityNumber) | sum(severityNumber) | count() |
|----------------+--------------+-------+---------------------+---------------------+---------|
| 9              | INFO         | 1     | 13.0                | 26                  | 2       |
| 17             | ERROR        | 1     | 13.0                | 26                  | 2       |
| 13             | WARN         | 0     | 12.466666666666667  | 374                 | 30      |
| 5              | DEBUG        | 0     | 12.466666666666667  | 374                 | 30      |
+----------------+--------------+-------+---------------------+---------------------+---------+
```


## Example 2: Calculate count by severity span

```ppl
source=otellogs
| sort @timestamp
| fields severityNumber, severityText
| eventstats count() as cnt by span(severityNumber, 5) as severity_span
| head 6
```

The query returns the following results:

```text
fetched rows / total rows = 6/6
+----------------+--------------+-----+
| severityNumber | severityText | cnt |
|----------------+--------------+-----|
| 9              | INFO         | 8   |
| 17             | ERROR        | 8   |
| 13             | WARN         | 8   |
| 5              | DEBUG        | 8   |
| 9              | INFO         | 8   |
| 21             | FATAL        | 4   |
+----------------+--------------+-----+
```


## Example 3: Null bucket handling with bucket_nullable=false

```ppl
source=otellogs
| eventstats bucket_nullable=false count() as cnt by `attributes.user.email`
| sort @timestamp
| fields severityText, `attributes.user.email`, cnt
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+-----------------------+------+
| severityText | attributes.user.email | cnt  |
|--------------+-----------------------+------|
| INFO         | null                  | null |
| ERROR        | user@example.com      | 1    |
| WARN         | null                  | null |
| DEBUG        | null                  | null |
+--------------+-----------------------+------+
```


## Example 4: Null bucket handling with bucket_nullable=true

```ppl
source=otellogs
| eventstats bucket_nullable=true count() as cnt by `attributes.user.email`
| sort @timestamp
| fields severityText, `attributes.user.email`, cnt
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+-----------------------+-----+
| severityText | attributes.user.email | cnt |
|--------------+-----------------------+-----|
| INFO         | null                  | 25  |
| ERROR        | user@example.com      | 1   |
| WARN         | null                  | 25  |
| DEBUG        | null                  | 25  |
+--------------+-----------------------+-----+
```
