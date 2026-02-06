# streamstats (otellogs)

The `streamstats` command calculates cumulative or rolling statistics as events are processed in order.

## Example 1: Calculate running average, sum, and count by group

```ppl
source=otellogs
| sort @timestamp
| streamstats avg(severityNumber) as running_avg, sum(severityNumber) as running_sum, count() as running_count by flags
| fields severityNumber, severityText, flags, running_avg, running_sum, running_count
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------+-------+-------------+-------------+---------------+
| severityNumber | severityText | flags | running_avg | running_sum | running_count |
|----------------+--------------+-------+-------------+-------------+---------------|
| 9              | INFO         | 1     | 9.0         | 9           | 1             |
| 17             | ERROR        | 1     | 13.0        | 26          | 2             |
| 13             | WARN         | 0     | 13.0        | 13          | 1             |
| 5              | DEBUG        | 0     | 9.0         | 18          | 2             |
+----------------+--------------+-------+-------------+-------------+---------------+
```


## Example 2: Calculate running maximum over a 2-row window

```ppl
source=otellogs
| sort @timestamp
| streamstats current=false window=2 max(severityNumber) as prev_max
| fields severityNumber, severityText, prev_max
| head 6
```

The query returns the following results:

```text
fetched rows / total rows = 6/6
+----------------+--------------+----------+
| severityNumber | severityText | prev_max |
|----------------+--------------+----------|
| 9              | INFO         | null     |
| 17             | ERROR        | 9        |
| 13             | WARN         | 17       |
| 5              | DEBUG        | 17       |
| 9              | INFO         | 13       |
| 21             | FATAL        | 9        |
+----------------+--------------+----------+
```


## Example 3: Global window with group-by

```ppl
source=otellogs
| sort @timestamp
| streamstats window=2 global=true avg(severityNumber) as running_avg by flags
| fields severityNumber, severityText, flags, running_avg
| head 6
```

The query returns the following results:

```text
fetched rows / total rows = 6/6
+----------------+--------------+-------+-------------+
| severityNumber | severityText | flags | running_avg |
|----------------+--------------+-------+-------------|
| 9              | INFO         | 1     | 9.0         |
| 17             | ERROR        | 1     | 13.0        |
| 13             | WARN         | 0     | 13.0        |
| 5              | DEBUG        | 0     | 9.0         |
| 9              | INFO         | 0     | 7.0         |
| 21             | FATAL        | 0     | 15.0        |
+----------------+--------------+-------+-------------+
```


## Example 4: Group-specific windows

```ppl
source=otellogs
| sort @timestamp
| streamstats window=2 global=false avg(severityNumber) as running_avg by flags
| fields severityNumber, severityText, flags, running_avg
| head 6
```

The query returns the following results:

```text
fetched rows / total rows = 6/6
+----------------+--------------+-------+-------------+
| severityNumber | severityText | flags | running_avg |
|----------------+--------------+-------+-------------|
| 9              | INFO         | 1     | 9.0         |
| 17             | ERROR        | 1     | 13.0        |
| 13             | WARN         | 0     | 13.0        |
| 5              | DEBUG        | 0     | 9.0         |
| 9              | INFO         | 0     | 7.0         |
| 21             | FATAL        | 0     | 15.0        |
+----------------+--------------+-------+-------------+
```


## Example 5: Null bucket handling with bucket_nullable=false

```ppl
source=otellogs
| streamstats bucket_nullable=false count() as cnt by `attributes.user.email`
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


## Example 6: Null bucket handling with bucket_nullable=true

```ppl
source=otellogs
| streamstats bucket_nullable=true count() as cnt by `attributes.user.email`
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
| INFO         | null                  | 1   |
| ERROR        | user@example.com      | 1   |
| WARN         | null                  | 2   |
| DEBUG        | null                  | 3   |
+--------------+-----------------------+-----+
```

