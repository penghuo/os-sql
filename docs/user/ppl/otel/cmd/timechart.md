# timechart (otellogs)

The `timechart` command creates a time-based aggregation of data, grouping by time intervals.

## Example 1: Count events by time span and severity

```ppl
source=otellogs
| timechart span=1s count() by severityText
| head 10
```

The query returns the following results:

```text
fetched rows / total rows = 10/10
+-----------------------------+--------------+---------+
| @timestamp                  | severityText | count() |
|-----------------------------+--------------+---------|
| 2024-01-15 10:30:00.000000  | INFO         | 1       |
| 2024-01-15 10:30:01.000000  | ERROR        | 1       |
| 2024-01-15 10:30:02.000000  | WARN         | 1       |
| 2024-01-15 10:30:03.000000  | DEBUG        | 1       |
| 2024-01-15 10:30:04.000000  | INFO         | 1       |
| 2024-01-15 10:30:05.000000  | FATAL        | 1       |
| 2024-01-15 10:30:06.000000  | TRACE        | 1       |
| 2024-01-15 10:30:07.000000  | ERROR        | 1       |
| 2024-01-15 10:30:08.000000  | WARN         | 1       |
| 2024-01-15 10:30:09.000000  | INFO         | 1       |
+-----------------------------+--------------+---------+
```


## Example 2: Average severity by time span

```ppl
source=otellogs
| timechart span=10s avg(severityNumber)
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-----------------------------+---------------------+
| @timestamp                  | avg(severityNumber) |
|-----------------------------+---------------------|
| 2024-01-15 10:30:00.000000  | 10.9                |
| 2024-01-15 10:30:10.000000  | 12.1                |
| 2024-01-15 10:30:20.000000  | 14.2                |
| 2024-01-15 10:30:30.000000  | 9.0                 |
+-----------------------------+---------------------+
```


## Example 3: Sum by time span and flags

```ppl
source=otellogs
| timechart span=1m sum(severityNumber) by flags
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+-----------------------------+-------+---------------------+
| @timestamp                  | flags | sum(severityNumber) |
|-----------------------------+-------+---------------------|
| 2024-01-15 10:30:00.000000  | 0     | 374                 |
| 2024-01-15 10:30:00.000000  | 1     | 26                  |
+-----------------------------+-------+---------------------+
```


## Example 4: Count with limit parameter

```ppl
source=otellogs
| timechart span=1m limit=3 count() by severityText
| head 10
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-----------------------------+--------------+---------+
| @timestamp                  | severityText | count() |
|-----------------------------+--------------+---------|
| 2024-01-15 10:30:00.000000  | INFO         | 8       |
| 2024-01-15 10:30:00.000000  | OTHER        | 20      |
| 2024-01-15 10:30:00.000000  | ERROR        | 2       |
| 2024-01-15 10:30:00.000000  | WARN         | 2       |
+-----------------------------+--------------+---------+
```


## Example 5: Using useother=false

```ppl
source=otellogs
| timechart span=1m useother=false limit=3 count() by severityText
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+-----------------------------+--------------+---------+
| @timestamp                  | severityText | count() |
|-----------------------------+--------------+---------|
| 2024-01-15 10:30:00.000000  | INFO         | 8       |
| 2024-01-15 10:30:00.000000  | ERROR        | 2       |
| 2024-01-15 10:30:00.000000  | WARN         | 2       |
+-----------------------------+--------------+---------+
```

