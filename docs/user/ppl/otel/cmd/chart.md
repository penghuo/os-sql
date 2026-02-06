# chart (otellogs)

The `chart` command transforms search results by applying a statistical aggregation function and optionally grouping the data.

## Example 1: Basic aggregation without grouping

```ppl
source=otellogs
| chart avg(severityNumber)
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------------------+
| avg(severityNumber) |
|---------------------|
| 12.5                |
+---------------------+
```


## Example 2: Group by a single field

```ppl
source=otellogs
| chart count() by flags
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+-------+---------+
| flags | count() |
|-------+---------|
| 0     | 30      |
| 1     | 2       |
+-------+---------+
```


## Example 3: Using over [] by [] to group by multiple fields

```ppl
source=otellogs
| chart avg(severityNumber) over flags by severityText
| head 6
```

The query returns the following results:

```text
fetched rows / total rows = 6/6
+-------+--------------+---------------------+
| flags | severityText | avg(severityNumber) |
|-------+--------------+---------------------|
| 0     | DEBUG        | 5.0                 |
| 0     | DEBUG2       | 6.0                 |
| 0     | DEBUG3       | 7.0                 |
| 0     | DEBUG4       | 8.0                 |
| 0     | ERROR        | 17.0                |
| 0     | ERROR2       | 18.0                |
+-------+--------------+---------------------+
```


## Example 4: Using limit functionality

```ppl
source=otellogs
| chart limit=3 count() over flags by severityText
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-------+--------------+---------+
| flags | severityText | count() |
|-------+--------------+---------|
| 0     | OTHER        | 24      |
| 0     | INFO         | 6       |
| 1     | ERROR        | 1       |
| 1     | INFO         | 1       |
+-------+--------------+---------+
```


## Example 5: Using span for grouping

```ppl
source=otellogs
| chart max(severityNumber) by severityNumber span=10, flags
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------+-------+---------------------+
| severityNumber | flags | max(severityNumber) |
|----------------+-------+---------------------|
| 0              | 0     | 9                   |
| 0              | 1     | 9                   |
| 10             | 0     | 24                  |
+----------------+-------+---------------------+
```

