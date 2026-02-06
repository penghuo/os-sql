# subquery (otellogs)

The `subquery` command allows embedding one PPL query within another for advanced filtering and data retrieval.

## Example 1: IN subquery

```ppl
source=otellogs
| where severityNumber in [ source=otellogs | where flags = 1 | fields severityNumber ]
| stats count() by severityText
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+---------+--------------+
| count() | severityText |
|---------+--------------|
| 8       | INFO         |
| 2       | ERROR        |
+---------+--------------+
```


## Example 2: NOT IN subquery

```ppl
source=otellogs
| where severityNumber not in [ source=otellogs | where severityNumber > 15 | fields severityNumber ]
| stats count() by severityText
| head 5
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+---------+--------------+
| count() | severityText |
|---------+--------------|
| 1       | DEBUG        |
| 1       | DEBUG2       |
| 1       | DEBUG3       |
| 1       | DEBUG4       |
| 8       | INFO         |
+---------+--------------+
```


## Example 3: Scalar subquery in where

```ppl
source=otellogs
| where severityNumber > [ source=otellogs | stats avg(severityNumber) ]
| stats count() as high_severity_count
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------------------+
| high_severity_count |
|---------------------|
| 16                  |
+---------------------+
```


## Example 4: Scalar subquery in eval

```ppl
source=otellogs
| eval max_sev = [ source=otellogs | stats max(severityNumber) ]
| fields severityNumber, severityText, max_sev
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------+--------------+---------+
| severityNumber | severityText | max_sev |
|----------------+--------------+---------|
| 9              | INFO         | 24      |
| 17             | ERROR        | 24      |
| 13             | WARN         | 24      |
+----------------+--------------+---------+
```

