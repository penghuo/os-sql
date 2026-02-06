# reverse (otellogs)

The `reverse` command reverses the display order of the search results.

## Example 1: Basic reverse operation

```ppl
source=otellogs
| sort @timestamp
| fields severityNumber, severityText
| head 4
| reverse
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 5              | DEBUG        |
| 13             | WARN         |
| 17             | ERROR        |
| 9              | INFO         |
+----------------+--------------+
```


## Example 2: Use the reverse and sort commands

```ppl
source=otellogs
| sort severityNumber
| fields severityNumber, severityText
| head 4
| reverse
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 4              | TRACE4       |
| 3              | TRACE3       |
| 2              | TRACE2       |
| 1              | TRACE        |
+----------------+--------------+
```


## Example 3: Double reverse

```ppl
source=otellogs
| sort @timestamp
| fields severityNumber, severityText
| head 4
| reverse
| reverse
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 9              | INFO         |
| 17             | ERROR        |
| 13             | WARN         |
| 5              | DEBUG        |
+----------------+--------------+
```
