# sort (otellogs)

The `sort` command sorts the search results by the specified fields.

## Example 1: Sort by one field

The following query sorts all documents by the `severityNumber` field in ascending order:

```ppl
source=otellogs
| sort severityNumber
| fields severityNumber, severityText
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 1              | TRACE        |
| 2              | TRACE2       |
| 3              | TRACE3       |
| 4              | TRACE4       |
+----------------+--------------+
```


## Example 2: Sort by one field in descending order

The following query sorts all documents by the `severityNumber` field in descending order. You can use either prefix notation (`- severityNumber`) or suffix notation (`severityNumber desc`):

```ppl
source=otellogs
| sort - severityNumber
| fields severityNumber, severityText
| head 4
```

This query is equivalent to the following query:

```ppl
source=otellogs
| sort severityNumber desc
| fields severityNumber, severityText
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 24             | FATAL4       |
| 23             | FATAL3       |
| 22             | FATAL2       |
| 21             | FATAL        |
+----------------+--------------+
```


## Example 3: Sort by multiple fields in prefix notation

The following query uses prefix notation to sort all documents by the `flags` field in ascending order and the `severityNumber` field in descending order:

```ppl
source=otellogs
| sort + flags, - severityNumber
| fields severityNumber, flags, severityText
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+-------+--------------+
| severityNumber | flags | severityText |
|----------------+-------+--------------|
| 24             | 0     | FATAL4       |
| 23             | 0     | FATAL3       |
| 22             | 0     | FATAL2       |
| 21             | 0     | FATAL        |
+----------------+-------+--------------+
```


## Example 4: Sort by multiple fields in suffix notation

The following query uses suffix notation to sort all documents by the `flags` field in ascending order and the `severityNumber` field in descending order:

```ppl
source=otellogs
| sort flags asc, severityNumber desc
| fields severityNumber, flags, severityText
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+-------+--------------+
| severityNumber | flags | severityText |
|----------------+-------+--------------|
| 24             | 0     | FATAL4       |
| 23             | 0     | FATAL3       |
| 22             | 0     | FATAL2       |
| 21             | 0     | FATAL        |
+----------------+-------+--------------+
```


## Example 5: Sort fields with null values

The default ascending order lists null values first. The following query sorts the `traceId` field in the default order:

```ppl
source=otellogs
| sort traceId
| fields traceId, severityText
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------------------------+--------------+
| traceId                          | severityText |
|----------------------------------+--------------|
|                                  | WARN         |
|                                  | DEBUG        |
|                                  | INFO         |
|                                  | FATAL        |
+----------------------------------+--------------+
```


## Example 6: Specify the number of sorted documents to return

The following query sorts all documents and returns two documents:

```ppl
source=otellogs
| sort 2 severityNumber
| fields severityNumber, severityText
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 1              | TRACE        |
| 2              | TRACE2       |
+----------------+--------------+
```


## Example 7: Sort by specifying field type

The following query uses the `sort` command with `str()` to sort numeric values lexicographically:

```ppl
source=otellogs
| sort str(severityNumber)
| fields severityNumber, severityText
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 1              | TRACE        |
| 10             | INFO2        |
| 11             | INFO3        |
| 12             | INFO4        |
+----------------+--------------+
```
