# stats (otellogs)

The `stats` command calculates aggregations on the search results.

## Example 1: Calculate the count of events

The following query calculates the count of events in the `otellogs` index:

```ppl
source=otellogs
| stats count()
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------+
| count() |
|---------|
| 32      |
+---------+
```


## Example 2: Calculate the average of a field

The following query calculates the average severityNumber for all logs:

```ppl
source=otellogs
| stats avg(severityNumber)
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


## Example 3: Calculate the average of a field by group

The following query calculates the average severityNumber for all logs, grouped by flags:

```ppl
source=otellogs
| stats avg(severityNumber) by flags
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+---------------------+-------+
| avg(severityNumber) | flags |
|---------------------+-------|
| 13.0                | 1     |
| 12.466666666666667  | 0     |
+---------------------+-------+
```


## Example 4: Calculate the average, sum, and count of a field by group

The following query calculates the average severityNumber, sum of severityNumbers, and count of events for all logs, grouped by flags:

```ppl
source=otellogs
| stats avg(severityNumber), sum(severityNumber), count() by flags
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+---------------------+---------------------+---------+-------+
| avg(severityNumber) | sum(severityNumber) | count() | flags |
|---------------------+---------------------+---------+-------|
| 13.0                | 26                  | 2       | 1     |
| 12.466666666666667  | 374                 | 30      | 0     |
+---------------------+---------------------+---------+-------+
```


## Example 5: Calculate the maximum of a field

The following query calculates the maximum severityNumber for all logs:

```ppl
source=otellogs
| stats max(severityNumber)
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------------------+
| max(severityNumber) |
|---------------------|
| 24                  |
+---------------------+
```


## Example 6: Calculate the maximum and minimum of a field by group

The following query calculates the maximum and minimum severityNumber for all logs, grouped by flags:

```ppl
source=otellogs
| stats max(severityNumber), min(severityNumber) by flags
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+---------------------+---------------------+-------+
| max(severityNumber) | min(severityNumber) | flags |
|---------------------+---------------------+-------|
| 17                  | 9                   | 1     |
| 24                  | 1                   | 0     |
+---------------------+---------------------+-------+
```


## Example 7: Calculate the distinct count of a field

To retrieve the count of distinct values of a field, you can use the `DISTINCT_COUNT` (or `DC`) function instead of `COUNT`. The following query calculates both the count and the distinct count of the `severityText` field for all logs:

```ppl
source=otellogs
| stats count(severityText), distinct_count(severityText)
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------------------+-----------------------------+
| count(severityText) | distinct_count(severityText) |
|--------------------+-----------------------------|
| 32                 | 24                          |
+--------------------+-----------------------------+
```


## Example 8: Calculate the count by a span

The following query retrieves the count of `severityNumber` values grouped into 5-number intervals:

```ppl
source=otellogs
| stats count(severityNumber) by span(severityNumber, 5) as severity_span
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+-----------------------+---------------+
| count(severityNumber) | severity_span |
|-----------------------+---------------|
| 4                     | 0             |
| 8                     | 5             |
| 8                     | 10            |
| 8                     | 15            |
| 4                     | 20            |
+-----------------------+---------------+
```


## Example 9: Calculate the count by a flags and span

The following query retrieves the count of `severityNumber` grouped into 10-number intervals and broken down by `flags`:

```ppl
source=otellogs
| stats count() as cnt by span(severityNumber, 10) as severity_span, flags
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-----+---------------+-------+
| cnt | severity_span | flags |
|-----+---------------+-------|
| 9   | 0             | 0     |
| 1   | 0             | 1     |
| 17  | 10            | 0     |
| 1   | 10            | 1     |
+-----+---------------+-------|
```


## Example 10: Count and retrieve a severityText list by flags and severity span

The following query calculates the count of `severityNumber` values grouped into 10-number intervals as well as by `flags` and also returns a list of up to 5 severityText values for each group:

```ppl
source=otellogs
| stats count() as cnt, take(severityText, 5) by span(severityNumber, 10) as severity_span, flags
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-----+--------------------------------------+---------------+-------+
| cnt | take(severityText, 5)                | severity_span | flags |
|-----+--------------------------------------+---------------+-------|
| 9   | [TRACE,TRACE2,TRACE3,TRACE4,DEBUG]   | 0             | 0     |
| 1   | [INFO]                               | 0             | 1     |
| 17  | [INFO2,WARN,WARN2,INFO3,WARN3]       | 10            | 0     |
| 1   | [ERROR]                              | 10            | 1     |
+-----+--------------------------------------+---------------+-------+
```


## Example 11: Calculate the percentile of a field

The following query calculates the 90th percentile of `severityNumber` for all logs:

```ppl
source=otellogs
| stats percentile(severityNumber, 90)
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+------------------------------+
| percentile(severityNumber, 90) |
|------------------------------|
| 22                           |
+------------------------------+
```


## Example 12: Calculate the percentile of a field by group

The following query calculates the 90th percentile of `severityNumber` for all logs, grouped by `flags`:

```ppl
source=otellogs
| stats percentile(severityNumber, 90) by flags
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+--------------------------------+-------+
| percentile(severityNumber, 90) | flags |
|--------------------------------+-------|
| 17                             | 1     |
| 22                             | 0     |
+--------------------------------+-------+
```


## Example 13: Calculate the percentile by a flags and span

The following query calculates the 90th percentile of `severityNumber`, grouped into 10-number intervals as well as by `flags`:

```ppl
source=otellogs
| stats percentile(severityNumber, 90) as p90 by span(severityNumber, 10) as severity_span, flags
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-----+---------------+-------+
| p90 | severity_span | flags |
|-----+---------------+-------|
| 8   | 0             | 0     |
| 9   | 0             | 1     |
| 24  | 10            | 0     |
| 17  | 10            | 1     |
+-----+---------------+-------+
```


## Example 14: Collect all values in a field using LIST

The following query collects all `severityText` values for the first 5 records:

```ppl
source=otellogs
| sort @timestamp
| head 5
| stats list(severityText)
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+------------------------------------+
| list(severityText)                 |
|------------------------------------|
| [INFO,ERROR,WARN,DEBUG,INFO]       |
+------------------------------------+
```


## Example 15: Ignore a null bucket

The following query excludes null values from grouping by setting `bucket_nullable=false`:

```ppl
source=otellogs
| stats bucket_nullable=false count() as cnt by `attributes.user.email`
```

The query returns the following results:

```text
fetched rows / total rows = 7/7
+-----+---------------------------+
| cnt | attributes.user.email     |
|-----+---------------------------|
| 1   | admin@company.org         |
| 1   | alice@wonderland.net      |
| 1   | grpc-user@service.net     |
| 1   | support@helpdesk.io       |
| 1   | test.user@domain.co.uk    |
| 1   | user@example.com          |
| 1   | webhook@partner.com       |
+-----+---------------------------+
```


## Example 16: Collect unique values in a field using VALUES

The following query collects all unique `severityText` values for the first 8 records, sorted lexicographically with duplicates removed:

```ppl
source=otellogs
| sort @timestamp
| head 8
| stats values(severityText)
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+-------------------------------------+
| values(severityText)                |
|-------------------------------------|
| [DEBUG,ERROR,FATAL,INFO,TRACE,WARN] |
+-------------------------------------+
```
