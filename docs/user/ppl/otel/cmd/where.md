# where (otellogs)

The `where` command filters the search results. It only returns results that match the specified conditions.

## Example 1: Filter by numeric values

The following query returns logs where `severityNumber` is greater than `15`:

```ppl
source=otellogs
| where severityNumber > 15
| sort @timestamp
| fields severityNumber, severityText
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 17             | ERROR        |
| 21             | FATAL        |
| 17             | ERROR        |
| 18             | ERROR2       |
+----------------+--------------+
```

## Example 2: Filter using combined criteria

The following query combines multiple conditions using an `AND` operator:

```ppl
source=otellogs
| where severityNumber > 10 AND flags = 0
| sort @timestamp
| fields severityNumber, flags, severityText
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------+-------+--------------+
| severityNumber | flags | severityText |
|----------------+-------+--------------|
| 13             | 0     | WARN         |
| 21             | 0     | FATAL        |
| 17             | 0     | ERROR        |
+----------------+-------+--------------+
```


## Example 3: Filter with multiple possible values

The following query fetches all the documents from the `otellogs` index where `severityNumber` is `9` or `severityText` is `ERROR`:

```ppl
source=otellogs
| where severityNumber=9 or severityText="ERROR"
| sort @timestamp
| fields severityNumber, severityText
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 9              | INFO         |
| 17             | ERROR        |
| 9              | INFO         |
| 17             | ERROR        |
+----------------+--------------+
```


## Example 4: Filter by text patterns

The `LIKE` operator enables pattern matching on string fields using wildcards.

### Matching a single character

The following query uses an underscore (`_`) to match a single character:

```ppl
source=otellogs
| where LIKE(severityText, 'ERR__')
| sort @timestamp
| fields severityNumber, severityText
| head 2
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 17             | ERROR        |
| 17             | ERROR        |
+----------------+--------------+
```

### Matching multiple characters

The following query uses a percent sign (`%`) to match multiple characters:

```ppl
source=otellogs
| where LIKE(severityText, 'ERR%')
| sort @timestamp
| fields severityNumber, severityText
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 17             | ERROR        |
| 17             | ERROR        |
| 18             | ERROR2       |
+----------------+--------------+
```

## Example 5: Filter by excluding specific values

The following query uses a `NOT` operator to exclude matching records:

```ppl
source=otellogs
| where NOT severityText = 'INFO'
| sort @timestamp
| fields severityNumber, severityText
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 17             | ERROR        |
| 13             | WARN         |
| 5              | DEBUG        |
| 21             | FATAL        |
+----------------+--------------+
```


## Example 6: Filter using value lists

The following query uses an `IN` operator to match multiple values:

```ppl
source=otellogs
| where severityText IN ('INFO', 'ERROR')
| sort @timestamp
| fields severityNumber, severityText
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 9              | INFO         |
| 17             | ERROR        |
| 9              | INFO         |
| 17             | ERROR        |
+----------------+--------------+
```


## Example 7: Filter records with missing data

The following query returns records where the `traceId` field is empty (null or blank):

```ppl
source=otellogs
| where traceId = ''
| sort @timestamp
| fields severityText, traceId
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+---------+
| severityText | traceId |
|--------------+---------|
| WARN         |         |
| DEBUG        |         |
| INFO         |         |
| FATAL        |         |
+--------------+---------+
```


## Example 8: Filter using grouped conditions

The following query combines multiple conditions using parentheses and logical operators:

```ppl
source=otellogs
| where (severityNumber > 15 OR severityNumber < 5) AND flags = 0
| sort @timestamp
| fields severityNumber, flags, severityText
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+-------+--------------+
| severityNumber | flags | severityText |
|----------------+-------+--------------|
| 21             | 0     | FATAL        |
| 17             | 0     | ERROR        |
| 2              | 0     | TRACE2       |
| 18             | 0     | ERROR2       |
+----------------+-------+--------------+
```
