# Expressions (otellogs)

## Arithmetic Operators

Arithmetic expressions are formed by numeric literals and binary operators: `+`, `-`, `*`, `/`, `%`.

```ppl
source=otellogs
| where severityNumber > (10 + 5)
| fields severityNumber, severityText
| head 5
```

Expected output:

```text
fetched rows / total rows = 5/5
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 17             | ERROR        |
| 21             | FATAL        |
| 17             | ERROR        |
| 18             | ERROR2       |
| 19             | ERROR3       |
+----------------+--------------+
```


## Predicate Operators

### Greater than operator

```ppl
source=otellogs
| where severityNumber > 20
| fields severityNumber, severityText
```

Expected output:

```text
fetched rows / total rows = 4/4
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 21             | FATAL        |
| 22             | FATAL2       |
| 23             | FATAL3       |
| 24             | FATAL4       |
+----------------+--------------+
```


### Equal operator (alternative syntax)

The `==` operator can be used as an alternative to `=` for equality comparisons:

```ppl
source=otellogs
| where severityNumber == 9
| stats count() as info_count
```

Expected output:

```text
fetched rows / total rows = 1/1
+------------+
| info_count |
|------------|
| 8          |
+------------+
```


### IN operator

```ppl
source=otellogs
| where severityNumber in (9, 13, 17)
| stats count() by severityText
```

Expected output:

```text
fetched rows / total rows = 3/3
+---------+--------------+
| count() | severityText |
|---------+--------------|
| 2       | ERROR        |
| 8       | INFO         |
| 2       | WARN         |
+---------+--------------+
```


### OR operator

```ppl
source=otellogs
| where severityNumber = 9 OR severityNumber = 17
| stats count() by severityText
```

Expected output:

```text
fetched rows / total rows = 2/2
+---------+--------------+
| count() | severityText |
|---------+--------------|
| 2       | ERROR        |
| 8       | INFO         |
+---------+--------------+
```


### NOT operator

```ppl
source=otellogs
| where not severityNumber in (9, 13, 17)
| stats count()
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------+
| count() |
|---------|
| 20      |
+---------+
```

