# bin (otellogs)

The `bin` command groups numeric values into buckets of equal intervals.

## Example 1: Basic numeric span

```ppl
source=otellogs
| sort @timestamp
| bin severityNumber span=5
| fields severityNumber, severityText
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 5-10           | INFO         |
| 15-20          | ERROR        |
| 10-15          | WARN         |
| 5-10           | DEBUG        |
+----------------+--------------+
```


## Example 2: Large numeric span

```ppl
source=otellogs
| sort @timestamp
| bin severityNumber span=10
| fields severityNumber
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+
| severityNumber |
|----------------|
| 0-10           |
| 10-20          |
| 10-20          |
| 0-10           |
+----------------+
```


## Example 3: Low bin count

```ppl
source=otellogs
| sort @timestamp
| bin severityNumber bins=2
| fields severityNumber
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+
| severityNumber |
|----------------|
| 0-20           |
| 0-20           |
| 0-20           |
| 0-20           |
+----------------+
```


## Example 4: High bin count

```ppl
source=otellogs
| sort @timestamp
| bin severityNumber bins=10
| fields severityNumber, severityText
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 8-10           | INFO         |
| 16-18          | ERROR        |
| 12-14          | WARN         |
| 4-6            | DEBUG        |
+----------------+--------------+
```


## Example 5: Span with start/end range

```ppl
source=otellogs
| sort @timestamp
| bin severityNumber span=5 start=0 end=25
| fields severityNumber
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+
| severityNumber |
|----------------|
| 5-10           |
| 15-20          |
| 10-15          |
| 5-10           |
+----------------+
```


## Example 6: Default behavior (no parameters)

```ppl
source=otellogs
| sort @timestamp
| bin severityNumber
| fields severityNumber, severityText
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-----------+--------------+
| severityNumber | severityText |
|-----------+--------------|
| 9.0-10.0  | INFO         |
| 17.0-18.0 | ERROR        |
| 13.0-14.0 | WARN         |
| 5.0-6.0   | DEBUG        |
+-----------+--------------+
```
