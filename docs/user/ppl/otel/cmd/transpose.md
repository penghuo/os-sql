# transpose (otellogs)

The `transpose` command outputs rows as columns, effectively transposing each result row into a corresponding column.

## Example 1: Transpose results

```ppl
source=otellogs
| sort @timestamp
| head 5
| fields severityNumber, severityText, flags
| transpose
```

Expected output:

```text
fetched rows / total rows = 3/3
+----------------+-------+-------+-------+-------+-------+
| column         | row 1 | row 2 | row 3 | row 4 | row 5 |
|----------------+-------+-------+-------+-------+-------|
| flags          | 1     | 1     | 0     | 0     | 0     |
| severityNumber | 9     | 17    | 13    | 5     | 9     |
| severityText   | INFO  | ERROR | WARN  | DEBUG | INFO  |
+----------------+-------+-------+-------+-------+-------+
```


## Example 2: Transpose with specified row count

```ppl
source=otellogs
| sort @timestamp
| head 5
| fields severityNumber, severityText, flags
| transpose 4
```

Expected output:

```text
fetched rows / total rows = 3/3
+----------------+-------+-------+-------+-------+
| column         | row 1 | row 2 | row 3 | row 4 |
|----------------+-------+-------+-------+-------|
| flags          | 1     | 1     | 0     | 0     |
| severityNumber | 9     | 17    | 13    | 5     |
| severityText   | INFO  | ERROR | WARN  | DEBUG |
+----------------+-------+-------+-------+-------+
```


## Example 3: Transpose with custom column name

```ppl
source=otellogs
| sort @timestamp
| head 5
| fields severityNumber, severityText, flags
| transpose 4 column_name='field_names'
```

Expected output:

```text
fetched rows / total rows = 3/3
+----------------+-------+-------+-------+-------+
| field_names    | row 1 | row 2 | row 3 | row 4 |
|----------------+-------+-------+-------+-------|
| flags          | 1     | 1     | 0     | 0     |
| severityNumber | 9     | 17    | 13    | 5     |
| severityText   | INFO  | ERROR | WARN  | DEBUG |
+----------------+-------+-------+-------+-------+
```

