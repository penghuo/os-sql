# rename (otellogs)

The `rename` command renames one or more fields in the search results.

## Example 1: Rename a field

The following query renames one field:

```ppl
source=otellogs
| sort @timestamp
| rename severityNumber as sn
| fields sn
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----+
| sn |
|----|
| 9  |
| 17 |
| 13 |
| 5  |
+----+
```


## Example 2: Rename multiple fields

The following query renames multiple fields:

```ppl
source=otellogs
| sort @timestamp
| rename severityNumber as sn, severityText as st
| fields sn, st
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----+-------+
| sn | st    |
|----+-------|
| 9  | INFO  |
| 17 | ERROR |
| 13 | WARN  |
| 5  | DEBUG |
+----+-------+
```


## Example 3: Rename fields using wildcards

The following query renames multiple fields using wildcard patterns:

```ppl
source=otellogs
| sort @timestamp
| rename severity* as sev_*
| fields sev_Number, sev_Text
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+------------+----------+
| sev_Number | sev_Text |
|------------+----------|
| 9          | INFO     |
| 17         | ERROR    |
| 13         | WARN     |
| 5          | DEBUG    |
+------------+----------+
```


## Example 4: Rename fields using multiple wildcard patterns

The following query renames multiple fields using multiple wildcard patterns:

```ppl
source=otellogs
| sort @timestamp
| rename severity* as sev_*, *Id as *_id
| fields sev_Number, sev_Text, trace_id
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+------------+----------+----------------------------------+
| sev_Number | sev_Text | trace_id                         |
|------------+----------+----------------------------------|
| 9          | INFO     | b3cb01a03c846973fd496b973f49be85 |
| 17         | ERROR    | 7475a30207dbef54d29e42c37f09a528 |
| 13         | WARN     |                                  |
| 5          | DEBUG    |                                  |
+------------+----------+----------------------------------+
```


## Example 5: Rename an existing field to another existing field

The following query renames an existing field to another existing field. The target field is removed and the source field is renamed to the target field:

```ppl
source=otellogs
| sort @timestamp
| rename severityText as severityNumber
| fields severityNumber
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+
| severityNumber |
|----------------|
| INFO           |
| ERROR          |
| WARN           |
| DEBUG          |
+----------------+
```
