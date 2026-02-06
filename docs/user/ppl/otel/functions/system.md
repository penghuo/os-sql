# System Functions (otellogs)

## TYPEOF

Returns the name of the data type of the value passed to it:

```ppl
source=otellogs
| eval `typeof(date)` = typeof(DATE('2008-04-14')), `typeof(int)` = typeof(1), `typeof(now())` = typeof(now()), `typeof(sev)` = typeof(severityNumber)
| fields `typeof(date)`, `typeof(int)`, `typeof(now())`, `typeof(sev)`
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------------+-------------+---------------+-------------+
| typeof(date) | typeof(int) | typeof(now()) | typeof(sev) |
|--------------+-------------+---------------+-------------|
| DATE         | INT         | TIMESTAMP     | LONG        |
+--------------+-------------+---------------+-------------+
```

Example checking various field types:

```ppl
source=otellogs
| eval type_body = typeof(body), type_flags = typeof(flags), type_timestamp = typeof(`@timestamp`)
| fields type_body, type_flags, type_timestamp
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-----------+------------+----------------+
| type_body | type_flags | type_timestamp |
|-----------+------------+----------------|
| STRING    | LONG       | TIMESTAMP      |
+-----------+------------+----------------+
```

