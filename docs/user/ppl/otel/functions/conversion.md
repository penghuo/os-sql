# Type Conversion Functions (otellogs)

## CAST to string

```ppl
source=otellogs
| sort @timestamp
| eval str_severity = CAST(severityNumber as string)
| fields severityNumber, str_severity
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+----------------+--------------+
| severityNumber | str_severity |
|----------------+--------------|
| 9              | 9            |
| 17             | 17           |
| 13             | 13           |
| 5              | 5            |
+----------------+--------------+
```


## CAST to number

```ppl
source=otellogs
| eval cbool = CAST(true as int), cstring = CAST('42' as int)
| fields cbool, cstring
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-------+---------+
| cbool | cstring |
|-------+---------|
| 1     | 42      |
+-------+---------+
```


## CAST to boolean

```ppl
source=otellogs
| eval bool_result = CAST(1 as boolean)
| fields bool_result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-------------+
| bool_result |
|-------------|
| True        |
+-------------+
```


## CAST to date

```ppl
source=otellogs
| eval cdate = CAST('2024-01-15' as date), ctime = CAST('10:30:00' as time), ctimestamp = CAST('2024-01-15 10:30:00' as timestamp)
| fields cdate, ctime, ctimestamp
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+------------+----------+---------------------+
| cdate      | ctime    | ctimestamp          |
|------------+----------+---------------------|
| 2024-01-15 | 10:30:00 | 2024-01-15 10:30:00 |
+------------+----------+---------------------+
```


## CAST chained

```ppl
source=otellogs
| eval chained = CAST(CAST(true as string) as boolean)
| fields chained
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------+
| chained |
|---------|
| True    |
+---------+
```


## TOSTRING - Convert to binary

```ppl
source=otellogs
| sort @timestamp
| eval binary_str = tostring(severityNumber, "binary")
| fields severityNumber, binary_str
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+----------------+------------+
| severityNumber | binary_str |
|----------------+------------|
| 9              | 1001       |
| 17             | 10001      |
| 13             | 1101       |
| 5              | 101        |
+----------------+------------+
```


## TOSTRING - Convert to hex

```ppl
source=otellogs
| sort @timestamp
| eval hex_str = tostring(severityNumber, "hex")
| fields severityNumber, hex_str
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+----------------+---------+
| severityNumber | hex_str |
|----------------+---------|
| 9              | 9       |
| 17             | 11      |
| 13             | d       |
| 5              | 5       |
+----------------+---------+
```


## TOSTRING - Format with commas

```ppl
source=otellogs
| eval commas_str = tostring(12345, "commas")
| fields commas_str
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+------------+
| commas_str |
|------------|
| 12,345     |
+------------+
```


## TOSTRING - Convert to duration

```ppl
source=otellogs
| eval duration = tostring(3661, "duration")
| fields duration
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+----------+
| duration |
|----------|
| 01:01:01 |
+----------+
```


## TOSTRING - Boolean to string

```ppl
source=otellogs
| eval bool_str = tostring(1=1)
| fields bool_str
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+----------+
| bool_str |
|----------|
| TRUE     |
+----------+
```


## TONUMBER - Convert binary string

```ppl
source=otellogs
| eval int_value = tonumber('010101', 2)
| fields int_value
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-----------+
| int_value |
|-----------|
| 21.0      |
+-----------+
```


## TONUMBER - Convert hex string

```ppl
source=otellogs
| eval int_value = tonumber('FF', 16)
| fields int_value
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-----------+
| int_value |
|-----------|
| 255.0     |
+-----------+
```


## TONUMBER - Convert decimal string

```ppl
source=otellogs
| eval int_value = tonumber('1234')
| fields int_value
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-----------+
| int_value |
|-----------|
| 1234.0    |
+-----------+
```
