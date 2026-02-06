# Date and Time Functions (otellogs)

## NOW

```ppl
source=otellogs
| eval current = NOW()
| fields current
| head 1
```

Expected output will show the current timestamp (varies by execution time).


## CURDATE

```ppl
source=otellogs
| eval today = CURDATE()
| fields today
| head 1
```

Expected output will show the current date.


## YEAR

```ppl
source=otellogs
| sort @timestamp
| eval yr = YEAR(@timestamp)
| fields @timestamp, yr
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+-------------------------------+------+
| @timestamp                    | yr   |
|-------------------------------+------|
| 2024-01-15 10:30:00.123456789 | 2024 |
| 2024-01-15 10:30:01.234567890 | 2024 |
| 2024-01-15 10:30:02.345678901 | 2024 |
| 2024-01-15 10:30:03.456789012 | 2024 |
+-------------------------------+------+
```


## MONTH

```ppl
source=otellogs
| sort @timestamp
| eval mo = MONTH(@timestamp)
| fields @timestamp, mo
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+-------------------------------+----+
| @timestamp                    | mo |
|-------------------------------+----|
| 2024-01-15 10:30:00.123456789 | 1  |
| 2024-01-15 10:30:01.234567890 | 1  |
| 2024-01-15 10:30:02.345678901 | 1  |
| 2024-01-15 10:30:03.456789012 | 1  |
+-------------------------------+----+
```


## DAYOFMONTH

```ppl
source=otellogs
| sort @timestamp
| eval dom = DAYOFMONTH(@timestamp)
| fields @timestamp, dom
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+-------------------------------+-----+
| @timestamp                    | dom |
|-------------------------------+-----|
| 2024-01-15 10:30:00.123456789 | 15  |
| 2024-01-15 10:30:01.234567890 | 15  |
| 2024-01-15 10:30:02.345678901 | 15  |
| 2024-01-15 10:30:03.456789012 | 15  |
+-------------------------------+-----+
```


## HOUR

```ppl
source=otellogs
| sort @timestamp
| eval hr = HOUR(@timestamp)
| fields @timestamp, hr
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+-------------------------------+----+
| @timestamp                    | hr |
|-------------------------------+----|
| 2024-01-15 10:30:00.123456789 | 10 |
| 2024-01-15 10:30:01.234567890 | 10 |
| 2024-01-15 10:30:02.345678901 | 10 |
| 2024-01-15 10:30:03.456789012 | 10 |
+-------------------------------+----+
```


## MINUTE

```ppl
source=otellogs
| sort @timestamp
| eval mi = MINUTE(@timestamp)
| fields @timestamp, mi
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+-------------------------------+----+
| @timestamp                    | mi |
|-------------------------------+----|
| 2024-01-15 10:30:00.123456789 | 30 |
| 2024-01-15 10:30:01.234567890 | 30 |
| 2024-01-15 10:30:02.345678901 | 30 |
| 2024-01-15 10:30:03.456789012 | 30 |
+-------------------------------+----+
```


## SECOND

```ppl
source=otellogs
| sort @timestamp
| eval sec = SECOND(@timestamp)
| fields @timestamp, sec
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+-------------------------------+-----+
| @timestamp                    | sec |
|-------------------------------+-----|
| 2024-01-15 10:30:00.123456789 | 0   |
| 2024-01-15 10:30:01.234567890 | 1   |
| 2024-01-15 10:30:02.345678901 | 2   |
| 2024-01-15 10:30:03.456789012 | 3   |
+-------------------------------+-----+
```


## DAYOFWEEK

```ppl
source=otellogs
| sort @timestamp
| eval dow = DAYOFWEEK(@timestamp)
| fields @timestamp, dow
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+-------------------------------+-----+
| @timestamp                    | dow |
|-------------------------------+-----|
| 2024-01-15 10:30:00.123456789 | 2   |
| 2024-01-15 10:30:01.234567890 | 2   |
| 2024-01-15 10:30:02.345678901 | 2   |
| 2024-01-15 10:30:03.456789012 | 2   |
+-------------------------------+-----+
```


## DAYNAME

```ppl
source=otellogs
| sort @timestamp
| eval dayname = DAYNAME(@timestamp)
| fields @timestamp, dayname
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+-------------------------------+---------+
| @timestamp                    | dayname |
|-------------------------------+---------|
| 2024-01-15 10:30:00.123456789 | Monday  |
| 2024-01-15 10:30:01.234567890 | Monday  |
| 2024-01-15 10:30:02.345678901 | Monday  |
| 2024-01-15 10:30:03.456789012 | Monday  |
+-------------------------------+---------+
```


## MONTHNAME

```ppl
source=otellogs
| sort @timestamp
| eval monthname = MONTHNAME(@timestamp)
| fields @timestamp, monthname
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+-------------------------------+-----------+
| @timestamp                    | monthname |
|-------------------------------+-----------|
| 2024-01-15 10:30:00.123456789 | January   |
| 2024-01-15 10:30:01.234567890 | January   |
| 2024-01-15 10:30:02.345678901 | January   |
| 2024-01-15 10:30:03.456789012 | January   |
+-------------------------------+-----------+
```


## DATE_FORMAT

```ppl
source=otellogs
| sort @timestamp
| eval formatted = DATE_FORMAT(@timestamp, '%Y-%m-%d %H:%i')
| fields @timestamp, formatted
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+-------------------------------+------------------+
| @timestamp                    | formatted        |
|-------------------------------+------------------|
| 2024-01-15 10:30:00.123456789 | 2024-01-15 10:30 |
| 2024-01-15 10:30:01.234567890 | 2024-01-15 10:30 |
| 2024-01-15 10:30:02.345678901 | 2024-01-15 10:30 |
| 2024-01-15 10:30:03.456789012 | 2024-01-15 10:30 |
+-------------------------------+------------------+
```


## DATEDIFF

```ppl
source=otellogs
| eval diff = DATEDIFF(DATE('2024-01-20'), DATE('2024-01-15'))
| fields diff
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+------+
| diff |
|------|
| 5    |
+------+
```


## ADDDATE

```ppl
source=otellogs
| eval result = ADDDATE(DATE('2024-01-15'), 10)
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+------------+
| result     |
|------------|
| 2024-01-25 |
+------------+
```


## SUBDATE

```ppl
source=otellogs
| eval result = SUBDATE(DATE('2024-01-15'), 5)
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+------------+
| result     |
|------------|
| 2024-01-10 |
+------------+
```
