# String Functions (otellogs)

## CONCAT

```ppl
source=otellogs
| sort @timestamp
| eval result = CONCAT('Level: ', severityText)
| fields severityText, result
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+--------------+--------------+
| severityText | result       |
|--------------+--------------|
| INFO         | Level: INFO  |
| ERROR        | Level: ERROR |
| WARN         | Level: WARN  |
| DEBUG        | Level: DEBUG |
+--------------+--------------+
```

## CONCAT_WS

```ppl
source=otellogs
| sort @timestamp
| eval result = CONCAT_WS(' - ', severityText, 'log')
| fields severityText, result
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+--------------+--------------+
| severityText | result       |
|--------------+--------------|
| INFO         | INFO - log   |
| ERROR        | ERROR - log  |
| WARN         | WARN - log   |
| DEBUG        | DEBUG - log  |
+--------------+--------------+
```

## LENGTH

```ppl
source=otellogs
| sort @timestamp
| eval len = LENGTH(severityText)
| fields severityText, len
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+--------------+-----+
| severityText | len |
|--------------+-----|
| INFO         | 4   |
| ERROR        | 5   |
| WARN         | 4   |
| DEBUG        | 5   |
+--------------+-----+
```

## LIKE

```ppl
source=otellogs
| where LIKE(severityText, 'ERR%')
| sort @timestamp
| fields severityText
| head 3
```

Expected output:

```text
fetched rows / total rows = 3/3
+--------------+
| severityText |
|--------------|
| ERROR        |
| ERROR        |
| ERROR2       |
+--------------+
```

## LOWER

```ppl
source=otellogs
| sort @timestamp
| eval lower_text = LOWER(severityText)
| fields severityText, lower_text
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+--------------+------------+
| severityText | lower_text |
|--------------+------------|
| INFO         | info       |
| ERROR        | error      |
| WARN         | warn       |
| DEBUG        | debug      |
+--------------+------------+
```

## UPPER

```ppl
source=otellogs
| eval upper_text = UPPER('hello')
| fields upper_text
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+------------+
| upper_text |
|------------|
| HELLO      |
+------------+
```

## SUBSTRING

```ppl
source=otellogs
| sort @timestamp
| eval sub = SUBSTRING(severityText, 1, 3)
| fields severityText, sub
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+--------------+-----+
| severityText | sub |
|--------------+-----|
| INFO         | INF |
| ERROR        | ERR |
| WARN         | WAR |
| DEBUG        | DEB |
+--------------+-----+
```

## TRIM

```ppl
source=otellogs
| eval trimmed = TRIM('  hello  ')
| fields trimmed
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------+
| trimmed |
|---------|
| hello   |
+---------+
```

## LTRIM

```ppl
source=otellogs
| eval trimmed = LTRIM('   hello')
| fields trimmed
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------+
| trimmed |
|---------|
| hello   |
+---------+
```

## RTRIM

```ppl
source=otellogs
| eval trimmed = RTRIM('hello   ')
| fields trimmed
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------+
| trimmed |
|---------|
| hello   |
+---------+
```

## REPLACE

```ppl
source=otellogs
| sort @timestamp
| eval replaced = REPLACE(severityText, 'ERROR', 'ERR')
| fields severityText, replaced
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+--------------+----------+
| severityText | replaced |
|--------------+----------|
| INFO         | INFO     |
| ERROR        | ERR      |
| WARN         | WARN     |
| DEBUG        | DEBUG    |
+--------------+----------+
```

## REVERSE

```ppl
source=otellogs
| sort @timestamp
| eval reversed = REVERSE(severityText)
| fields severityText, reversed
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+--------------+----------+
| severityText | reversed |
|--------------+----------|
| INFO         | OFNI     |
| ERROR        | RORRE    |
| WARN         | NRAW     |
| DEBUG        | GUBED    |
+--------------+----------+
```

## LOCATE

```ppl
source=otellogs
| eval pos = LOCATE('world', 'hello world')
| fields pos
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-----+
| pos |
|-----|
| 7   |
+-----+
```

## POSITION

```ppl
source=otellogs
| eval pos = POSITION('world' IN 'hello world')
| fields pos
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-----+
| pos |
|-----|
| 7   |
+-----+
```

## RIGHT

```ppl
source=otellogs
| sort @timestamp
| eval right_chars = RIGHT(severityText, 2)
| fields severityText, right_chars
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+--------------+-------------+
| severityText | right_chars |
|--------------+-------------|
| INFO         | FO          |
| ERROR        | OR          |
| WARN         | RN          |
| DEBUG        | UG          |
+--------------+-------------+
```
