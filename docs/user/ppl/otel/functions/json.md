# JSON Functions (otellogs)

## JSON_VALID

```ppl
source=otellogs
| eval is_valid = json_valid('{"key": "value"}'), is_invalid = json_valid('{invalid}')
| fields is_valid, is_invalid
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+----------+------------+
| is_valid | is_invalid |
|----------+------------|
| True     | False      |
+----------+------------+
```


## JSON_OBJECT

```ppl
source=otellogs
| sort @timestamp
| eval json_result = json_object('severity', severityNumber, 'text', severityText)
| fields json_result
| head 3
```

Expected output:

```text
fetched rows / total rows = 3/3
+----------------------------------+
| json_result                      |
|----------------------------------|
| {"severity":9,"text":"INFO"}     |
| {"severity":17,"text":"ERROR"}   |
| {"severity":13,"text":"WARN"}    |
+----------------------------------+
```


## JSON_ARRAY

```ppl
source=otellogs
| eval json_arr = json_array(severityNumber, severityText, flags)
| fields json_arr
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------------+
| json_arr      |
|---------------|
| [9,"INFO",1]  |
+---------------+
```


## JSON_ARRAY_LENGTH

```ppl
source=otellogs
| eval arr_len = json_array_length('[1,2,3,4,5]')
| fields arr_len
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------+
| arr_len |
|---------|
| 5       |
+---------+
```

Example with non-array JSON:

```ppl
source=otellogs
| eval arr_len = json_array_length('{"key": "value"}')
| fields arr_len
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------+
| arr_len |
|---------|
| null    |
+---------+
```


## JSON_EXTRACT

```ppl
source=otellogs
| eval extracted = json_extract('{"a": {"b": "value"}}', 'a.b')
| fields extracted
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-----------+
| extracted |
|-----------|
| "value"   |
+-----------+
```


## JSON_KEYS

```ppl
source=otellogs
| eval keys = json_keys('{"severity": 9, "text": "INFO", "flags": 1}')
| fields keys
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-----------------------------+
| keys                        |
|-----------------------------|
| ["flags","severity","text"] |
+-----------------------------+
```
