# Statistical Functions (otellogs)

These functions are available in the eval command context.

## MAX (eval context)

Returns the maximum value from all provided arguments:

```ppl
source=otellogs
| sort @timestamp
| eval max_val = MAX(severityNumber, 15)
| fields severityNumber, severityText, max_val
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+----------------+--------------+---------+
| severityNumber | severityText | max_val |
|----------------+--------------+---------|
| 9              | INFO         | 15      |
| 17             | ERROR        | 17      |
| 13             | WARN         | 15      |
| 5              | DEBUG        | 15      |
+----------------+--------------+---------+
```

Example with string comparison:

```ppl
source=otellogs
| sort @timestamp
| eval result = MAX(severityText, 'INFO')
| fields severityText, result
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+--------------+--------+
| severityText | result |
|--------------+--------|
| INFO         | INFO   |
| ERROR        | INFO   |
| WARN         | WARN   |
| DEBUG        | INFO   |
+--------------+--------+
```


## MIN (eval context)

Returns the minimum value from all provided arguments:

```ppl
source=otellogs
| sort @timestamp
| eval min_val = MIN(severityNumber, 10)
| fields severityNumber, severityText, min_val
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+----------------+--------------+---------+
| severityNumber | severityText | min_val |
|----------------+--------------+---------|
| 9              | INFO         | 9       |
| 17             | ERROR        | 10      |
| 13             | WARN         | 10      |
| 5              | DEBUG        | 5       |
+----------------+--------------+---------+
```

Example with string comparison:

```ppl
source=otellogs
| sort @timestamp
| eval result = MIN(severityText, 'INFO')
| fields severityText, result
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+--------------+--------+
| severityText | result |
|--------------+--------|
| INFO         | INFO   |
| ERROR        | ERROR  |
| WARN         | INFO   |
| DEBUG        | DEBUG  |
+--------------+--------+
```

