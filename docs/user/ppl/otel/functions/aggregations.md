# Aggregation Functions (otellogs)

## COUNT

```ppl
source=otellogs
| stats count(), c(), count, c
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------+-----+-------+----+
| count() | c() | count | c  |
|---------+-----+-------+----|
| 32      | 32  | 32    | 32 |
+---------+-----+-------+----+
```

Example of filtered counting:

```ppl
source=otellogs
| stats count(eval(severityNumber > 15)) as high_severity_count
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------------------+
| high_severity_count |
|---------------------|
| 8                   |
+---------------------+
```


## SUM

```ppl
source=otellogs
| stats sum(severityNumber) by flags
```

Expected output:

```text
fetched rows / total rows = 2/2
+---------------------+-------+
| sum(severityNumber) | flags |
|---------------------+-------|
| 26                  | 1     |
| 374                 | 0     |
+---------------------+-------+
```


## AVG

```ppl
source=otellogs
| stats avg(severityNumber) by flags
```

Expected output:

```text
fetched rows / total rows = 2/2
+---------------------+-------+
| avg(severityNumber) | flags |
|---------------------+-------|
| 13.0                | 1     |
| 12.466666666666667  | 0     |
+---------------------+-------+
```


## MAX

```ppl
source=otellogs
| stats max(severityNumber)
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------------------+
| max(severityNumber) |
|---------------------|
| 24                  |
+---------------------+
```

Example with text field:

```ppl
source=otellogs
| stats max(severityText)
```

Expected output:

```text
fetched rows / total rows = 1/1
+-------------------+
| max(severityText) |
|-------------------|
| WARN3             |
+-------------------+
```


## MIN

```ppl
source=otellogs
| stats min(severityNumber)
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------------------+
| min(severityNumber) |
|---------------------|
| 1                   |
+---------------------+
```

Example with text field:

```ppl
source=otellogs
| stats min(severityText)
```

Expected output:

```text
fetched rows / total rows = 1/1
+-------------------+
| min(severityText) |
|-------------------|
| DEBUG             |
+-------------------+
```


## VAR_SAMP

```ppl
source=otellogs
| stats var_samp(severityNumber)
```

Expected output:

```text
fetched rows / total rows = 1/1
+-------------------------+
| var_samp(severityNumber) |
|-------------------------|
| 49.16129032258065       |
+-------------------------+
```


## VAR_POP

```ppl
source=otellogs
| stats var_pop(severityNumber)
```

Expected output:

```text
fetched rows / total rows = 1/1
+------------------------+
| var_pop(severityNumber) |
|------------------------|
| 47.625                 |
+------------------------+
```


## STDDEV_SAMP

```ppl
source=otellogs
| stats stddev_samp(severityNumber)
```

Expected output:

```text
fetched rows / total rows = 1/1
+----------------------------+
| stddev_samp(severityNumber) |
|----------------------------|
| 7.011512015564797          |
+----------------------------+
```


## STDDEV_POP

```ppl
source=otellogs
| stats stddev_pop(severityNumber)
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------------------------+
| stddev_pop(severityNumber) |
|---------------------------|
| 6.90108686941794          |
+---------------------------+
```


## DISTINCT_COUNT / DC

```ppl
source=otellogs
| stats distinct_count(severityText), dc(severityText)
```

Expected output:

```text
fetched rows / total rows = 1/1
+-----------------------------+-------------------+
| distinct_count(severityText) | dc(severityText)  |
|-----------------------------+-------------------|
| 24                          | 24                |
+-----------------------------+-------------------+
```
