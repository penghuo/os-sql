# Condition Functions (otellogs)

## ISNULL

```ppl
source=otellogs
| sort @timestamp
| eval result = isnull(`attributes.user.email`)
| fields result, `attributes.user.email`, severityText
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+--------+-----------------------+--------------+
| result | attributes.user.email | severityText |
|--------+-----------------------+--------------|
| True   | null                  | INFO         |
| False  | user@example.com      | ERROR        |
| True   | null                  | WARN         |
| True   | null                  | DEBUG        |
+--------+-----------------------+--------------+
```

Using with if() to label records

```ppl
source=otellogs
| sort @timestamp
| eval status = if(isnull(`attributes.user.email`), 'no-email', 'has-email')
| fields severityText, `attributes.user.email`, status
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+--------------+-----------------------+-----------+
| severityText | attributes.user.email | status    |
|--------------+-----------------------+-----------|
| INFO         | null                  | no-email  |
| ERROR        | user@example.com      | has-email |
| WARN         | null                  | no-email  |
| DEBUG        | null                  | no-email  |
+--------------+-----------------------+-----------+
```

Filtering with where clause

```ppl
source=otellogs
| where isnull(`attributes.user.email`)
| sort @timestamp
| fields severityNumber, severityText, `attributes.user.email`
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+----------------+--------------+-----------------------+
| severityNumber | severityText | attributes.user.email |
|----------------+--------------+-----------------------|
| 9              | INFO         | null                  |
| 13             | WARN         | null                  |
| 5              | DEBUG        | null                  |
| 9              | INFO         | null                  |
+----------------+--------------+-----------------------+
```


## ISNOTNULL

```ppl
source=otellogs
| sort @timestamp
| eval has_email = isnotnull(`attributes.user.email`)
| fields severityText, `attributes.user.email`, has_email
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+--------------+-----------------------+-----------+
| severityText | attributes.user.email | has_email |
|--------------+-----------------------+-----------|
| INFO         | null                  | False     |
| ERROR        | user@example.com      | True      |
| WARN         | null                  | False     |
| DEBUG        | null                  | False     |
+--------------+-----------------------+-----------+
```

Filtering with where clause

```ppl
source=otellogs
| where isnotnull(`attributes.user.email`)
| sort @timestamp
| fields severityText, `attributes.user.email`
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+--------------+-----------------------+
| severityText | attributes.user.email |
|--------------+-----------------------|
| ERROR        | user@example.com      |
| INFO         | admin@company.org     |
| TRACE2       | alice@wonderland.net  |
| DEBUG2       | test.user@domain.co.uk|
+--------------+-----------------------+
```


## IFNULL

```ppl
source=otellogs
| sort @timestamp
| eval result = ifnull(`attributes.user.email`, 'default@example.com')
| fields result, `attributes.user.email`, severityText
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+---------------------+-----------------------+--------------+
| result              | attributes.user.email | severityText |
|---------------------+-----------------------+--------------|
| default@example.com | null                  | INFO         |
| user@example.com    | user@example.com      | ERROR        |
| default@example.com | null                  | WARN         |
| default@example.com | null                  | DEBUG        |
+---------------------+-----------------------+--------------+
```


## NULLIF

```ppl
source=otellogs
| sort @timestamp
| eval result = nullif(severityText, 'INFO')
| fields result, severityText
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+--------+--------------+
| result | severityText |
|--------+--------------|
| null   | INFO         |
| ERROR  | ERROR        |
| WARN   | WARN         |
| DEBUG  | DEBUG        |
+--------+--------------+
```


## IF

```ppl
source=otellogs
| sort @timestamp
| eval result = if(severityNumber > 10, 'high', 'low')
| fields result, severityNumber, severityText
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+--------+----------------+--------------+
| result | severityNumber | severityText |
|--------+----------------+--------------|
| low    | 9              | INFO         |
| high   | 17             | ERROR        |
| high   | 13             | WARN         |
| low    | 5              | DEBUG        |
+--------+----------------+--------------+
```

```ppl
source=otellogs
| sort @timestamp
| eval is_error = if(severityNumber > 15 AND isnotnull(traceId), true, false)
| fields is_error, severityNumber, traceId
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+----------+----------------+----------------------------------+
| is_error | severityNumber | traceId                          |
|----------+----------------+----------------------------------|
| False    | 9              | b3cb01a03c846973fd496b973f49be85 |
| True     | 17             | 7475a30207dbef54d29e42c37f09a528 |
| False    | 13             |                                  |
| False    | 5              |                                  |
+----------+----------------+----------------------------------+
```


## CASE

```ppl
source=otellogs
| sort @timestamp
| eval level = case(severityNumber > 20, 'CRITICAL', severityNumber > 15, 'HIGH', severityNumber > 10, 'MEDIUM' else 'LOW')
| fields level, severityNumber, severityText
| head 6
```

Expected output:

```text
fetched rows / total rows = 6/6
+----------+----------------+--------------+
| level    | severityNumber | severityText |
|----------+----------------+--------------|
| LOW      | 9              | INFO         |
| HIGH     | 17             | ERROR        |
| MEDIUM   | 13             | WARN         |
| LOW      | 5              | DEBUG        |
| LOW      | 9              | INFO         |
| CRITICAL | 21             | FATAL        |
+----------+----------------+--------------+
```


## COALESCE

```ppl
source=otellogs
| sort @timestamp
| eval result = coalesce(`attributes.user.email`, traceId, 'unknown')
| fields result, `attributes.user.email`, traceId
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+----------------------------------+-----------------------+----------------------------------+
| result                           | attributes.user.email | traceId                          |
|----------------------------------+-----------------------+----------------------------------|
| b3cb01a03c846973fd496b973f49be85 | null                  | b3cb01a03c846973fd496b973f49be85 |
| user@example.com                 | user@example.com      | 7475a30207dbef54d29e42c37f09a528 |
| unknown                          | null                  |                                  |
| unknown                          | null                  |                                  |
+----------------------------------+-----------------------+----------------------------------+
```


## ISBLANK

```ppl
source=otellogs
| sort @timestamp
| eval blank_trace = isblank(traceId)
| fields blank_trace, traceId
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+-------------+----------------------------------+
| blank_trace | traceId                          |
|-------------+----------------------------------|
| False       | b3cb01a03c846973fd496b973f49be85 |
| False       | 7475a30207dbef54d29e42c37f09a528 |
| True        |                                  |
| True        |                                  |
+-------------+----------------------------------+
```


## ISEMPTY

```ppl
source=otellogs
| sort @timestamp
| eval empty_trace = isempty(traceId)
| fields empty_trace, traceId
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+-------------+----------------------------------+
| empty_trace | traceId                          |
|-------------+----------------------------------|
| False       | b3cb01a03c846973fd496b973f49be85 |
| False       | 7475a30207dbef54d29e42c37f09a528 |
| True        |                                  |
| True        |                                  |
+-------------+----------------------------------+
```
