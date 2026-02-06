# IP Address Functions (otellogs)

## CIDRMATCH

The `cidrmatch` function checks if an IP address is within a specified CIDR range.

```ppl
source=otellogs
| where isnotnull(`attributes.client.ip`)
| where cidrmatch(`attributes.client.ip`, '192.168.0.0/16')
| fields `attributes.client.ip`, severityText, body
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------------------+--------------+----------------------------------------------------------------------------------+
| attributes.client.ip | severityText | body                                                                             |
|---------------------+--------------+----------------------------------------------------------------------------------|
| 192.168.1.1         | DEBUG        | 192.168.1.1 - - [15/Jan/2024:10:30:03 +0000] "GET /api/products?search=laptop&category=electronics HTTP/1.1" 200 1234 "-" "Mozilla/5.0" |
+---------------------+--------------+----------------------------------------------------------------------------------+
```

Example with different CIDR range:

```ppl
source=otellogs
| where isnotnull(`attributes.client.ip`)
| eval is_internal = cidrmatch(`attributes.client.ip`, '10.0.0.0/8')
| fields `attributes.client.ip`, is_internal
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------------------+-------------+
| attributes.client.ip | is_internal |
|---------------------+-------------|
| 192.168.1.1         | False       |
+---------------------+-------------+
```
