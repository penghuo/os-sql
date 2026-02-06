# rare (otellogs)

The `rare` command identifies the least common combination of values across all fields specified in the field list.

## Example 1: Find the least common values without showing counts

The following query uses the `rare` command with `showcount=false` to find the least common flags without displaying frequency counts:

```ppl
source=otellogs
| rare showcount=false flags
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+-------+
| flags |
|-------|
| 1     |
| 0     |
+-------+
```


## Example 2: Find the least common values grouped by field

The following query uses the `rare` command with a `by` clause to find the least common severityNumber values grouped by flags:

```ppl
source=otellogs
| rare showcount=false severityNumber by flags
```

The query returns the following results:

```text
fetched rows / total rows = 12/12
+-------+----------------+
| flags | severityNumber |
|-------+----------------|
| 1     | 9              |
| 1     | 17             |
| 0     | 1              |
| 0     | 2              |
| 0     | 3              |
| 0     | 4              |
| 0     | 5              |
| 0     | 6              |
| 0     | 7              |
| 0     | 8              |
+-------+----------------+
```


## Example 3: Find the least common values with frequency counts

The following query uses the `rare` command with default settings to find the least common flags values and display their frequency counts:

```ppl
source=otellogs
| rare flags
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+-------+-------+
| flags | count |
|-------+-------|
| 1     | 2     |
| 0     | 30    |
+-------+-------+
```


## Example 4: Customize the count field name

The following query uses the `rare` command with the `countfield` parameter to specify a custom name for the frequency count field:

```ppl
source=otellogs
| rare countfield='cnt' flags
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+-------+-----+
| flags | cnt |
|-------+-----|
| 1     | 2   |
| 0     | 30  |
+-------+-----+
```


## Example 5: Specify null value handling

The following query uses the `rare` command with `usenull=false` to exclude null values from the results:

```ppl
source=otellogs
| rare usenull=false `attributes.user.email`
```

The query returns the following results:

```text
fetched rows / total rows = 7/7
+------------------------+-------+
| attributes.user.email  | count |
|------------------------+-------|
| admin@company.org      | 1     |
| alice@wonderland.net   | 1     |
| grpc-user@service.net  | 1     |
| support@helpdesk.io    | 1     |
| test.user@domain.co.uk | 1     |
| user@example.com       | 1     |
| webhook@partner.com    | 1     |
+------------------------+-------+
```

The following query uses `usenull=true` to include null values in the results:

```ppl
source=otellogs
| rare usenull=true `attributes.user.email`
```

The query returns the following results:

```text
fetched rows / total rows = 8/8
+------------------------+-------+
| attributes.user.email  | count |
|------------------------+-------|
| admin@company.org      | 1     |
| alice@wonderland.net   | 1     |
| grpc-user@service.net  | 1     |
| support@helpdesk.io    | 1     |
| test.user@domain.co.uk | 1     |
| user@example.com       | 1     |
| webhook@partner.com    | 1     |
| null                   | 25    |
+------------------------+-------+
```
