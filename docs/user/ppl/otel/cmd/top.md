# top (otellogs)

The `top` command finds the most common combination of values across all fields specified in the field list.

## Example 1: Display counts in the default count column

The following query finds the most common severityText values:

```ppl
source=otellogs
| top severityText
```

By default, the `top` command automatically includes a `count` column showing the frequency of each value:

```text
fetched rows / total rows = 10/10
+--------------+-------+
| severityText | count |
|--------------+-------|
| INFO         | 7     |
| ERROR        | 2     |
| WARN         | 2     |
| DEBUG        | 2     |
| TRACE        | 1     |
| TRACE2       | 1     |
| TRACE3       | 1     |
| TRACE4       | 1     |
| DEBUG2       | 1     |
| DEBUG3       | 1     |
+--------------+-------+
```


## Example 2: Find the most common values without the count display

The following query uses `showcount=false` to hide the `count` column in the results:

```ppl
source=otellogs
| top showcount=false flags
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+-------+
| flags |
|-------|
| 0     |
| 1     |
+-------+
```

## Example 3: Rename the count column

The following query uses the `countfield` parameter to specify a custom name (`cnt`) for the count column instead of the default `count`:

```ppl
source=otellogs
| top countfield='cnt' flags
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+-------+-----+
| flags | cnt |
|-------+-----|
| 0     | 30  |
| 1     | 2   |
+-------+-----+
```

## Example 4: Limit the number of returned results

The following query returns the top 1 most common severityText value:

```ppl
source=otellogs
| top 1 showcount=false severityText
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------------+
| severityText |
|--------------|
| INFO         |
+--------------+
```


## Example 5: Group the results

The following query uses the `by` clause to find the most common severityNumber within each flags group and show it separately for each flags value:

```ppl
source=otellogs
| top 1 showcount=false severityNumber by flags
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+-------+----------------+
| flags | severityNumber |
|-------+----------------|
| 0     | 13             |
| 1     | 9              |
+-------+----------------+
```

## Example 6: Specify null value handling

The following query specifies `usenull=false` to exclude null values:

```ppl
source=otellogs
| top usenull=false `attributes.user.email`
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

The following query specifies `usenull=true` to include null values in the results:

```ppl
source=otellogs
| top usenull=true `attributes.user.email`
```

The query returns the following results:

```text
fetched rows / total rows = 8/8
+------------------------+-------+
| attributes.user.email  | count |
|------------------------+-------|
| null                   | 25    |
| admin@company.org      | 1     |
| alice@wonderland.net   | 1     |
| grpc-user@service.net  | 1     |
| support@helpdesk.io    | 1     |
| test.user@domain.co.uk | 1     |
| user@example.com       | 1     |
| webhook@partner.com    | 1     |
+------------------------+-------+
```
