# table (otellogs)

The `table` command is an alias for the `fields` command and provides the same field selection capabilities.

## Example: Basic table command usage

```ppl
source=otellogs
| sort @timestamp
| table severityText severityNumber flags
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+----------------+-------+
| severityText | severityNumber | flags |
|--------------+----------------+-------|
| INFO         | 9              | 1     |
| ERROR        | 17             | 1     |
| WARN         | 13             | 0     |
| DEBUG        | 5              | 0     |
+--------------+----------------+-------+
```
