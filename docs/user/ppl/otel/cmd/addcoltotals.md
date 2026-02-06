# addcoltotals (otellogs)

The `addcoltotals` command computes the sum of each column and adds a summary row showing the total.

## Example 1: Basic example

```ppl
source=otellogs
| sort @timestamp
| fields severityText, severityNumber, flags
| head 3
| addcoltotals labelfield='severityText'
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
| Total        | 39             | 2     |
+--------------+----------------+-------+
```


## Example 2: Adding column totals with custom label

```ppl
source=otellogs
| stats count() by flags
| addcoltotals `count()` label='Sum' labelfield='Total'
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+---------+-------+-------+
| count() | flags | Total |
|---------+-------+-------|
| 2       | 1     | null  |
| 30      | 0     | null  |
| 32      | null  | Sum   |
+---------+-------+-------+
```


## Example 3: Using all options with stats

```ppl
source=otellogs
| where severityNumber > 10
| stats avg(severityNumber) as avg_sev, count() as count by flags
| head 3
| addcoltotals avg_sev, count label='Sum' labelfield='Column Total'
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+---------+-------+-------+--------------+
| avg_sev | count | flags | Column Total |
|---------+-------+-------+--------------|
| 17.0    | 1     | 1     | null         |
| 16.0    | 15    | 0     | null         |
| 33.0    | 16    | null  | Sum          |
+---------+-------+-------+--------------+
```

