# addtotals (otellogs)

The `addtotals` command computes the sum of numeric fields and can create both column totals (summary row) and row totals (new field).

## Example 1: Basic example with column totals

```ppl
source=otellogs
| sort @timestamp
| head 3
| fields severityText, severityNumber, flags
| addtotals col=true labelfield='severityText' label='Total'
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+----------------+-------+-------+
| severityText | severityNumber | flags | Total |
|--------------+----------------+-------+-------|
| INFO         | 9              | 1     | 10    |
| ERROR        | 17             | 1     | 18    |
| WARN         | 13             | 0     | 13    |
| Total        | 39             | 2     | null  |
+--------------+----------------+-------+-------+
```


## Example 2: Column totals without row totals

```ppl
source=otellogs
| sort @timestamp
| fields severityText, severityNumber, flags
| head 4
| addtotals col=true row=false label='Sum' labelfield='Total'
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+--------------+----------------+-------+-------+
| severityText | severityNumber | flags | Total |
|--------------+----------------+-------+-------|
| INFO         | 9              | 1     | null  |
| ERROR        | 17             | 1     | null  |
| WARN         | 13             | 0     | null  |
| DEBUG        | 5              | 0     | null  |
| null         | 44             | 2     | Sum   |
+--------------+----------------+-------+-------+
```


## Example 3: Using all options

```ppl
source=otellogs
| where severityNumber > 10
| stats avg(severityNumber) as avg_sev, count() as count by flags
| head 3
| addtotals avg_sev, count row=true col=true fieldname='Row Total' label='Sum' labelfield='Column Total'
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+---------+-------+-------+-----------+--------------+
| avg_sev | count | flags | Row Total | Column Total |
|---------+-------+-------+-----------+--------------|
| 17.0    | 1     | 1     | 18.0      | null         |
| 16.0    | 15    | 0     | 31.0      | null         |
| 33.0    | 16    | null  | null      | Sum          |
+---------+-------+-------+-----------+--------------+
```

