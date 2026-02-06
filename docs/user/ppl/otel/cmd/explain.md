# explain (otellogs)

The `explain` command displays the execution plan of a query.

## Example 1: Explain a PPL query

```ppl
explain source=otellogs
| where severityNumber > 15
| stats count() by severityText
```

The query returns execution plan results showing logical and physical plans.


## Example 2: Explain with simple mode

```ppl
explain simple source=otellogs
| where flags = 1
| stats count() by severityText
```

The query returns simplified logical plan output showing the query structure:

```json
{
  "calcite": {
    "logical": """LogicalProject
  LogicalAggregate
    LogicalFilter
      CalciteLogicalIndexScan
"""
  }
}
```


## Example 3: Explain aggregation query

```ppl
explain source=otellogs
| stats avg(severityNumber), sum(severityNumber) by flags
```

The query returns execution plan showing aggregation operations.

