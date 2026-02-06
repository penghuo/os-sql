# trendline (otellogs)

The `trendline` command calculates moving averages of fields.

## Example 1: Calculate simple moving average for one field

```ppl
source=otellogs
| sort @timestamp
| trendline sma(2, severityNumber) as sn_trend
| fields severityNumber, sn_trend
| head 5
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+----------------+----------+
| severityNumber | sn_trend |
|----------------+----------|
| 9              | null     |
| 17             | 13.0     |
| 13             | 15.0     |
| 5              | 9.0      |
| 9              | 7.0      |
+----------------+----------+
```


## Example 2: Calculate simple moving average for multiple fields

```ppl
source=otellogs
| sort @timestamp
| trendline sma(3, severityNumber) as sn_trend sma(3, flags) as flags_trend
| fields severityNumber, sn_trend, flags, flags_trend
| head 5
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+----------------+--------------------+-------+---------------------+
| severityNumber | sn_trend           | flags | flags_trend         |
|----------------+--------------------+-------+---------------------|
| 9              | null               | 1     | null                |
| 17             | null               | 1     | null                |
| 13             | 13.0               | 0     | 0.6666666666666666  |
| 5              | 11.666666666666666 | 0     | 0.3333333333333333  |
| 9              | 9.0                | 0     | 0.0                 |
+----------------+--------------------+-------+---------------------+
```


## Example 3: Calculate weighted moving average

```ppl
source=otellogs
| sort @timestamp
| trendline wma(2, severityNumber) as weighted_avg
| fields severityNumber, weighted_avg
| head 5
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+----------------+--------------------+
| severityNumber | weighted_avg       |
|----------------+--------------------|
| 9              | null               |
| 17             | 14.333333333333334 |
| 13             | 14.333333333333334 |
| 5              | 7.666666666666667  |
| 9              | 7.666666666666667  |
+----------------+--------------------+
```


## Example 4: Default alias without specifying name

```ppl
source=otellogs
| sort @timestamp
| trendline sma(2, severityNumber)
| fields severityNumber, severityNumber_trendline
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------------------+
| severityNumber | severityNumber_trendline |
|----------------+--------------------------|
| 9              | null                     |
| 17             | 13.0                     |
| 13             | 15.0                     |
| 5              | 9.0                      |
+----------------+--------------------------+
```
