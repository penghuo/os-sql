# ClickBench Baselines — c6a.4xlarge

## Sources
- **ClickHouse-Parquet**: https://raw.githubusercontent.com/ClickHouse/ClickBench/main/clickhouse-parquet/results/c6a.4xlarge.json
- **Elasticsearch**: https://raw.githubusercontent.com/ClickHouse/ClickBench/main/elasticsearch/results/c6a.4xlarge.json

## Metadata

| | ClickHouse (Parquet, single) | Elasticsearch |
|---|---|---|
| Date | 2026-03-09 | 2025-09-08 |
| Machine | c6a.4xlarge | c6a.4xlarge |
| Load Time (s) | 0 | 9531 |
| Data Size (bytes) | 14,779,976,446 | 85,859,504,096 |
| Tags | C++, column-oriented, embedded, stateless, ClickHouse derivative | Java, search, lukewarm-cold-run |

## ClickHouse-Parquet Results (all 43 queries)

Each entry is `[cold, hot1, hot2]` in seconds.

```
result[0]  = [0.036, 0.025, 0.025]
result[1]  = [0.238, 0.068, 0.063]
result[2]  = [0.325, 0.105, 0.106]
result[3]  = [0.383, 0.116, 0.111]
result[4]  = [0.573, 0.434, 0.435]
result[5]  = [0.905, 0.702, 0.69]
result[6]  = [0.235, 0.069, 0.065]
result[7]  = [0.201, 0.059, 0.064]
result[8]  = [0.729, 0.54, 0.54]
result[9]  = [1.202, 0.614, 0.622]
result[10] = [0.593, 0.251, 0.248]
result[11] = [0.597, 0.271, 0.268]
result[12] = [0.928, 0.644, 0.653]
result[13] = [2.217, 0.956, 0.967]
result[14] = [1.01, 0.735, 0.751]
result[15] = [0.671, 0.528, 0.52]
result[16] = [2.628, 1.801, 1.794]
result[17] = [2.004, 1.17, 1.172]
result[18] = [5.18, 3.668, 3.668]
result[19] = [0.299, 0.11, 0.114]
result[20] = [9.487, 1.169, 1.174]
result[21] = [11.069, 1.34, 1.334]
result[22] = [21.533, 1.983, 1.963]
result[23] = [54.51, 4.135, 4.151]
result[24] = [2.438, 0.497, 0.493]
result[25] = [0.739, 0.272, 0.29]
result[26] = [2.453, 0.499, 0.494]
result[27] = [9.605, 1.795, 1.79]
result[28] = [9.734, 9.526, 9.531]
result[29] = [0.269, 0.102, 0.096]
result[30] = [2.31, 0.779, 0.778]
result[31] = [5.842, 1.095, 1.1]
result[32] = [6.471, 4.541, 4.493]
result[33] = [10.53, 3.107, 3.113]
result[34] = [10.542, 3.107, 3.123]
result[35] = [0.507, 0.37, 0.37]
result[36] = [0.359, 0.126, 0.121]
result[37] = [0.319, 0.102, 0.102]
result[38] = [0.371, 0.07, 0.086]
result[39] = [0.439, 0.143, 0.147]
result[40] = [0.297, 0.078, 0.071]
result[41] = [0.27, 0.069, 0.062]
result[42] = [0.237, 0.054, 0.057]
```

## Elasticsearch Results (all 43 queries)

Each entry is `[cold, hot1, hot2]` in seconds. `null` = query not supported/failed.

```
result[0]  = [0.088, 0.002, 0.003]
result[1]  = [0.162, 0.005, 0.004]
result[2]  = [0.775, 0.615, 0.587]
result[3]  = [1.904, 0.272, 0.294]
result[4]  = [0.326, 0.335, 0.287]
result[5]  = [1.43, 0.557, 0.517]
result[6]  = [0.317, 0.302, 0.317]
result[7]  = [0.96, 0.862, 0.865]
result[8]  = [9.273, 8.909, 8.773]
result[9]  = [13.188, 12.709, 12.939]
result[10] = [1.608, 1.142, 1.143]
result[11] = [1.802, 1.522, 1.531]
result[12] = [4.371, 4.215, 4.216]
result[13] = [4.244, 4.254, 4.211]
result[14] = [4.647, 4.43, 4.537]
result[15] = [0.845, 0.805, 0.877]
result[16] = [8.333, 8.709, 8.756]
result[17] = [8.684, 8.546, 8.62]
result[18] = [19.984, 20.234, 19.957]
result[19] = [0.135, 0.004, 0.003]
result[20] = [27.196, 14.498, 14.516]
result[21] = [19.142, 17.073, 17.127]
result[22] = [37.757, 26.757, 26.835]
result[23] = [14.545, 14.498, 14.489]
result[24] = [0.849, 0.778, 0.776]
result[25] = [1.652, 1.622, 1.627]
result[26] = [0.782, 0.775, 0.779]
result[27] = [89.371, 85.607, 85.682]
result[28] = [null, null, null]
result[29] = [120.908, 127.011, 128.136]
result[30] = [3.199, 1.638, 1.627]
result[31] = [5.097, 1.73, 1.679]
result[32] = [5.747, 5.988, 5.623]
result[33] = [13.781, 12.778, 12.715]
result[34] = [12.657, 12.604, 12.722]
result[35] = [23.429, 23.336, 23.306]
result[36] = [8.353, 8.216, 8.196]
result[37] = [6.981, 6.46, 6.447]
result[38] = [8.32, 8.169, 8.095]
result[39] = [13.614, 12.761, 12.76]
result[40] = [0.109, 0.026, 0.028]
result[41] = [0.059, 0.021, 0.021]
result[42] = [0.254, 0.165, 0.164]
```
