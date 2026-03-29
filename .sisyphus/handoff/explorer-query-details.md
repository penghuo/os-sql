# ClickBench Query Details: Q04, Q05, Q08, Q09, Q11, Q13

## Query SQL Text (from queries_trino.sql)

| Query | SQL |
|-------|-----|
| Q04 (line 4) | `SELECT AVG(UserID) FROM hits;` |
| Q05 (line 5) | `SELECT COUNT(DISTINCT UserID) FROM hits;` |
| Q08 (line 8) | `SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;` |
| Q09 (line 9) | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;` |
| Q11 (line 11) | `SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;` |
| Q13 (line 13) | `SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;` |

## ClickHouse-Parquet Baseline (c6a.4xlarge) — [cold, hot1, hot2] seconds

| Query | Index | Run 1 (cold) | Run 2 (hot) | Run 3 (hot) |
|-------|-------|-------------|-------------|-------------|
| Q04   | [3]   | 0.383       | 0.116       | 0.111       |
| Q05   | [4]   | 0.573       | 0.434       | 0.435       |
| Q08   | [7]   | 0.201       | 0.059       | 0.064       |
| Q09   | [8]   | 0.729       | 0.540       | 0.540       |
| Q11   | [10]  | 0.593       | 0.251       | 0.248       |
| Q13   | [12]  | 0.928       | 0.644       | 0.653       |

## OpenSearch Latest Results (m5.8xlarge) — [cold, hot1, hot2] seconds

| Query | Index | Run 1 (cold) | Run 2 (hot) | Run 3 (hot) |
|-------|-------|-------------|-------------|-------------|
| Q04   | [3]   | 1.945       | 1.790       | 1.805       |
| Q05   | [4]   | 2.484       | 2.432       | 2.390       |
| Q08   | [7]   | 0.697       | 0.257       | 0.292       |
| Q09   | [8]   | 4.105       | 3.495       | 3.444       |
| Q11   | [10]  | 1.482       | 0.422       | 0.332       |
| Q13   | [12]  | 1.088       | 0.572       | 0.600       |

## Hot-Run Comparison (best hot run)

| Query | ClickHouse-Parquet | OpenSearch | Ratio (OS/CH) |
|-------|-------------------|------------|---------------|
| Q04   | 0.111s            | 1.790s     | 16.1x slower  |
| Q05   | 0.434s            | 2.390s     | 5.5x slower   |
| Q08   | 0.059s            | 0.257s     | 4.4x slower   |
| Q09   | 0.540s            | 3.444s     | 6.4x slower   |
| Q11   | 0.248s            | 0.332s     | 1.3x slower   |
| Q13   | 0.644s            | 0.572s     | 0.9x (faster) |

### Key Observations
- Q13 is the only query where OpenSearch outperforms ClickHouse-Parquet
- Q04 (AVG(UserID)) has the largest gap at 16.1x
- Q11 and Q08 are relatively close (1.3x and 4.4x)
- Q05 and Q09 involve COUNT(DISTINCT UserID) and are 5-6x slower
