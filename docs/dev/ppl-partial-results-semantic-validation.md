# PPL Partial Results 语义验证报告

日期：2026-09-16

## 1. 验证目标

本报告验证 partial response 对用户是否有意义，而不只是验证运行期间是否返回了非空
`datarows`。

针对五类 query，采用下面的语义标准：

| Partial 模型 | 有意义的判定标准 |
|---|---|
| `FINAL_PREFIX` | 每个已发布 row 都不可再修改，并且在相同 offset 上与 final 完全一致 |
| `PROVISIONAL_WITH_COVERAGE` | Snapshot 可以被修改，但必须同时提供可解释、单调且小于 100% 的 coverage/progress |
| `FINAL_PAGE_PREFIX` | 每个完成的 page 都是 final result 的精确前缀 |
| `PROGRESS_ONLY` | 没有 partial rows 时，运行期间必须提供可解释的 progress |

共同 gate：

1. first meaningful latency 必须小于 final latency 的 80%；
2. running response 不能宣称 100%；
3. final response 必须为 100%；
4. 有同步对照的 query，async final 必须与同步 response 完全一致；
5. 不允许把 first non-empty 直接当成 first meaningful。

## 2. 测试环境

| 项目 | 值 |
|---|---:|
| OpenSearch node | 1 |
| JVM heap | 16 GiB |
| `ppl_async_agg_demo_00` | 5,000,000 documents，12 primary shards |
| `ppl_async_agg_demo_*` | 120,000,000 documents，288 primary shards |
| Composite page size | 1,000 buckets |
| Artificial delay | 无 |

## 3. 总结

| Query 类型 | Partial 模型 | First non-empty | First meaningful | Final | 结论 |
|---|---|---:|---:|---:|---|
| Non-blocking REX | `FINAL_PREFIX` | 329ms | 329ms | 11.621s | **PASS** |
| Fully pushed aggregation | `PROVISIONAL_WITH_COVERAGE` | 1.176s | 1.176s | 22.474s | **PASS** |
| Composite aggregation | `FINAL_PAGE_PREFIX` | 1.362s | 1.362s | 9.753s | **PASS** |
| Incremental Eventstats | `PROVISIONAL_WITH_COVERAGE` | 235ms | 无 | 72.168s | **FAIL** |
| Unsupported finite Window | `PROGRESS_ONLY` | 无 | 无 | 16.908s | **FAIL** |

因此，当前 PoC 不能得出“所有 query type 都支持 meaningful partial results”的结论。

## 4. Non-blocking REX

Query：

```text
source=ppl_async_agg_demo_00
| rex field=email "(?<user>[^@]+)@(?<domain>.+)"
| fields event_id, email, user, domain
| head 4000000
```

验证内容：

1. Listener update mode 为 `APPEND`；
2. 在 31 个不同 sequence 上，从当前结果尾部抽取新追加的 rows；
3. Query 完成后，在相同 offset 重新读取 final rows；
4. 31 个窗口全部与 final 完全一致；
5. 所有观察到的 `user` 和 `domain` 都重新使用 email 计算并验证。

结果：

```text
first meaningful = 329ms
final            = 11.621s
ratio            = 2.83%
sampled windows  = 31/31 match final
```

结论：**PASS**。这些 rows 是真正稳定、可立即展示的 final prefix。

## 5. Fully pushed aggregation

Query：

```text
source=ppl_async_agg_demo_*
| stats sum(event_id % 1000) as sum_mod_1000,
        avg(event_id % 997) as avg_mod_997,
        max(event_id % 991) as max_mod_991,
        min(event_id % 983) as min_mod_983
```

Partial reduce snapshot 会随着更多 shard 完成而改变，因此使用
`PROVISIONAL_WITH_COVERAGE`。

验证内容：

1. Listener update mode 为 `REPLACE`；
2. 首个非空 snapshot 同时带有 `fraction_done=0.020833`；
3. 运行期间 progress 单调且始终小于 1；
4. 观察到 32 个不同 aggregation states；
5. 每个 aggregate value 都满足类型和值域约束；
6. async final 与同步 response 完全一致。

结果：

```text
first meaningful    = 1.176s
first coverage      = 2.08%
final               = 22.474s
ratio               = 5.23%
synchronous         = 21.129s
distinct snapshots  = 32
```

结论：**PASS**。Snapshot 是 provisional result，但 coverage 使用户能够正确理解其完成程度。
这里的 progress 是 shard-work coverage，不是精确 document percentage。

## 6. Composite aggregation

Query：

```text
source=ppl_async_agg_demo_*
| stats count() as event_count by group_id
```

验证内容：

1. Listener update mode 为 `REPLACE`；
2. 完整捕获 10 个 running snapshots；
3. Snapshot totals 严格为 `1000, 2000, ..., 10000`；
4. 每个 snapshot 的全部 rows 都逐行等于 final 的相同前缀；
5. 每个 bucket 的确定性期望为 `event_count=12000`；
6. async final 与同步 response 完全一致。

结果：

```text
first meaningful = 1.362s
final            = 9.753s
ratio            = 13.97%
pages verified   = 10/10
```

结论：**PASS**。虽然运行期间 progress 是 indeterminate，但 completed page 中的 rows 已经是
final rows，因此不依赖 percentage 也具有明确语义。

## 7. Incremental Eventstats

Query：

```text
source=ppl_async_agg_demo_00
| sort event_id
| eventstats count() as group_count by group_id
| head 10000
```

内部 checkpoint 验证：

```text
first row group_count:
1 -> 2 -> 4 -> 8 -> 16 -> 32 -> 64 -> 128 -> 256 -> final 500
```

这组值与 deterministic input prefix 完全一致，async final 也与同步 response 完全一致。
因此 incremental Window state 和 `REPLACE` 收敛机制是正确的。

但是每个 running response 都是：

```json
{
  "progress": {
    "fraction_done": -1.0,
    "shards_total": -1,
    "shards_completed": -1
  }
}
```

结果：

```text
first non-empty = 235ms
first meaningful = none
final = 72.168s
synchronous = 16.461s
```

用户无法知道 `group_count=1`、`32` 或 `256` 分别基于多少 source input，也无法判断它们离
final state 多远。

结论：**FAIL**。当前结果只能证明 incremental producer 正确，不能证明其 partial response
对用户有意义。此外 async latency 是 sync 的约 4.38 倍，也是独立的 production blocker。

## 8. Unsupported finite Window

Query：

```text
source=ppl_async_agg_demo_00
| sort event_id
| streamstats window=100 avg(event_id) as moving_avg
| head 10000
| fields event_id, moving_avg
```

该 physical Window 不符合当前 incremental Window capability。

验证结果：

1. 所有 running responses 都没有 `datarows`；
2. 运行期间唯一 progress 为 `fraction_done=-1`；
3. final response 与同步 response 完全一致；
4. async 和 sync latency 基本相同。

```text
first meaningful = none
final            = 16.908s
synchronous      = 16.478s
```

结论：**FAIL**。Final correctness 没有问题，但运行期间既没有 partial result，也没有
meaningful progress。

## 9. Gap 与下一步

当前已经通过 meaningful partial gate：

- Non-blocking stable rows；
- Fully pushed non-composite aggregation；
- Fully pushed composite aggregation。

仍未通过：

- Incremental operator provisional snapshots；
- 没有 incremental producer 的 blocking plan。

要关闭这两个 gap：

1. Operator snapshot 必须把 `rowsConsumed` 转换成 source coverage，并随 partial response
   一起发布；
2. denominator 可以来自明确的 source limit、低成本 source estimate 或动态估算，但必须
   单调且严格不超过 100%；
3. 没有 partial producer 的 blocking plan 仍需独立的 source progress；
4. Eventstats snapshot materialization 和 change propagation 必须优化到接近同步 query
   的执行成本；
5. Fully pushed aggregation callback 需要 coalescing，避免过高的 job-state 更新频率。

## 10. Reproduce

执行脚本：

```bash
python3 scripts/ppl-partial-results-semantic-validation.py
```

原始 submit、每个 sequence response、同步 response 和 validation：

```text
build/reports/ppl-partial-results-semantic-validation/
```

机器可读总结：

```text
build/reports/ppl-partial-results-semantic-validation/summary.json
```

本报告覆盖的是上面五个具体 physical query shapes。它没有把一个 Eventstats case 外推为所有
Incremental Aggregate、Window、Dedup、TopK 及其任意组合都已通过。
