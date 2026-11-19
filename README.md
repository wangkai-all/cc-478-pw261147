# pointwise-delivery 事实仓库

带**日 / 周 / 月**三级汇总的事实仓库。底层更正沿层级自动传播,跨月边界与时区
保持一致;汇总任务可增量回补、全程可审计,失败时保留上一版可查询结果。

## 架构

```
事件摄取(幂等)                三级汇总(版本化快照 + 指针切换)
┌──────────────┐   ingest_seq   ┌─────────┐  上卷   ┌──────────┐
│ fact_events  │ ─────────────▶ │ agg_day │ ──────▶ │ agg_week │  ISO 周,可跨月
│ (event_id,   │  水位线增量     │  本地日  │ ──────▶ │ agg_month│  本地月
│  record_ver) │                └─────────┘         └──────────┘
└──────────────┘                 每层每版完整快照,agg_pointer 指向当前可查询版本
```

- **一致性根基**:周、月桶完全由"本地日"推导(同一配置时区),没有任何层级
  独立解释原始时间戳 → 跨月的 ISO 周(如 2026-W44 = 10-26~11-01)与月桶天然
  对齐,时区切换后全量重建即可保持口径一致。
- **更正传播**:事件按 `(event_id, record_version)` 版本化;更正产生新
  `ingest_seq`。增量汇总取变更事件**全部版本**涉及的桶为脏桶(覆盖更正把事件
  搬到其他日期/分区的情况),重算脏桶后沿 日→周→月 上卷。
- **失败保留旧版**:快照写入 + 核对 + 指针切换在同一事务;任何异常(含核对不
  符、演练注入失败)整体回滚,指针不动,上一版仍可查询。层级滞后(如 week 失
  败)会在下次运行自动补跑追平。
- **可审计**:`agg_run`(水位线区间/血缘/状态)、`agg_run_bucket`(每桶旧值
  →新值)、`agg_run_recon`(控制总额核对)、`ingest_log`(迟到/重复/冲突留痕)。

## 运行

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python scripts/seed_demo.py        # 演示数据:迟到/更正/跨月搬移/重复回补/失败演练
.venv/bin/python -m warehouse.app            # http://127.0.0.1:8765/
.venv/bin/python -m pytest tests/ -q         # 14 个测试
```

## 控制台页面

- **一致性总览**:事件层与三级当前版本的控制总额、水位线、一致徽标
- **层级依赖**:day→week/month 血缘连线(红色虚线 = 层级滞后待补跑),点击 run 看影响
- **汇总数据**:日/周/月切换 + 版本下拉切换(历史版本可查询)
- **更正影响**:选中 run 的桶级 旧值→新值(Δ 红绿)与水位线区间贡献事件
- **核对值**:每个 run 的 事件层 vs 本层 总额比对
- **摄取日志**:迟到 / 更正 / 重复回补 / 异常(stale·conflict·error)过滤

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/state` | 配置、指针、run 列表、核对、一致性、摄取日志 |
| GET | `/api/agg?level=day&run_id=` | 某层某版本汇总数据 + 版本列表 |
| GET | `/api/runs/<id>/impact` | 更正影响:桶级旧→新 + 贡献事件 |
| POST | `/api/ingest` | `{events:[{event_id, record_version, occurred_at, region, amount, qty}]}` |
| POST | `/api/aggregate` | `{full_rebuild?, simulate_failure?: day\|week\|month}` |
| POST | `/api/config/tz` | `{tz}` 切换时区并全量重建 |

摄取动作:`insert` 新增 · `correction` 更正(更高版本)· `duplicate` 重复回补
(幂等空操作)· `stale` 过旧版本 · `conflict` 同版本不同内容 · `error` 非法载荷。
事件日期已被当前 day 版本覆盖时标记 `is_late`。

## 关键设计

- 金额一律存**整数分**,核对值精确可比。
- 增量 = 水位线 `(wm_from, wm_to]` + 脏桶重算;重复回补后无脏数据 → 空跑
  (noop)run,不触碰任何桶。
- 单写者假设:`run_all` 入口会把遗留 running run 标记为 interrupted。
