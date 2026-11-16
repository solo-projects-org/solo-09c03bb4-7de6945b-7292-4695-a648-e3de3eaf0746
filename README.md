# 设备网关 UTC 三班停机日报与采样核验服务

接收设备网关的运行/停机状态切换事件，按 UTC 三班（00:00–08:00 夜班、
08:00–16:00 白班、16:00–24:00 晚班）生成按设备、按日期的停机区间日报。

同时接收生产线数据员使用的网关独立上报状态采样，按设备 + UTC 日期与
生效事件时序对照，发布采样核验：列出冲突采样号、采样/事件双方状态及依据事件号。

## 1. 启动

```bash
docker compose up --build
# Swagger: http://localhost:8000/docs
# OpenAPI: http://localhost:8000/openapi.json
```

SQLite 数据文件保存在命名卷 `gateway-data`（容器内 `/data/gateway.db`），
可用环境变量 `DOWNTIME_DB_PATH` 覆盖路径。

本地裸机运行（Python 3.11）：

```bash
pip install -r requirements.txt
DOWNTIME_DB_PATH=./data/gateway.db uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 2. 接口（均在 Swagger 可直接提交）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/events` | 提交单条切换事件 |
| POST | `/events/batch` | 批量提交（整批一个事务，冲突全回滚） |
| GET  | `/devices/{device_id}/events` | 事件修订流水 |
| GET  | `/devices/{device_id}/reports/{report_date}` | 获取/按需生成日报 |
| GET  | `/devices/{device_id}/reports/{report_date}?recompute=true` | 强制重算 |
| GET  | `/reports?device_id=` | 已生成日报列表（含失效记录） |
| POST | `/samples` | 提交单条独立状态采样 |
| POST | `/samples/batch` | 批量提交采样（整批一个事务，冲突全回滚） |
| GET  | `/devices/{device_id}/samples` | 某设备采样流水 |
| GET  | `/devices/{device_id}/verifications/{check_date}` | 获取/按需发布采样核验 |
| GET  | `/devices/{device_id}/verifications/{check_date}?recompute=true` | 强制重核 |
| GET  | `/verifications?device_id=` | 已发布核验列表（含失效记录） |
| GET  | `/health` | 健康检查 |

采样报文：

```json
{
  "device_id": "GW-01",
  "sample_id": "SP-20261115-001",
  "sample_time": "2026-11-15T06:00:00Z",
  "status": "STOPPED"
}
```

`sample_time` 必须带时区（内部统一转 UTC 并按 UTC 日期归日）；`status` 取
`RUNNING`/`STOPPED`。采样号在同一设备内唯一。

事件报文：

```json
{
  "device_id": "GW-01",
  "event_id": "EV-1",
  "revision": 2,
  "event_time": "2026-11-15T05:00:00Z",
  "status": "STOPPED"
}
```

`event_time` 必须带时区（内部统一转 UTC 存储）；`status` 取 `RUNNING`/`STOPPED`。

## 3. 业务规则

### 修订号裁决（`event_revisions` 全量留痕，`latest_events` 视图取每事件最高修订）

- 高修订可以更改同一事件的时刻与状态；
- 低修订回退 → `409` 拒收；
- 同版异文（revision 相同但时刻/状态不同）→ `409` 拒收；
- 同版同文重复提交 → 幂等成功，不增加数据版号。

### 状态延续与班制

- 事件表示一次切换，状态延续到下一次切换；
- 按 UTC 00:00、08:00、16:00 分三个班，班末沿用最后状态；
- 班初没有此前状态的时段记为 `UNKNOWN`，**不计入运行也不计入停机**；
- 每个区间给出起止时刻、分钟数、状态和来源事件号（`source_event_id`）。

### 失效与重算

- 每次被接受的事件修订使该设备数据版号 +1，并在**同一事务**内把
  受影响日期范围内已生成的 `CURRENT` 日报置为 `INVALIDATED`（带失效时间与原因）；
- 事件被移出原日期时，新旧日期区间内的日报全部失效；
- 失效日报查询返回 `status=INVALIDATED, report=null`，重算后恢复 `CURRENT`，
  日报内 `data_version` 更新为重算时的设备版号。

### 并发与原子性

- SQLite WAL + `busy_timeout`，所有写操作以 `BEGIN IMMEDIATE` 串行化；
- 事件写入（事件插入 + 日报失效 + 核验失效 + 事件版号递增）与采样写入
  （采样插入 + 核验失效 + 采样版号递增）各为单事务，失败整体回滚，
  不会暴露混合版本、半份日报或半份核验；
- 重算/重核均以 upsert 整体替换旧行，同样在单事务内完成；并发核验在
  `BEGIN IMMEDIATE` 后复查两路版号，要么复用同一份 `CURRENT`，要么整份重算。

### 独立状态采样

- 同号同文重复提交幂等（`idempotent=true`，采样版号不增）；
- 同号异文（时刻或状态任一不同）→ `409` 拒收，原采样不受影响；
- 采样号只在设备内唯一，不同设备可复用；
- 每条被接受的采样使该设备采样版号 +1；批量提交整批一个事务，
  任一冲突全部回滚。

### 采样核验

按设备 + UTC 日期发布。核验不改变采样本身，输出：

- `status`：`CURRENT`（已发布）/ `INVALIDATED`（依据数据已变更，需重算）/
  `NO_SAMPLES`（该日无采样，`verified=false`，不会写入任何“已核验”行）；
- `event_version` + `sample_version`：核验所依据的**两路数据版号**，失效后
  保留旧值，重算后同时更新；
- `samples`：当日每条采样的对照结果（`MATCH` / `CONFLICT` / `UNKNOWN`），
  含 `event_status` 与 `basis_event_id` / `basis_event_time`；
- `conflicts`：仅冲突项，含采样号、采样状态、事件状态、依据事件号与依据时刻。

对照规则：

- 按生效事件（每事件最高修订）的 (UTC 时刻, 事件号) 时序：
  采样时刻之前最后一次切换的状态即应有状态；
- 首次切换之前的采样无任何依据 → `UNKNOWN`，`basis_event_id=null`，
  不计冲突；
- 采样时刻恰等于切换时刻时，采用该切换后的状态（闭区间一侧）。

失效规则：

- **采样补传/更正**：使该采样时刻所在 UTC 日期的旧核验失效，其他日期不受影响；
- **事件追溯更正**：对比更正前后的时序，只失效“状态或依据事件真正改变”
  的日期区间；跨日延续的状态被更正时，延续到下一次切换为止的后续日期
  一并失效；未受影响的日期继续 `CURRENT` 可用；
- 失效行保留旧的两路版号、失效时间与原因，但**不再返回旧对照明细**，
  查询返回 `status=INVALIDATED`、`verified=false`、空的 `samples`/`conflicts`；
  用 `?recompute=true` 按当前两路版号重算后恢复完整核验；
- 无采样日期即使强制重算也保持 `NO_SAMPLES`。

## 4. 自测

```bash
pip install pytest httpx
pytest -v
```

12 个用例覆盖：修订裁决、三班计算与班初未知、跨日延续、失效重算、
事件跨日期移动、批量原子性、并发高修订、写失败回滚、分界点时刻、
无事件设备、输入校验、并发首次生成。

采样与核验另有 15 个用例（`tests/test_samples.py`）：同号同文幂等 /
同号异文拒收 / 设备隔离 / 输入校验、批量原子性、逐条对照与冲突明细
（采样号 + 双方状态 + 依据事件号）、首次切换前 UNKNOWN、无采样日期
不发布核验、采样补传只失效当日、事件追溯更正只失效受影响日期、
跨日延续更正波及后续日期、仅推移延续事件时刻时更早日期不受影响、
事件跨日移动时新旧日期都失效、采样时刻按 UTC 归日、采样写入失败回滚、
事件写入失败不半失效核验、重算失败保留旧失效行、两路写入与重核并发、
OpenAPI 同时暴露新旧接口且旧日报接口可用。
