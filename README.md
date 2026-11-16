# 设备网关 UTC 三班停机日报服务

接收设备网关的运行/停机状态切换事件，按 UTC 三班（00:00–08:00 夜班、
08:00–16:00 白班、16:00–24:00 晚班）生成按设备、按日期的停机区间日报。

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
| GET  | `/health` | 健康检查 |

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
- “事件插入 + 日报失效 + 版号递增”是一个事务，写入失败整体回滚，
  不会暴露混合版本或半份日报；
- 重算以 upsert 整体替换旧行，同样在单事务内完成。

## 4. 自测

```bash
pip install pytest httpx
pytest -v
```

11 个用例覆盖：修订裁决、三班计算与班初未知、跨日延续、失效重算、
事件跨日期移动、批量原子性、并发高修订、写失败回滚、分界点时刻、
无事件设备、输入校验。
