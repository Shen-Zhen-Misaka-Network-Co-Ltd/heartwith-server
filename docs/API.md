# Heartwith API

## 1. 创建采集会话

`POST /api/v1/collector/sessions`

Request:

```json
{
  "display_name": "Allen",
  "device_model": "Mi Band 8",
  "client_platform": "ios",
  "app_version": "1.0.0"
}
```

Response:

```json
{
  "collector_id": "col_...",
  "collector_token": "secret-token",
  "ingest_url": "http://localhost:8000/api/v1/hr/batches",
  "policy": {
    "batch_window_ms": 8000,
    "max_batch_window_ms": 8000,
    "low_power_batch_window_ms": 8000,
    "change_flush_bpm": 3,
    "offline_cache_seconds": 300
  }
}
```

## 2. 心率批量上传

`POST /api/v1/hr/batches`

Headers:

```text
Authorization: Bearer {collector_token}
Content-Type: application/cbor
```

CBOR payload 的逻辑结构：

```json
{
  "schema": 1,
  "collector_id": "col_...",
  "seq": 1024,
  "sent_at_ms": 1760000000000,
  "display_name": "Allen",
  "device_model": "Mi Band 8",
  "samples": [
    { "dt_ms": -9000, "bpm": 82 },
    { "dt_ms": -8000, "bpm": 83 },
    { "dt_ms": 0, "bpm": 85 }
  ],
  "ble": {
    "rssi": -61,
    "source": "heart_rate_service_2a37"
  }
}
```

Response:

```json
{
  "ok": true,
  "accepted": 3,
  "server_time_ms": 1760000000123,
  "next_policy": null
}
```

Rules:

- 移动端默认使用 `application/cbor`，调试时可使用 `application/json`。
- `collector_id + seq` 幂等，重复包返回 `accepted: 0`。
- `bpm` 合法范围为 `30-240`。
- 服务端会在客户端时间偏差过大时用接收时间校正。
- 稳定心率不应每秒上传，客户端应按策略批量 flush。
- 移动端离线缓存最多保留最近 `offline_cache_seconds`，恢复上传时长缓存会按约 `5s` 粒度降采样，并始终保留最新样本。

## 3. 大厅快照

`GET /api/v1/lobby/participants`

Response:

```json
{
  "server_time_ms": 1760000000123,
  "participants": [
    {
      "collector_id": "col_...",
      "display_name": "Allen",
      "device_model": "Mi Band 8",
      "status": "online",
      "last_bpm": 85,
      "last_seen_ms": 1760000000000,
      "updated_at_ms": 1760000000123
    }
  ]
}
```

Status:

- `online`: 最近 `20s` 内有心率。
- `stale`: `20-120s` 内无新心率。
- `offline`: 超过 `120s`。

## 4. 大厅实时推送

`GET /api/v1/lobby/events`

Transport: Server-Sent Events.

Event:

```json
{
  "type": "participant_update",
  "participant": {
    "collector_id": "col_...",
    "display_name": "Allen",
    "device_model": "Mi Band 8",
    "status": "online",
    "last_bpm": 85,
    "last_seen_ms": 1760000000000,
    "updated_at_ms": 1760000000123
  }
}
```

## 5. 最近曲线

`GET /api/v1/participants/{collector_id}/series?window_seconds=300&max_points=420`

Query:

- `window_seconds`: `60` 到 `86400`，默认 `300`。
- `max_points`: `60` 到 `1200`，默认 `420`。服务端会按时间桶聚合，避免长时间范围返回过多点。

Response:

```json
{
  "collector_id": "col_...",
  "window_seconds": 300,
  "samples": [
    { "t_ms": 1760000000000, "bpm": 82 },
    { "t_ms": 1760000010000, "bpm": 85 }
  ]
}
```
