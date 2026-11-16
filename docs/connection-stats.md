# 连接统计与最近关闭的连接

内核可以按“来源、进程、目标、网络、规则、链路”的组合，按 UTC 小时统计连接数与流量（连接统计），并在内存中保留最近关闭的连接。两者都只在内存中，由 GUI 定时读取、落盘。

## 启用

```yaml
connection-stats: true   # 默认 false
```

- 只决定是否**计数**：关闭后已有的数据不再增加，但照常保留到过期，仍可读取。
- 运行中可以经 `PATCH /configs` 改：`{"connection-stats": true}`；`GET /configs` 返回当前值。配置重载时按配置的值。
- 开启时已经存在的连接，从开启后的下一次传输开始计入，不计为新建连接。
- 最近关闭的连接不受这个开关影响，总是记录。

## 统计口径

- 计入用户流量的出口连接（TCP、UDP），字节与 `/connections` 中每个连接的上传、下载同口径；经 tunnel 发出的内核内部请求同样计入（入站类型为 `Inner`）。
- 不计 `dialer-proxy` 的中转连接与节点自身的复用传输（这些字节已包含在它们承载的连接中），以及按需探测建立的连接。
- 统计键：入站类型、入站名称、入站用户、来源 IP；进程路径（取不到时为进程名）；目标（主机，没有时为嗅探主机，都没有时为目标 IP，并标出 `targetIsIP`，不含端口）；网络；规则类型与规则内容；出口节点的 provider 与链路 `chain`（出口节点在前，其后是各层组）。
- 字节记入它流动的那个小时；连接数是该小时**新建**的连接数。
- 保留最近 72 小时，最多 20000 个键；超过后新键计入各字段都为 `*overflow*` 的溢出条目，响应带 `overflow: true`。

## `GET /usage/connections`

参数 `since`、`until` 为 UTC 小时（Unix 时间 / 3600，闭区间），默认为保留的最早小时与当前小时。

```json
{
  "instance": "<本次运行的标识，与 /usage/proxies 相同>",
  "enabled": true,
  "counting": { "on": [[492990, 492999]], "partial": [[493000, 493000]] },
  "tick": 81234,
  "startedAt": "…", "hour": 493000, "settled": 492999, "retainedSince": 492928,
  "overflow": false,
  "entries": [
    {
      "inType": "Tun", "inName": "", "inUser": "", "srcIP": "198.18.0.1",
      "process": "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
      "target": "www.google.com", "targetIsIP": false, "network": "tcp",
      "rule": "DomainSuffix", "rulePayload": "google.com",
      "provider": "", "chain": ["JP 01", "MyProxy"],
      "buckets": [{ "hour": 492998, "up": 1024, "down": 40960, "conns": 12 }]
    }
  ]
}
```

- `settled` 及之前的小时已经封桶，不再变化；之后的小时还会增长。同一 `instance` 内，同一条目同一小时的值只增不减。
- `counting`：所请求区间内（保留期内、本次运行以来）各小时的计数状态。`on` 为整个小时都在计数，`partial` 为有时计数、有时没有，未列出的小时没有计数。
- **增量读取**：`tick` 是内核开始读取之前的时钟（每秒加一，不随系统时间回拨）。带 `touchedSince=<上一次的 tick − 1>` 时只返回此后计过数的条目，每个条目仍给出所请求区间内的全部小时。增量读取用于刷新尚未封桶的小时；已封桶的小时应不带 `touchedSince` 完整读取一次。

## `GET /connections/closed`

参数 `after`（序号，默认 0）、`limit`（默认 1000），按序号升序返回序号大于 `after` 的记录。

```json
{
  "instance": "…", "startedAt": "…",
  "oldest": 120, "latest": 245,
  "evictedUntil": null,
  "truncated": false,
  "records": [
    { "id": "…", "metadata": { … }, "upload": 1024, "download": 40960, "start": "…",
      "chains": ["JP 01", "MyProxy"], "providerChains": ["", ""], "rule": "…", "rulePayload": "…",
      "seq": 245, "closedAt": "…" }
  ]
}
```

- 记录的字段与 `/connections` 中的连接相同，另带 `seq` 与 `closedAt`。
- 字节是该连接自身的计数：关闭时正在进行、之后才返回的读写也会计入，这时记录会重新排到末尾、取得新的序号，按序号继续读取的客户端会再次读到它，应按 `id` 用新值替换。
- 保留最近 31 分钟、最多 10000 条，超过任一限制丢弃最旧的。`evictedUntil` 是因容量被丢弃的记录中最晚的关闭时间：关闭时间晚于它（且在 31 分钟内）的连接都还在。`truncated` 表示 `after` 之后有记录已被丢弃。
- 同样不记中转连接与复用传输。

`/connections` 的快照另带 `instance`，可据此确认它与关闭记录来自同一次运行。
