# gewe-gateway

`gewe-gateway` 是给 `gewe-openclaw` 准备的单机网关。

它的定位很明确：

- 一个网关实例只服务一个 GeWe 账号
- 一个 GeWe 官方 webhook 只打到这个网关
- 多台 `gewe-openclaw` 通过 `gatewayUrl + gatewayKey` 注册自己负责的群
- 网关按群转发入站消息，并把所有出站请求串行发回同一个微信号

这版目标是个人生产可用，适合：

- 10 台以内的 `gewe-openclaw`
- 5 个以内的微信号
- 每个微信号对应一个单独部署的 `gewe-gateway`

## 特性

- GeWe webhook 入站分流
- OpenClaw 实例注册、续租、注销
- 群绑定冲突保护
- GeWe 兼容发送和下载代理
- SQLite 持久化出站队列
- 启动恢复 `running` 任务
- 瞬时错误重试和死信重入队
- 入站去重表和出站历史自动清理
- `healthz`、`readyz`、队列统计接口

## 配置

可以通过 JSON 配置文件或环境变量配置。最小样例见 [config.example.json](/Users/wangnov/gewe-gateway/config.example.json)。

核心配置项：

- `listenAddr`
- `databasePath`
- `gatewayKey`
- `geweApiBaseUrl`
- `geweToken`
- `geweAppId`
- `geweWebhookSecret`
- `instanceLeaseTtl`
- `forwardTimeout`
- `outboundMaxAttempts`
- `outboundBaseBackoff`
- `outboundMaxBackoff`
- `workerPollInterval`
- `inboundDedupeRetention`
- `outboundHistoryRetention`
- `maintenanceInterval`

对应环境变量：

- `GEWE_GATEWAY_LISTEN_ADDR`
- `GEWE_GATEWAY_DATABASE_PATH`
- `GEWE_GATEWAY_KEY`
- `GEWE_GATEWAY_GEWE_API_BASE_URL`
- `GEWE_GATEWAY_GEWE_TOKEN`
- `GEWE_GATEWAY_GEWE_APP_ID`
- `GEWE_GATEWAY_GEWE_WEBHOOK_SECRET`
- `GEWE_GATEWAY_INSTANCE_LEASE_TTL`
- `GEWE_GATEWAY_FORWARD_TIMEOUT`
- `GEWE_GATEWAY_OUTBOUND_MAX_ATTEMPTS`
- `GEWE_GATEWAY_OUTBOUND_BASE_BACKOFF`
- `GEWE_GATEWAY_OUTBOUND_MAX_BACKOFF`
- `GEWE_GATEWAY_WORKER_POLL_INTERVAL`
- `GEWE_GATEWAY_INBOUND_DEDUPE_RETENTION`
- `GEWE_GATEWAY_OUTBOUND_HISTORY_RETENTION`
- `GEWE_GATEWAY_MAINTENANCE_INTERVAL`

默认值：

- `geweApiBaseUrl = https://www.geweapi.com`
- `instanceLeaseTtl = 5m`
- `forwardTimeout = 10s`
- `outboundMaxAttempts = 3`
- `outboundBaseBackoff = 2s`
- `outboundMaxBackoff = 30s`
- `workerPollInterval = 500ms`
- `inboundDedupeRetention = 168h`
- `outboundHistoryRetention = 336h`
- `maintenanceInterval = 1h`

## 运行

```bash
go build ./cmd/gewe-gateway
./gewe-gateway -config ./config.example.json
```

建议部署方式：

- 单机部署
- 本地 SSD 或稳定块存储
- 给 SQLite 文件单独目录并定期备份
- 前面挂 Nginx、Caddy 或 Cloudflare Tunnel 暴露 HTTPS

## GeWe webhook

把 GeWe 官方 webhook 指向：

```text
https://your-gateway.example.com/gateway/v1/webhooks/gewe
```

鉴权使用：

- Header `X-GEWE-CALLBACK-TOKEN`
- 或 GeWe 兼容的 `X-WebHook-Token`
- 或查询参数 `?token=...`

值应与 `geweWebhookSecret` 相同。

## OpenClaw 插件接入

`gewe-openclaw` 配置网关模式时，需要提供：

- `gatewayUrl`
- `gatewayKey`
- `gatewayInstanceId`
- `webhookPublicUrl`
- 显式 `groups`

网关不会接受“抢全部群”的通配绑定。一个群同一时间只能被一个实例持有。

## 运维接口

- `GET /gateway/v1/healthz`
- `GET /gateway/v1/readyz`
- `GET /gateway/v1/queue/stats`
- `POST /gateway/v1/queue/dead/requeue`

其中 `queue/stats` 和 `dead/requeue` 需要带：

```text
X-GeWe-Gateway-Key: <gatewayKey>
```

## 已知语义

当前出站保证的是“持久化队列 + 重试 + 至少一次交付”。

这意味着在极窄窗口里，如果：

- GeWe 已经成功收到了消息
- 但网关还没来得及把任务标记为 `succeeded`
- 此时网关进程崩溃

重启后这条任务可能会再次发送。

对个人生产场景，这通常是可接受的；如果后面要继续收紧重复发送风险，可以再引入更强的上游幂等协作。
