# FastAPI Upstream Gateway

一个本地可运行的请求网关：请求可以转发到一组可注册、可注销的上游节点，
并按节点健康状况做负载均衡。全部环节只使用本地进程与本地端口，不依赖任何外部服务或凭据。

## 启动

```bash
uv sync
uv run python main.py        # 与原启动方式一致（uvicorn, 127.0.0.1）
# 或
uv run uvicorn main:app --port 8000
```

原有接口 `GET /` 保持不变，返回 `{"Hello": "World"}`。

## 管理接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/admin/upstreams` | 注册节点，body: `{"name": "a", "url": "http://127.0.0.1:9001"}` |
| `DELETE` | `/admin/upstreams/{name}` | 注销节点 |
| `GET` | `/admin/upstreams` | 查询所有节点状态、在途计数与最近一次判定原因 |
| `POST` | `/admin/upstreams/check` | 立即执行一轮健康探测（不等待周期） |

拒绝语义按类型区分：

- 重复注册 → `409 {"error": "node_already_exists"}`
- 注销/查询不存在的节点 → `404 {"error": "node_not_found"}`

除上述管理接口与 `/` 之外的所有请求路径都会被转发到上游节点。

## 健康判定与恢复

- 后台任务每 `GATEWAY_HEALTH_INTERVAL` 秒对每个节点探测一次 `GET <url><GATEWAY_HEALTH_PATH>`。
- 状态翻转需要**连续**结果达到阈值，阈值以内不改变状态，避免抖动误判：
  - 连续失败 `GATEWAY_FAILURE_THRESHOLD` 次 → 标记为不健康；
  - 连续成功 `GATEWAY_SUCCESS_THRESHOLD` 次 → 恢复健康。
- 探测结果（2xx/3xx/4xx 视为成功；5xx、超时、连接失败视为失败）会重置另一侧的连续计数。
- 每个节点的 `healthy`、`consecutive_failures/successes`、`last_reason`（最近一次判定原因）
  均可通过 `GET /admin/upstreams` 查询。
- 不健康节点不会被转发任何请求；没有健康节点时返回
  `503 {"error": "no_healthy_upstream"}`。

## 负载均衡策略

由 `GATEWAY_STRATEGY` 配置：

- `round_robin`（默认）：在健康节点间依次轮询。
- `least_in_flight`：选择在途请求数最少的健康节点，并列时轮转。

在途计数在选节点时于注册表锁内递增、请求结束后递减，并发下不会超发也不会不回落。

## 超时与重试语义

- 每次转发尝试使用 `GATEWAY_REQUEST_TIMEOUT` 秒超时。
- 仅对幂等方法（`GET/HEAD/OPTIONS/PUT/DELETE`）重试，最多额外尝试
  `GATEWAY_MAX_RETRIES` 次；**每次重试必定换到不同的健康节点**。
- 非幂等方法（如 `POST`）绝不重试，不会被重复下发。
- 失败归一为可区分的响应：
  - 上游超时 → `504 {"error": "upstream_timeout"}`
  - 连接失败 → `502 {"error": "upstream_connect_error"}`
  - 上游返回 5xx → `502 {"error": "upstream_server_error"}`（detail 含上游状态码）
  - 无健康节点 → `503 {"error": "no_healthy_upstream"}`
- 逐跳头（`Connection`、`Keep-Alive`、`TE`、`Trailers`、`Transfer-Encoding`、`Upgrade`
  及 `Proxy-*`）不会透传；转发时补齐 `X-Forwarded-For/Host/Proto`。

## 可配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `GATEWAY_STRATEGY` | `round_robin` | 选择策略：`round_robin` / `least_in_flight` |
| `GATEWAY_HEALTH_INTERVAL` | `5.0` | 健康探测周期（秒） |
| `GATEWAY_FAILURE_THRESHOLD` | `3` | 连续失败多少次标记不健康 |
| `GATEWAY_SUCCESS_THRESHOLD` | `2` | 连续成功多少次恢复健康 |
| `GATEWAY_REQUEST_TIMEOUT` | `5.0` | 单次转发超时（秒） |
| `GATEWAY_MAX_RETRIES` | `2` | 幂等请求的最大额外重试次数 |
| `GATEWAY_HEALTH_PATH` | `/health` | 健康探测路径 |

## 本地验证

```bash
uv sync
uv run pytest            # 日志中会打印节点选择过程与健康状态变化
```

测试使用本地启动的假上游（真实 uvicorn 进程 + 本地端口），覆盖：
健康阈值翻转与防抖动、无健康节点、超时后换节点重试、非幂等请求不重试、
上游 5xx/连接失败归一、逐跳头与转发头、并发下最少在途分布与计数回落、
重复注册/注销不存在节点的可区分拒绝。

手动验证示例：

```bash
uv run python main.py &                       # 网关，假设端口 8000
uv run python -m http.server 9001 &           # 任意本地假上游（需自带 /health 或用管理接口观察失败计数）
curl -X POST localhost:8000/admin/upstreams -H 'content-type: application/json' \
     -d '{"name":"a","url":"http://127.0.0.1:9001"}'
curl localhost:8000/admin/upstreams           # 查看健康状态与 last_reason
curl localhost:8000/some/path                 # 转发到上游
```
