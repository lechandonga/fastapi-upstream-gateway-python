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
| `DELETE` | `/admin/upstreams/{name}` | 注销节点（有在途请求时进入优雅下线，返回 `202`） |
| `GET` | `/admin/upstreams` | 查询所有节点状态、在途计数与最近一次判定原因 |
| `POST` | `/admin/upstreams/check` | 立即执行一轮健康探测（不等待周期） |

拒绝语义按类型区分：

- 重复注册 → `409 {"error": "node_already_exists"}`
- 注销/查询不存在的节点 → `404 {"error": "node_not_found"}`
- 节点已在下线中再次注销 → `409 {"error": "node_draining"}`（detail 含剩余在途数）

`GET /admin/upstreams` 的每个节点快照新增/包含以下字段：

- `state`：`healthy` / `unhealthy` / `draining` 三态；
- `draining`：是否处于下线中；
- `in_flight`：当前在途请求数（下线期间表示剩余在途数）。

原有字段（`healthy`、连续计数、`last_reason` 等）与响应结构保持不变。

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

在途计数在选节点时于注册表锁内递增，**响应体完整发完（或客户端提前断开）后**才递减，
并发下不会超发也不会不回落。

## 流式 / 大响应 / 上游压缩

网关对所有上游响应采用边收边发的流式透传：

- 上游响应头一到就开始回传，后续 body 按块立即转发给客户端，
  不会等整个响应收完；大文件与持续输出（SSE、长轮询等）都适用。
- 透传基于**编码后的原始字节**（`aiter_raw`）：
  - 上游 `gzip/br/zstd` 等压缩体连同 `Content-Encoding` 原样转发，
    客户端解压结果与直连上游完全一致，网关不解压、不二次压缩；
  - 上游的分块传输（chunked）由 httpx 解帧、再由 ASGI 服务器按客户端能力重新组帧，
    `Transfer-Encoding` 等逐跳头不透传，内容不截断、不错位。
- 重复响应头（如多个 `Set-Cookie`）原样保留。

超时规则（与普通请求区分）：

- 建连与等待响应头仍受 `GATEWAY_REQUEST_TIMEOUT` 约束；
- body 阶段采用**数据空闲超时** `GATEWAY_STREAM_IDLE_TIMEOUT`
  （未配置时回退为 `GATEWAY_REQUEST_TIMEOUT`）：上游超过该时长一个字节都不出，
  流被中断；只要上游在持续出数据，传输总时长可以远超普通请求超时，不会被打断。

在途计数与连接生命周期：

- 节点在途计数在整个 body 传输期间一直占用，直到上游发完、客户端提前断开或传输异常才回落；
- 客户端中途放弃（断连）会关闭到上游的连接并释放计数，不会造成计数或连接泄漏。

## 请求头 / 响应头策略

双向都执行两层剔除（头名匹配一律大小写不敏感）：

1. 固定的标准逐跳头（RFC 7230：`Connection`、`Keep-Alive`、`Proxy-*`、`TE`、
   `Trailer(s)`、`Transfer-Encoding`、`Upgrade`）永远不透传；
2. 消息自身 `Connection` 头里**点名声明**的头也按逐跳头剔除
   （例如客户端发 `Connection: x-internal-mark`，则 `X-Internal-Mark` 绝不到上游；
   响应方向同理）；
3. 运维可通过配置额外声明禁传头名单：
   - `GATEWAY_BLOCKED_REQUEST_HEADERS`：客户端怎么带都不会发往上游（如内部令牌、内部标记）；
   - `GATEWAY_BLOCKED_RESPONSE_HEADERS`：上游响应里被点名的头不会原样回给客户端。

标准转发头的补齐语义保持不变：转发时重写 `Host` 为上游地址，并补齐
`X-Forwarded-For` / `X-Forwarded-Host` / `X-Forwarded-Proto`。

## 优雅下线

注销节点时按在途情况分流：

- **无在途请求**：行为与旧版一致，立即从注册表移除，`DELETE` 返回 `200`；
- **有在途请求**：立即把节点摘出可选池（新请求绝不会再落到它，健康探测也跳过它），
  节点进入 `draining` 状态，`DELETE` 返回 `202`，快照里可看到
  `state="draining"` 与剩余 `in_flight`；等所有在途请求正常完成（含流式响应发完或
  客户端断开）后节点才真正移除，在途请求不会被中断。

并发语义与可区分回应：

- 注销与并发请求相撞时，下线开始之后的新请求只会落在其他健康节点；
  计数随每个请求结束正常回落，最后一个请求结束时节点自动消失；
- 对正在下线的节点再次注销 → `409 {"error": "node_draining"}`；
- 注销根本不存在的节点 → `404 {"error": "node_not_found"}`；
- 下线完成后再注销等同于注销不存在节点（`404 node_not_found`）。

## 超时与重试语义

- 每次转发尝试的建连/响应头阶段使用 `GATEWAY_REQUEST_TIMEOUT` 秒超时；
  流式 body 阶段使用数据空闲超时（见“流式 / 大响应 / 上游压缩”一节）。
- 仅对幂等方法（`GET/HEAD/OPTIONS/PUT/DELETE`）重试，最多额外尝试
  `GATEWAY_MAX_RETRIES` 次；**每次重试必定换到不同的健康节点**。
- 非幂等方法（如 `POST`）绝不重试，不会被重复下发。
- 失败归一为可区分的响应：
  - 上游超时 → `504 {"error": "upstream_timeout"}`
  - 连接失败 → `502 {"error": "upstream_connect_error"}`
  - 上游返回 5xx → `502 {"error": "upstream_server_error"}`（detail 含上游状态码）
  - 无健康节点 → `503 {"error": "no_healthy_upstream"}`
- 头处理策略见“请求头 / 响应头策略”一节；转发时补齐 `X-Forwarded-For/Host/Proto`。

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
| `GATEWAY_STREAM_IDLE_TIMEOUT` | 未设置（回退到 `GATEWAY_REQUEST_TIMEOUT`） | 流式 body 的数据空闲超时（秒）：只限制两次数据之间的间隔，不限制总时长 |
| `GATEWAY_BLOCKED_REQUEST_HEADERS` | 空 | 禁发往上游的请求头，逗号分隔，大小写不敏感 |
| `GATEWAY_BLOCKED_RESPONSE_HEADERS` | 空 | 禁回给客户端的响应头，逗号分隔，大小写不敏感 |

所有环境变量均为可选项，不配置时行为与旧版默认值一致；启动方式、`GET /`、
管理接口路径与原有响应字段均保持向后兼容（仅新增字段与新的 `409 node_draining` 情形）。

## 本地验证

```bash
uv sync
uv run pytest            # 日志中会打印节点选择过程与健康状态变化
```

测试使用本地启动的假上游（真实 uvicorn 进程 + 本地端口），覆盖：
健康阈值翻转与防抖动、无健康节点、超时后换节点重试、非幂等请求不重试、
上游 5xx/连接失败归一、逐跳头与转发头、并发下最少在途分布与计数回落、
重复注册/注销不存在节点的可区分拒绝，以及新增场景：

- 大响应（512KB 分块流式）逐字节正确、压缩响应（gzip）与直连上游报文一致、
  chunked 响应正确重组；
- 首块数据提前到达（不必等整个响应）、传输期间在途计数占用与结束后回落；
- 上游持续出数据时总时长超过普通超时也不被打断；上游迟迟不出数据时按空闲超时中断；
- 客户端读到一半断开，在途计数最终回落、无泄漏；
- 配置的禁传请求头/响应头剔除，`Connection` 点名的双向逐跳头剔除，转发头仍补齐；
- 优雅下线：无在途立即移除（`200`）、有在途返回 `202` 且 `state=draining`/剩余在途可见、
  在途完成后自动移除、下线中不接新请求、重复注销 `409 node_draining`、
  注销不存在节点 `404 node_not_found`、注销与并发请求互斥不超选、计数不泄漏。

手动验证示例：

```bash
uv run python main.py &                       # 网关，假设端口 8000
uv run python -m http.server 9001 &           # 任意本地假上游（需自带 /health 或用管理接口观察失败计数）
curl -X POST localhost:8000/admin/upstreams -H 'content-type: application/json' \
     -d '{"name":"a","url":"http://127.0.0.1:9001"}'
curl localhost:8000/admin/upstreams           # 查看健康状态与 last_reason
curl localhost:8000/some/path                 # 转发到上游
```
