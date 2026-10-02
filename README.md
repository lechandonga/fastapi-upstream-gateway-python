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
| `DELETE` | `/admin/upstreams/{name}` | 注销节点（无在途立即移除；有在途进入优雅下线，返回 `202`） |
| `GET` | `/admin/upstreams` | 查询所有节点状态、在途计数与最近一次判定原因 |
| `POST` | `/admin/upstreams/check` | 立即执行一轮健康探测（不等待周期） |

拒绝语义按类型区分：

- 重复注册 → `409 {"error": "node_already_exists"}`
- 注销/查询不存在的节点 → `404 {"error": "node_not_found"}`
- 节点已在“下线中”时再次注销 → `409 {"error": "node_draining"}`（detail 含剩余在途数）

节点快照在原有字段之外新增 `state`（`active` / `draining`）与布尔别名
`draining`，原有字段含义保持不变。

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

- 每次转发尝试使用 `GATEWAY_REQUEST_TIMEOUT` 秒超时。对流式响应而言，
  这是**相邻数据块之间的空闲超时**，不是总传输时长上限：上游持续出数据时
  （大文件、长流式输出）总耗时可以远超该值且不会被打断；只有上游迟迟
  不再出数据时才超时。
- 仅对幂等方法（`GET/HEAD/OPTIONS/PUT/DELETE`）重试，最多额外尝试
  `GATEWAY_MAX_RETRIES` 次；**每次重试必定换到不同的健康节点**。
- 非幂等方法（如 `POST`）绝不重试，不会被重复下发。
- 失败归一为可区分的响应：
  - 上游超时 → `504 {"error": "upstream_timeout"}`
  - 连接失败 → `502 {"error": "upstream_connect_error"}`
  - 上游返回 5xx → `502 {"error": "upstream_server_error"}`（detail 含上游状态码）
  - 响应头已返回后流出中断 → 已发出的响应直接终止（状态码已提交，无法改写）
  - 无健康节点 → `503 {"error": "no_healthy_upstream"}`

## 流式 / 大响应 / 压缩透传

- 网关使用 HTTP 流式转发，**不在网关侧缓冲整个响应体**：收到上游的响应头
  和第一批数据后立即开始向客户端回传，客户端无需等待上游响应结束。
- 上游的原始响应帧（含 gzip/deflate 等压缩体、分块传输）按字节透传，
  不解压、不重新分块；客户端拿到的内容与直连上游完全一致，可正常解压解析，
  不会截断、卡住或解析报错。
- 在途计数从选节点开始占用，**持续到响应体完整发完或客户端提前断开**才回落；
  客户端中途放弃会同时关闭上游连接，计数与连接均不泄漏。
- 网关在向上游发出请求后、收到**第一批响应体数据之前**如果发生超时或连接中断，
  仍按既有重试语义换节点重试；一旦已有数据开始回传则不再重试。

## 头处理策略

请求与响应双向都遵循同一套规则：

- 标准逐跳头（`Connection`、`Keep-Alive`、`TE`、`Trailers`、
  `Transfer-Encoding`、`Upgrade`、`Proxy-Authenticate/Authorization`）一律剔除。
- 解析对端 `Connection` 头中**点名声明的头**（如
  `Connection: keep-alive, x-internal-token`），这些头在本次连接上也按
  逐跳头处理：请求方向不会发往上游，响应方向不会回给客户端。
- 运维可通过配置额外声明“绝不出网关”的头（如内部令牌、内部标记），
  无论对端怎么带都会被剔除（见下表两个新增配置项）。
- 上述剔除不影响标准转发头语义：转发时仍补齐
  `X-Forwarded-For/Host/Proto`，并把 `Host` 重写为上游地址。

## 优雅下线

`DELETE /admin/upstreams/{name}` 的状态流转：

1. 节点立即从可选节点池摘除（`state=draining`），不再接任何新请求，
   健康探测也会跳过它；
2. 若节点没有在途请求：行为与旧版一致，立即真正移除，返回 `200`；
3. 若仍有在途请求：返回 `202`，节点保持 `draining` 状态并继续服务已在途的
   请求，`GET /admin/upstreams` 期间持续可见其 `state` 与剩余 `in_flight`；
4. 全部在途请求正常结束（含客户端提前断开）后，节点被真正移除。

并发语义：注销与请求并发时，已开始下线的节点不可能再接到新请求；在途请求
不会被注销动作中断；计数随请求结束正常回落并触发最终移除。对下线中节点
再次注销稳定返回 `409 node_draining`，注销不存在的节点稳定返回
`404 node_not_found`，两者明确可区分。

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
| `GATEWAY_BLOCK_REQUEST_HEADERS` | （空） | 请求方向绝不出网关的头，逗号分隔（如 `x-internal-token,x-internal-flag`） |
| `GATEWAY_BLOCK_RESPONSE_HEADERS` | （空） | 响应方向绝不回给客户端的头，逗号分隔 |

## 本地验证

```bash
uv sync
uv run pytest            # 日志中会打印节点选择过程与健康状态变化
```

测试使用本地启动的假上游（真实 uvicorn 进程 + 本地端口），覆盖：
健康阈值翻转与防抖动、无健康节点、超时后换节点重试、非幂等请求不重试、
上游 5xx/连接失败归一、逐跳头与转发头、并发下最少在途分布与计数回落、
重复注册/注销不存在节点的可区分拒绝，以及：

- 大响应（512KB）字节级一致、gzip 压缩响应原样透传且可正常解析、
  分块流式响应完整无截断；
- 总时长超过 `GATEWAY_REQUEST_TIMEOUT` 但持续出数据的流不被超时打断，
  上游迟迟不出数据（首块前/传输中途）时正确超时；
- 流式传输全程在途计数占用、客户端中途断开后计数与连接回落、节点可立即复用；
- 配置禁传头与双向 `Connection` 点名头（响应方向用原始 socket 假上游构造
  非标准逐跳响应）均被剔除；
- 优雅下线：有在途时 `202` + `draining` 状态与剩余计数可见、无在途立即
  `200` 移除、下线节点不再接新请求、在途请求完整跑完后移除、重复注销
  `409 node_draining` 与注销不存在 `404 node_not_found` 可区分，
  以及注销与并发请求相撞时计数不丢、在途不中断。

假上游提供 `/large`、`/gzip`、`/stream?chunks=&interval=&stall_after=`、
`/slow-first`、`/headers-echo` 等端点，全部只监听本地端口。

运行日志中会继续打印节点选择（`select/release node=... in_flight=...`）、
健康状态翻转、节点摘池（`node draining`）与下线完成（`node unregistered`）。

## 兼容性

- 启动方式不变：`uv run python main.py` 或 `uv run uvicorn main:app --port 8000`。
- `GET /` 仍返回 `{"Hello": "World"}`；注册/查询/手动健康检查接口路径与
  状态码不变；无在途节点的注销仍是同步移除、返回 `200`。
- 节点快照为**只增不改**：新增 `state`、`draining` 字段，其余字段与含义不变。
- 所有新增配置项默认关闭（空列表），不配置时头处理与旧行为一致。

手动验证示例：

```bash
uv run python main.py &                       # 网关，假设端口 8000
uv run python -m http.server 9001 &           # 任意本地假上游（需自带 /health 或用管理接口观察失败计数）
curl -X POST localhost:8000/admin/upstreams -H 'content-type: application/json' \
     -d '{"name":"a","url":"http://127.0.0.1:9001"}'
curl localhost:8000/admin/upstreams           # 查看健康状态与 last_reason
curl localhost:8000/some/path                 # 转发到上游
```
