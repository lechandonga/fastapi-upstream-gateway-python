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
  - 超时只在“两次收到数据之间”计时：每收到一块数据就重新起算，因此大响应、
    上游压缩（gzip/br/zstd）与分块响应在持续传输时永远不会因为总耗时长被切断；
  - 网关对上游的 httpx 客户端显式关闭了 `total` 总时长超时，只保留
    `connect/write` 的普通超时与 `read` 的按间隔空闲超时。

### 上游停滞 / 中途失败时的处理语义

网关把“是否已经向客户端发出过响应字节”作为提交点（commit point）：

- **响应头已到、但一块 body 数据都没出时上游停滞 / 报错 / 断连**：
  此时客户端什么都还没收到。对幂等方法（`GET/HEAD/OPTIONS/PUT/DELETE`），
  网关会丢弃这次尝试、排除该节点后**换到其他健康节点重新请求**，客户端最终拿到
  完整内容；非幂等方法不重试。
- **所有可用节点都失败（或重试次数用完）**：返回明确的失败结果，绝不会给一个
  空 body 的 200：
  - 上游迟迟不出数据（空闲超时，含响应头后无数据）→ `504 {"error": "upstream_timeout"}`；
  - 上游在首块数据前出错 / 断连 → `502 {"error": "upstream_stream_error"}`；
  - 连接失败、5xx 的归一语义不变（见“超时与重试语义”）。
- **已经向客户端发出过 body 数据后上游才停滞 / 报错 / 断连**：HTTP 状态码与响应头
  已经在途，无法再换成 JSON 错误。网关此时**不会**把残缺 body 当成正常结束
  （不再发送结束块），而是直接中断连接，客户端会明确收到连接中断/读取失败
  （如 `httpx.RemoteProtocolError`），可以与“完整读完”区分开来；在途计数同样
  立即回落，节点可继续服务后续请求。

压缩与分块的一致性：网关转发的是 `aiter_raw` 的编码后字节，不缓冲整个响应
（只在“首块数据到达前”这一未提交窗口内允许换节点重试），因此大响应与持续输出
仍然逐块即达；`Content-Encoding` 原样保留、chunked 由 httpx 解帧后由 ASGI 服务器
重新组帧，客户端最终字节与直连上游完全一致，不会截断、错位或解析报错。

在途计数与连接生命周期：

- 节点在途计数在整个 body 传输期间一直占用，直到上游发完、客户端提前断开或传输异常才回落；
- 客户端**还在正常接收/等待数据期间**，计数始终占用，配合优雅下线时该节点不会被提前摘掉；
- 客户端中途放弃（断连）后：独立的断开监听任务立即取消转发，到上游的连接被关闭、
  计数确定回落；所有清理在 shielded 作用域内执行，即使转发任务被取消也不会留下
  泄漏的连接或计数（同一用例连续多跑结果一致）。

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
- 重试不仅覆盖建连超时/失败与上游 5xx，也覆盖**响应头已返回但首块 body
  数据到达前**的停滞（空闲超时）、上游中途报错与连接断开；这些情况客户端
  尚无任何响应字节，换节点取完整内容对客户端完全透明。
- 非幂等方法（如 `POST`）绝不重试，不会被重复下发。
- 失败归一为可区分的响应：
  - 上游超时 → `504 {"error": "upstream_timeout"}`
  - 连接失败 → `502 {"error": "upstream_connect_error"}`
  - 上游返回 5xx → `502 {"error": "upstream_server_error"}`（detail 含上游状态码）
  - 首块数据前上游流失败/断连 → `502 {"error": "upstream_stream_error"}`
  - 无健康节点 → `503 {"error": "no_healthy_upstream"}`
  - 首块数据已发出后上游失败：无法再改状态码，网关中断连接，客户端收到明确的
    传输中断（而不是残缺但“成功”的 200）。
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
- 上游持续出数据时总时长超过普通超时也不被打断；上游迟迟不出数据时按空闲超时处理；
- **上游回完响应头不出数据**：另有健康节点时换节点拿到完整内容；没有可用节点或
  重试耗尽时返回明确的 `504 upstream_timeout`（不是空 body 的 200）；
- **上游传到一半报错 / 连接断开**：首块数据前失败会换节点重试，无节点时返回
  `502 upstream_stream_error`；已发出数据后失败则中断连接，客户端读到明确的传输
  失败而不是残缺的“成功”200，在途计数回落；
- **客户端读到一半主动断开**：断开后在途计数确定性回落、连接不泄漏；断开前客户端
  仍在等待数据时计数一直占用（该用例参数化连跑 5 次，整套测试可重复多跑不 flaky）；
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

流式失败语义可用测试里的假上游复现（`tests/conftest.py` 中 `make_fake_upstream`）：

```bash
# 直接反复运行全套用例（真实 uvicorn 进程 + 本地端口），日志里可按
# scenario=... 关键字区分每个业务场景：
uv run pytest -q tests/test_streaming_headers_drain.py

# 关键日志事件：
#   scenario=stream_commit        首块数据到达，响应提交（此后不可换状态码）
#   scenario=upstream_idle_timeout 上游超空闲阈值无数据（committed=False 时换节点）
#   scenario=upstream_stream_error 首块数据前上游报错/断连
#   scenario=upstream_5xx         上游 5xx
#   scenario=client_abort         客户端中途断开，取消转发并清理
#   scenario=stream_complete      body 完整转发结束

# 手动模拟“响应头回来后迟迟不出数据”：
#   1) 启动两个假上游，给第一个设置 app.state.header_stalls = True（见 /stall-headers）
#   2) 都注册到网关后 GET /stall-headers
#      -> 坏节点卡住后自动换到健康节点，响应为完整的 complete-body
#   3) 只注册坏节点（或把 GATEWAY_MAX_RETRIES=0）再请求
#      -> 得到 504 {"error":"upstream_timeout"}，而不是空 200
#
# 模拟“传到一半出错”：对假上游设置 app.state.half_errors = True，
#   请求 /half-then-error -> 客户端读到 part-0/part-1 后收到连接中断；
#   另注册一个健康节点时，fail_before_byte 场景会直接换节点取完整内容。
#
# 模拟“客户端中途断开”：请求 /slow-stream 收到 first\n 后直接关闭客户端，
#   GET /admin/upstreams 可见 in_flight 很快回落为 0 且无残留连接。
```
