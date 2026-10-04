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

- 建连与等待响应头受 `GATEWAY_REQUEST_TIMEOUT` 约束；
- body 阶段**只看数据空闲、不看总时长**：数据空闲超时为
  `GATEWAY_STREAM_IDLE_TIMEOUT`（未配置时回退为 `GATEWAY_REQUEST_TIMEOUT`）。
  上游连续两次读到数据的间隔超过它才中断；只要上游在持续出数据，
  传输总时长可以任意长，永远不会因为“总时长超过普通请求超时”被打断。
  大响应、gzip/br/zstd 压缩响应、上游分块响应都走同一条读取路径，语义一致。

上游异常时的处理（关键：客户端绝不可能拿到一个“看起来成功、内容却不全”的响应）：

- **响应提交点**：网关在收到上游**第一个 body 字节**（或干净的空 body 结束）之前，
  不会向客户端发送任何响应头或状态码。
- 上游**回完响应头后迟迟不出数据**（卡住）、在第一个 body 字节之前**报错或断连**：
  该次尝试被释放，对幂等方法按重试策略**换另一个健康节点重取**，
  客户端只会看到最终那个完整响应；所有可用节点都失败时返回明确的失败结果
  （`504 upstream_timeout` / `502 upstream_broken_response` / `503 no_healthy_upstream`），
  不会返回空 body 的 200。
- 上游在**已经传出部分 body 之后**停滞超过空闲超时、中途报错或连接被截断：
  此时响应头和部分数据已经发给客户端，HTTP 无法再改状态码或透明重试，
  网关直接在传输层中断响应（不发送正常的结束标记）。客户端会收到一个
  可识别的传输错误（如 httpx 的 `ReadTimeout` / `RemoteProtocolError`），
  从而能明确判定“内容不完整、本次失败”，而不是把残缺内容当成一次成功的 200。
  非幂等方法以及没有其它健康节点的请求同理，不会伪造成功。
- **客户端中途断开**：ASGI 层取消响应任务后，网关立即关闭到上游的连接并释放该节点的
  在途计数，不会傻等到上游空闲超时，也不泄漏连接；只要客户端还能正常收数据，
  在途计数就一直占着。配合优雅下线时，仍在传 body 的节点保持 `draining`、
  不会被提前摘除，直到该在途请求随客户端断开或传完而结束。

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
  - 上游在第一个 body 字节之前就断连/响应损坏 → `502 {"error": "upstream_broken_response"}`
  - 上游返回 5xx → `502 {"error": "upstream_server_error"}`（detail 含上游状态码）
  - 无健康节点 → `503 {"error": "no_healthy_upstream"}`
  - 已提交（部分 body 已发出）之后上游失败无法用状态码表达，改为在传输层中断，
    客户端收到可识别的传输错误，详见“流式 / 大响应 / 上游压缩”一节。
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
- 上游持续出数据时总时长超过普通超时也不被打断；上游回完头不出数据且另有健康节点时
  自动换节点拿到完整内容，没有可用节点时返回明确的 `504` 而不是空 200；
- 上游传到一半截断/出错时客户端收到可识别的传输错误而非残缺 200，
  在途计数随之回落、无泄漏；
- 客户端读到一半断开，在途计数立即回落、连接与计数不泄漏
  （用同一迭代器持续读取，真实模拟未放弃的客户端；用例连续多轮稳定不抖动）；
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

流式失败语义可用内置假上游端点复现（测试用 uvicorn 假上游，见 `tests/conftest.py`）：

- `GET /stall-headers`：正常时返回完整 body；把假上游 `app.state.headers_stall = True`
  后，只回响应头、永不返回 body。注册一个 stall 节点加一个健康节点，
  请求应拿到健康节点的完整内容；只注册 stall 节点时应得到 `504`，而不是空 200。
- `GET /slow-stream`：先吐一块 `first\n`，之后挂住直到 `POST /release-slow-stream`
  放行，用于观察在途计数占用/回落、空闲超时中断和优雅下线。
- `GET /steady-stream?duration=1.2&interval=0.05`：持续小批量出数据，
  把 `GATEWAY_REQUEST_TIMEOUT` 调到远小于总时长，验证流不被总时长打断。
- 客户端读到一半主动关闭连接，再查 `GET /admin/upstreams`，
  `in_flight` 应很快回落到 0；先 `DELETE /admin/upstreams/{name}` 进入优雅下线时，
  在 body 传完前节点保持 `draining` 不会消失。
