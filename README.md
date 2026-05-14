# VeighNa框架的QuestDB数据库接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.0.0-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows|linux|macos-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.10|3.11|3.12|3.13-blue.svg" />
</p>

## 说明

对接 QuestDB 的高性能时序数据库接口实现。

主要特性：

- 通过 QuestDB ILP/HTTP 高速写入K线和Tick数据。
- 通过 PGWire 执行建表、查询、汇总和逻辑删除。
- 使用 QuestDB WAL 表和 `DEDUP UPSERT KEYS` 支持重复数据覆盖。
- 使用 `deleted` 字段进行逻辑删除，查询和汇总时自动过滤已删除数据。

## 安装

```bash
pip install vnpy_questdb
```

## QuestDB 端口说明

当前实现会同时使用 QuestDB 的两个服务端口：

- `database.port`：PGWire SQL端口，默认 `8812`。代码使用 `psycopg` 连接该端口，用于创建表、读取数据、查询汇总、执行逻辑删除，以及检查 WAL 事务是否已经应用。
- `database.http_port`：HTTP端口，默认 `9000`。代码使用 `questdb.ingress.Sender` 通过 ILP/HTTP 写入K线和Tick数据。

这两个端口的职责不同，不能互相替代。`8812` 面向 PostgreSQL Wire Protocol 查询；`9000` 面向 QuestDB HTTP服务，其中包含 ILP/HTTP 写入入口和 Web Console。如果使用 Docker、远程服务器或防火墙，需要同时开放这两个端口。

示例 Docker 端口映射：

```bash
docker run --rm -p 8812:8812 -p 9000:9000 questdb/questdb
```

当前版本使用 ILP/HTTP 写入，不使用 QuestDB 的 ILP/TCP `9009` 端口。

## VeighNa 配置

在 VeighNa 配置文件中选择 QuestDB 数据库，并配置连接参数：

```json
{
    "database.name": "questdb",
    "database.host": "localhost",
    "database.port": 8812,
    "database.user": "admin",
    "database.password": "quest",
    "database.database": "qdb",
    "database.http_port": 9000
}
```

参数说明：

- `database.name`：数据库适配器名称，使用本插件时配置为 `questdb`。
- `database.host`：QuestDB 服务地址，PGWire 和 HTTP ILP 都会连接该地址。
- `database.port`：PGWire SQL端口，用于查询和管理操作。
- `database.user`：PGWire 用户名，QuestDB 默认值为 `admin`。
- `database.password`：PGWire 密码，QuestDB 默认值为 `quest`。
- `database.database`：PGWire 数据库名，QuestDB 默认值为 `qdb`。
- `database.http_port`：QuestDB HTTP端口，用于 ILP/HTTP 数据写入。

如果 QuestDB 部署在远程主机，请确认 `database.host` 可以从运行 VeighNa 的机器访问，并确认 `database.port` 与 `database.http_port` 均已开放。
