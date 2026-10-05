# 1.0.1版本

1. 空的database.host和localhost改为127.0.0.1，避免Windows上连接超时
2. 补充公开接口文档和类型注解，增加离线测试并在持续集成中运行

# 1.0.0版本

1. 增加QuestDB数据库接口
2. 支持通过ILP/HTTP写入K线和Tick数据
3. 支持通过PGWire查询数据和汇总信息
4. 支持逻辑删除和DEDUP重复写入覆盖
