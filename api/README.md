# MediaWorker HTTP API 契约（Swagger）

本目录的 HTTP API 契约由 [swaggo/swag](https://github.com/swaggo/swag) 从代码注解生成，不再维护手写 markdown。注解位于各服务 `cmd/*/main.go`（general info）与 handler 源码（`@Summary` / `@Param` / `@Success` / `@Router` 等）。

## Spec 文件

| 服务 | JSON | YAML |
|---|---|---|
| control-plane | [./control-plane/swagger.json](./control-plane/swagger.json) | [./control-plane/swagger.yaml](./control-plane/swagger.yaml) |
| edge-node | [./edge-node/swagger.json](./edge-node/swagger.json) | [./edge-node/swagger.yaml](./edge-node/swagger.yaml) |
| ingest-worker | [./ingest-worker/swagger.json](./ingest-worker/swagger.json) | [./ingest-worker/swagger.yaml](./ingest-worker/swagger.yaml) |

## 再生成与校验

```bash
# 从注解重新生成三份 spec
bash scripts/gen-swagger.sh

# 路由对账：49 条 mux 注册路由 ↔ spec paths 一一对应
bash scripts/check-swagger-routes.sh
```

## 本地预览

```bash
# 校验 spec 合法性
npx @apidevtools/swagger-cli validate api/control-plane/swagger.json

# Swagger UI 预览（以 control-plane 为例）
docker run -p 8081:8080 -e SWAGGER_JSON=/spec/swagger.json -v $PWD/api/control-plane:/spec swaggerapi/swagger-ui
```

## 非 HTTP 文档导航

P2P 协议、配置项、类型契约等非 HTTP 内容已迁移至 `docs/`：

- [docs/protocols.md](../docs/protocols.md)：libp2p 流协议、GossipSub 主题、控制通道事件
- [docs/configuration.md](../docs/configuration.md)：四服务配置项表
- [docs/janitor.md](../docs/janitor.md)：janitor GC 服务（CLI、退出码、内部契约、部署、安全）
- [docs/shared-types.md](../docs/shared-types.md)：共享领域类型与存储层接口契约
- [docs/modules.md](../docs/modules.md)：系统概览与模块功能对照
- [docs/remediation-ledger.md](../docs/remediation-ledger.md)：T1-T20 remediation 历史台账

设计背景见 [docs/README.md](../docs/README.md)（总体架构）及 `docs/` 下领域文档（distribution / storage / ingest / policy）。
