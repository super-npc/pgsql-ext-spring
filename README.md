# pgsql-ext-spring

基于 PostgreSQL 18 的扩展生态项目，提供消息队列、时空数据、定时任务等功能支持。

## 项目依赖

- **类库依赖**: [pgmq-jdbc-client](https://github.com/roy20021/pgmq-jdbc-client)
- **Spring Boot 脚手架**: [bronya](https://github.com/super-npc/bronya)
- **Maven 仓库(可选/本地构建)**: [mvn-repo](https://github.com/super-npc/mvn-repo)

## 项目模块

| 模块 | 说明 |
|------|------|
| `pgsql-ext-mq` | PGMQ (Postgres Message Queue) 扩展的二开实现，提供消息队列能力 |
| `pgsql-ext-admin` | 带 UI 的后台管理界面，方便管理消息队列和配置 |

## 环境要求

- JDK 25
- Spring Boot 4
- PostgreSQL 18 (带扩展)
- Docker (可选，用于容器化部署)

## 快速开始

### 编译项目

```bash
mvn clean package -DskipTests
```

### Docker 部署

```bash
docker compose up -d
```

详情参考项目下的 `docker-compose.yml` 文件。

镜像支持 `linux/amd64` 和 `linux/arm64` 架构，推送到 Docker Hub。

## 配合 Postgres 镜像使用

`supernpc/pgsql:18` 是基于 PostgreSQL 18 安装了多个插件的合集镜像，首次创建数据库需要手动启用扩展：

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS postgis CASCADE;
CREATE EXTENSION IF NOT EXISTS postgis_topology CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_cron;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS plpython3u;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgmq;
```

## PGMQ 使用说明

### @PgMqListener 注解

使用 `@PgMqListener` 注解标记消息处理方法，支持两种订阅模式：

| 订阅模式 | 说明 |
|----------|------|
| `BROADCASTING` | 广播模式，所有消费者都会收到消息 |
| `CLUSTERING` | 集群模式，消息只会分配给一个消费者 |

**示例：广播模式**

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class DemoBroadcastingHandler {

    @PgMqListener(subscribeType = SubscribeType.BROADCASTING, bindMsgDto = DemoMsgDto.class)
    public void listener1(DemoMsgDto msg) {
        log.info("广播.listener1: {}", msg);
    }

    @PgMqListener(subscribeType = SubscribeType.BROADCASTING, bindMsgDto = DemoMsgDto.class)
    public void listener2(DemoMsgDto msg) {
        log.info("广播.listener2: {}", msg);
    }
}
```

**示例：集群模式**

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class DemoClusteringHandler {

    @PgMqListener(subscribeType = SubscribeType.CLUSTERING, bindMsgDto = DemoMsgDto.class)
    public void listener1(DemoMsgDto msg) {
        log.info("集群.listener1: {}", msg);
    }

    @PgMqListener(subscribeType = SubscribeType.CLUSTERING, bindMsgDto = DemoMsgDto.class)
    public void listener2(DemoMsgDto msg) {
        log.info("集群.listener2: {}", msg);
    }
}
```

## License

MIT License
