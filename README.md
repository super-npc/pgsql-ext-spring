```yaml
# 容器启动示例
services:
  redis:
    restart: always
    image: redis
    ports:
      - 6379:6379
    networks:
      - npc_net
  postgres:
    image: supernpc/pgsql:18
    networks:
      - npc_net
    environment:
      POSTGRES_USER: root
      POSTGRES_PASSWORD: root
    ports:
      - 5432:5432
  pgsql-ext-admin:
    image: supernpc/pgsql-ext-admin:latest
    container_name: pgsql-ext-admin
    networks:
      - npc_net
    environment:
      - TZ=Asia/Shanghai
      - spring.profiles.active=prd
      - feign.secret=123456 # 如果未使用微服务调用,固定默认值123456即可
      - password=123456 # 有使用加密功能,参考Jasypt实现
    ports:
      - 9019:9019
networks:
  npc_net:
    external: true
```
supernpc/pgsql:18 说明下,基于postgres18 安装了多个插件合集
第一次新建数据库,插件需要手动启用
支持拓展
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS postgis CASCADE;
CREATE EXTENSION IF NOT EXISTS postgis_topology CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_cron;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS plpython3u;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgmq;

项目依赖: 
1. 类库依赖 https://github.com/roy20021/pgmq-jdbc-client
2. springboot项目脚手架 https://github.com/super-npc/bronya
3. 仓库依赖 https://github.com/super-npc/mvn-repo

项目模块说明
pgsql-ext-mq pgmq拓展二开的实现
pgsql-ext-admin 带ui后台界面管理