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