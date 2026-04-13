package bronya.pgsql.ext.mq.autoconfig;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.andreaesposito.pgmq.jdbc.client.PgmqJdbcClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * PgMQ 消息队列配置类
 *
 * <p>负责初始化 PgmqJdbcClient 实例，仅支持 PostgreSQL 数据库。
 *
 * @author olive
 * @see <a href="https://github.com/roy20021/pgmq-jdbc-client">pgmq-jdbc-client</a>
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class PgMqConfig {

    /** 数据源 */
    private final DataSource dataSource;

    /**
     * 创建 PgmqJdbcClient 实例
     *
     * <p>该 Bean 仅支持 PostgreSQL 数据库，会在创建时自动校验数据库类型并初始化 pgmq 扩展。
     * <p><b>重要：</b>每次调用会获取新的连接，确保连接池管理的连接不会过期。
     *
     * @return PgmqJdbcClient 实例
     * @throws SQLException 如果数据库操作失败
     * @throws IllegalStateException 如果数据库不是 PostgreSQL
     */
    @Bean
    public PgmqJdbcClient pgmqJdbcClient() throws SQLException {
        log.info("初始化 PgMqClient");

        // 校验数据库类型并初始化扩展（使用临时连接）
        try (Connection connection = dataSource.getConnection()) {
            String dbProductName = connection.getMetaData().getDatabaseProductName();
            Assert.isTrue("PostgreSQL".equalsIgnoreCase(dbProductName),
                "PgMqService 仅支持 PostgreSQL 数据库，当前数据库类型为: " + dbProductName);

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("CREATE EXTENSION IF NOT EXISTS pgmq");
            }
        }

        // 返回使用 DataSource 的客户端，确保每次操作获取新连接
        return new DataSourcePgmqJdbcClient(dataSource);
    }
}
