package bronya.pgsql.ext.mq.autoconfig;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * PgMQ 消息队列配置类
 *
 * <p>负责初始化 pgmq 扩展，仅支持 PostgreSQL 数据库。
 *
 * @author olive
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class PgMqConfig {

    /** 数据源 */
    private final DataSource dataSource;

    /**
     * 初始化 pgmq 扩展
     *
     * <p>该方法仅支持 PostgreSQL 数据库，会在应用启动时自动校验数据库类型并初始化 pgmq 扩展。
     *
     * @throws SQLException 如果数据库操作失败
     * @throws IllegalStateException 如果数据库不是 PostgreSQL
     */
    @PostConstruct
    public void initPgmqExtension() throws SQLException {
        log.info("初始化 PgMq 扩展");

        // 校验数据库类型并初始化扩展（使用临时连接）
        try (Connection connection = dataSource.getConnection()) {
            String dbProductName = connection.getMetaData().getDatabaseProductName();
            Assert.isTrue("PostgreSQL".equalsIgnoreCase(dbProductName),
                "PgMqService 仅支持 PostgreSQL 数据库，当前数据库类型为: " + dbProductName);

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("CREATE EXTENSION IF NOT EXISTS pgmq");
            }
        }
    }
}
