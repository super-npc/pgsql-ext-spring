package bronya.pgsql.ext.mq.autoconfig;

import lombok.extern.slf4j.Slf4j;
import org.andreaesposito.pgmq.jdbc.client.*;

import javax.sql.DataSource;
import jakarta.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

/**
 * 基于 DataSource 的 PgmqJdbcClient 包装器
 *
 * <p>解决原 PgmqJdbcClient 使用单一连接导致的 "Socket closed" 和 "connection disabled" 问题。
 * 每次操作从 DataSource 获取新连接，使用完毕后自动归还连接池。
 *
 * @author olive
 */
@Slf4j
public class DataSourcePgmqJdbcClient extends PgmqJdbcClient {

    private final DataSource dataSource;
    private final Connection bootstrapConnection;

    /**
     * 使用临时连接初始化父类（不会被实际使用）
     */
    public DataSourcePgmqJdbcClient(DataSource dataSource) throws SQLException {
        this(getTempConnection(dataSource), dataSource);
    }

    private DataSourcePgmqJdbcClient(Connection bootstrapConnection, DataSource dataSource) throws SQLException {
        super(bootstrapConnection);
        this.bootstrapConnection = bootstrapConnection;
        this.dataSource = dataSource;
    }

    private static Connection getTempConnection(DataSource ds) throws SQLException {
        return ds.getConnection();
    }

    @PreDestroy
    public void closeBootstrapConnection() {
        try {
            if (bootstrapConnection != null && !bootstrapConnection.isClosed()) {
                bootstrapConnection.close();
            }
        } catch (Exception e) {
            log.warn("关闭 bootstrapConnection 失败: {}", e.getMessage());
        }
    }

    /**
     * 在连接中执行操作
     */
    private <T> T executeWithConnection(SqlFunction<Connection, T> action) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return action.apply(conn);
        }
    }

    @Override
    public List<Long> send(String queue, Json message) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.send(queue, message);
        });
    }

    @Override
    public List<Long> send(String queue, Json message, int delaySeconds) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.send(queue, message, delaySeconds);
        });
    }

    @Override
    public List<Long> send(String queue, Json message, Json headers) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.send(queue, message, headers);
        });
    }

    @Override
    public List<Long> send(String queue, Json message, OffsetDateTime delayAt) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.send(queue, message, delayAt);
        });
    }

    @Override
    public List<Long> send(String queue, Json message, Json headers, int delaySeconds) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.send(queue, message, headers, delaySeconds);
        });
    }

    @Override
    public List<Long> send(String queue, Json message, Json headers, OffsetDateTime delayAt) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.send(queue, message, headers, delayAt);
        });
    }

    @Override
    public List<Long> sendBatch(String queue, Json[] messages) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.sendBatch(queue, messages);
        });
    }

    @Override
    public List<Long> sendBatch(String queue, Json[] messages, int delaySeconds) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.sendBatch(queue, messages, delaySeconds);
        });
    }

    @Override
    public List<Long> sendBatch(String queue, Json[] messages, OffsetDateTime delayAt) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.sendBatch(queue, messages, delayAt);
        });
    }

    @Override
    public List<Long> sendBatch(String queue, Json[] messages, Json[] headers) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.sendBatch(queue, messages, headers);
        });
    }

    @Override
    public List<Long> sendBatch(String queue, Json[] messages, Json[] headers, int delaySeconds) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.sendBatch(queue, messages, headers, delaySeconds);
        });
    }

    @Override
    public List<Long> sendBatch(String queue, Json[] messages, Json[] headers, OffsetDateTime delayAt) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.sendBatch(queue, messages, headers, delayAt);
        });
    }

    @Override
    public List<MessageRecord> read(String queue, int visibilityTimeoutSeconds, int quantity, Json filter) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.read(queue, visibilityTimeoutSeconds, quantity, filter);
        });
    }

    @Override
    public List<MessageRecord> readWithPoll(String queue, int visibilityTimeoutSeconds, int pollSeconds, Json filter) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.readWithPoll(queue, visibilityTimeoutSeconds, pollSeconds, filter);
        });
    }

    @Override
    public List<MessageRecord> readWithPoll(String queue, int visibilityTimeoutSeconds, int quantity, int pollSeconds, int pollIntervalMs, Json filter) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.readWithPoll(queue, visibilityTimeoutSeconds, quantity, pollSeconds, pollIntervalMs, filter);
        });
    }

    @Override
    public Optional<MessageRecord> pop(String queue) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.pop(queue);
        });
    }

    @Override
    public List<Long> delete(String queue, long[] messageIds) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.delete(queue, messageIds);
        });
    }

    @Override
    public boolean delete(String queue, long messageId) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.delete(queue, messageId);
        });
    }

    @Override
    public boolean archive(String queue, long messageId) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.archive(queue, messageId);
        });
    }

    @Override
    public List<Long> archive(String queue, long[] messageIds) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.archive(queue, messageIds);
        });
    }

    @Override
    public Optional<MessageRecord> setVisibilityTimeout(String queue, long messageId, int visibilityTimeoutSeconds) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.setVisibilityTimeout(queue, messageId, visibilityTimeoutSeconds);
        });
    }

    @Override
    public void create(String queue) throws SQLException {
        executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            client.create(queue);
            return null;
        });
    }

    @Override
    public void createUnlogged(String queue) throws SQLException {
        executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            client.createUnlogged(queue);
            return null;
        });
    }

    @Override
    public boolean drop(String queue) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.drop(queue);
        });
    }

    @Override
    public Long purge(String queue) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.purge(queue);
        });
    }

    @Override
    public List<QueueRecord> listQueues() throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.listQueues();
        });
    }

    @Override
    public MetricResult metrics(String queue) throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.metrics(queue);
        });
    }

    @Override
    public List<MetricResult> metricsAll() throws SQLException {
        return executeWithConnection(conn -> {
            PgmqJdbcClient client = new PgmqJdbcClient(conn);
            return client.metricsAll();
        });
    }

    @FunctionalInterface
    private interface SqlFunction<T, R> {
        R apply(T t) throws SQLException;
    }
}
