package bronya.pgsql.ext.mq.mapper;

import bronya.pgsql.ext.mq.domain.SysPqMeta;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.andreaesposito.pgmq.jdbc.client.Json;
import org.andreaesposito.pgmq.jdbc.client.MessageRecord;
import org.andreaesposito.pgmq.jdbc.client.MetricResult;
import org.andreaesposito.pgmq.jdbc.client.QueueRecord;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

@Mapper
public interface PgmqMapper extends BaseMapper<SysPqMeta> {

    /* Sending Messages */
    List<Long> send(@Param("queueName") String queueName, @Param("msg") Json msg);

    List<Long> sendWithDelay(@Param("queueName") String queueName, @Param("msg") Json msg, @Param("delay") Integer delay);

    List<Long> sendWithTimestamp(@Param("queueName") String queueName, @Param("msg") Json msg, @Param("delay") OffsetDateTime delay);

    List<Long> sendWithHeaders(@Param("queueName") String queueName, @Param("msg") Json msg, @Param("headers") Json headers);

    List<Long> sendWithHeadersAndDelay(@Param("queueName") String queueName, @Param("msg") Json msg, @Param("headers") Json headers, @Param("delay") Integer delay);

    List<Long> sendWithHeadersAndTimestamp(@Param("queueName") String queueName, @Param("msg") Json msg, @Param("headers") Json headers, @Param("delay") OffsetDateTime delay);

    List<Long> sendBatch(@Param("queueName") String queueName, @Param("msgs") Json[] msgs);

    List<Long> sendBatchWithDelay(@Param("queueName") String queueName, @Param("msgs") Json[] msgs, @Param("delay") Integer delay);

    List<Long> sendBatchWithTimestamp(@Param("queueName") String queueName, @Param("msgs") Json[] msgs, @Param("delay") OffsetDateTime delay);

    List<Long> sendBatchWithHeaders(@Param("queueName") String queueName, @Param("msgs") Json[] msgs, @Param("headers") Json[] headers);

    List<Long> sendBatchWithHeadersAndDelay(@Param("queueName") String queueName, @Param("msgs") Json[] msgs, @Param("headers") Json[] headers, @Param("delay") Integer delay);

    List<Long> sendBatchWithHeadersAndTimestamp(@Param("queueName") String queueName, @Param("msgs") Json[] msgs, @Param("headers") Json[] headers, @Param("delay") OffsetDateTime delay);

    /* Reading Messages */
    List<MessageRecord> read(@Param("queueName") String queueName, @Param("vt") Integer vt, @Param("qty") Integer qty, @Param("conditional") Json conditional);

    List<MessageRecord> readWithPollFull(@Param("queueName") String queueName, @Param("vt") Integer vt, @Param("qty") Integer qty, @Param("maxPollSeconds") Integer maxPollSeconds, @Param("pollIntervalMs") Integer pollIntervalMs, @Param("conditional") Json conditional);

    List<MessageRecord> readWithPoll(@Param("queueName") String queueName, @Param("vt") Integer vt, @Param("qty") Integer qty, @Param("conditional") Json conditional);

    Optional<MessageRecord> pop(@Param("queueName") String queueName);

    /* Deleting/Archiving Messages */
    boolean deleteMessage(@Param("queueName") String queueName, @Param("msgId") long msgId);

    List<Long> deleteBatch(@Param("queueName") String queueName, @Param("msgIds") long[] msgIds);

    Long purge(@Param("queueName") String queueName);

    boolean archiveMessage(@Param("queueName") String queueName, @Param("msgId") long msgId);

    List<Long> archiveBatch(@Param("queueName") String queueName, @Param("msgIds") long[] msgIds);

    /* Queue Management */
    void create(@Param("queueName") String queueName);

    void createUnlogged(@Param("queueName") String queueName);

    boolean drop(@Param("queueName") String queueName);

    /* Utilities */
    List<QueueRecord> listQueues();

    MetricResult metrics(@Param("queueName") String queueName);

    List<MetricResult> metricsAll();

    Optional<MessageRecord> setVisibilityTimeout(@Param("queueName") String queueName, @Param("msgId") long msgId, @Param("vt") Integer vt);
}
