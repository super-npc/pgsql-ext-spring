package bronya.pgsql.ext.mq.domain;

import java.time.OffsetDateTime;

public record MetricResult(
        String queueName,
        Long queueLength,
        Integer newestMsgAgeSec,
        Integer oldestMsgAgeSec,
        Long totalMessages,
        OffsetDateTime scrapeTime,
        Long queueVisibleLength
) {
}
