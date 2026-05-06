package bronya.pgsql.ext.mq.domain;

import java.time.OffsetDateTime;

public record QueueRecord(
        String queueName,
        Boolean isPartitioned,
        Boolean isUnlogged,
        OffsetDateTime createdAt
) {
}
