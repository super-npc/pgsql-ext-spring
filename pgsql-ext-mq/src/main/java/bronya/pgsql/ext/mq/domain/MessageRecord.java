package bronya.pgsql.ext.mq.domain;

import java.time.OffsetDateTime;

public record MessageRecord(
        Long msgId,
        Integer readCt,
        OffsetDateTime enqueuedAt,
        OffsetDateTime vt,
        Json headers,
        Json message
) {
}
