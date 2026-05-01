package bronya.pgsql.ext.admin.example.pgmq.bench.handler;

import bronya.pgsql.ext.admin.example.pgmq.bench.dto.BenchPgMqDto;
import bronya.pgsql.ext.admin.example.pgmq.bench.service.PgMqBenchStats;
import bronya.pgsql.ext.mq.annotation.PgMqDeadLetterConfig;
import bronya.pgsql.ext.mq.annotation.PgMqListener;
import bronya.pgsql.ext.mq.annotation.PgMqListener.SubscribeType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class BenchPgMqHandler {
    private final PgMqBenchStats stats;

    @PgMqListener(
        subscribeType = SubscribeType.CLUSTERING,
        bindMsgDto = BenchPgMqDto.class,
        batchSize = 10,
        visibilityTimeout = 60,
        deadLetterConfig = @PgMqDeadLetterConfig(enableDlq = true, maxRetry = 3, retryDelaySeconds = 0)
    )
    public void clusteringConsumer(BenchPgMqDto msg) {
        int sleepMs = stats.consumerSleepMs();
        if (sleepMs > 0) {
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("interrupted");
            }
        }

        if (stats.shouldFailOnce()) {
            stats.incConsumeFailed(1);
            throw new RuntimeException("bench fail");
        }

        stats.incConsumed(1);
        if (msg != null && msg.getSeq() % 1000 == 0) {
            log.info("bench consumed: traceId={}, seq={}", msg.getTraceId(), msg.getSeq());
        }
    }
}

