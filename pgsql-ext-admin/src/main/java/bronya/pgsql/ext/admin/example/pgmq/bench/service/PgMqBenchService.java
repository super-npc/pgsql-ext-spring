package bronya.pgsql.ext.admin.example.pgmq.bench.service;

import bronya.pgsql.ext.admin.example.pgmq.bench.dto.BenchPgMqDto;
import bronya.pgsql.ext.mq.service.PgMqSender;
import cn.hutool.v7.core.util.RandomUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
public class PgMqBenchService {
    private final PgMqSender<BenchPgMqDto> sender;
    private final PgMqBenchStats stats;

    public PgMqBenchStats.Snapshot start(long totalMessages, int threads, int payloadSize, int delaySeconds) {
        stats.reset();
        String traceId = UUID.randomUUID().toString();
        int safeThreads = Math.max(1, threads);
        long safeTotalMessages = Math.max(0, totalMessages);
        int safePayloadSize = Math.max(0, payloadSize);
        int safeDelaySeconds = Math.max(0, delaySeconds);

        AtomicLong seq = new AtomicLong();
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            long perThread = safeTotalMessages / safeThreads;
            long remain = safeTotalMessages % safeThreads;
            for (int i = 0; i < safeThreads; i++) {
                long count = perThread + (i < remain ? 1 : 0);
                executor.submit(() -> this.produce(traceId, count, safePayloadSize, safeDelaySeconds, seq));
            }
        } finally {
            executor.shutdown();
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return stats.snapshot();
    }

    private void produce(String traceId, long count, int payloadSize, int delaySeconds, AtomicLong seq) {
        for (long i = 0; i < count; i++) {
            long s = seq.incrementAndGet();
            String payload = payloadSize == 0 ? "" : RandomUtil.randomLetters(payloadSize);
            BenchPgMqDto dto = new BenchPgMqDto(traceId, s, payload);
            try {
                if (delaySeconds > 0) {
                    sender.send(dto, delaySeconds);
                } else {
                    sender.send(dto);
                }
                stats.incProduced(1);
            } catch (Exception e) {
                stats.incProduceFailed(1);
            }
        }
    }
}

