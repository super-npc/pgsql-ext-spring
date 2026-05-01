package bronya.pgsql.ext.admin.example.pgmq.bench.service;

import lombok.Getter;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class PgMqBenchStats {
    private final AtomicLong produced = new AtomicLong();
    private final AtomicLong produceFailed = new AtomicLong();
    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong consumeFailed = new AtomicLong();
    private final AtomicLong lastConsumedAtEpochMs = new AtomicLong();

    private final AtomicInteger consumerSleepMs = new AtomicInteger(0);
    private final AtomicInteger consumerFailRatePercent = new AtomicInteger(0);

    @Getter
    private volatile Instant startedAt = Instant.now();

    public void reset() {
        produced.set(0);
        produceFailed.set(0);
        consumed.set(0);
        consumeFailed.set(0);
        lastConsumedAtEpochMs.set(0);
        startedAt = Instant.now();
    }

    public void incProduced(long delta) {
        produced.addAndGet(delta);
    }

    public void incProduceFailed(long delta) {
        produceFailed.addAndGet(delta);
    }

    public void incConsumed(long delta) {
        consumed.addAndGet(delta);
        lastConsumedAtEpochMs.set(System.currentTimeMillis());
    }

    public void incConsumeFailed(long delta) {
        consumeFailed.addAndGet(delta);
        lastConsumedAtEpochMs.set(System.currentTimeMillis());
    }

    public void setConsumerSleepMs(int ms) {
        consumerSleepMs.set(Math.max(ms, 0));
    }

    public void setConsumerFailRatePercent(int percent) {
        consumerFailRatePercent.set(Math.max(0, Math.min(100, percent)));
    }

    public int consumerSleepMs() {
        return consumerSleepMs.get();
    }

    public int consumerFailRatePercent() {
        return consumerFailRatePercent.get();
    }

    public boolean shouldFailOnce() {
        int p = consumerFailRatePercent();
        if (p <= 0) {
            return false;
        }
        return ThreadLocalRandom.current().nextInt(100) < p;
    }

    public Snapshot snapshot() {
        return new Snapshot(
            startedAt.toEpochMilli(),
            produced.get(),
            produceFailed.get(),
            consumed.get(),
            consumeFailed.get(),
            lastConsumedAtEpochMs.get(),
            consumerSleepMs(),
            consumerFailRatePercent()
        );
    }

    public record Snapshot(
        long startedAtEpochMs,
        long produced,
        long produceFailed,
        long consumed,
        long consumeFailed,
        long lastConsumedAtEpochMs,
        int consumerSleepMs,
        int consumerFailRatePercent
    ) {
    }
}

