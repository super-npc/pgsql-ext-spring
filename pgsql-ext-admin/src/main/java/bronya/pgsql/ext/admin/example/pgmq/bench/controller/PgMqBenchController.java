package bronya.pgsql.ext.admin.example.pgmq.bench.controller;

import bronya.pgsql.ext.admin.example.pgmq.bench.service.PgMqBenchService;
import bronya.pgsql.ext.admin.example.pgmq.bench.service.PgMqBenchStats;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/demo/pgmq/bench")
@RequiredArgsConstructor
public class PgMqBenchController {
    private final PgMqBenchService benchService;
    private final PgMqBenchStats stats;

    @GetMapping("/start")
    public PgMqBenchStats.Snapshot start(
        @RequestParam(defaultValue = "10000") long totalMessages,
        @RequestParam(defaultValue = "20") int threads,
        @RequestParam(defaultValue = "32") int payloadSize,
        @RequestParam(defaultValue = "0") int delaySeconds
    ) {
        return benchService.start(totalMessages, threads, payloadSize, delaySeconds);
    }

    @GetMapping("/stats")
    public PgMqBenchStats.Snapshot stats() {
        return stats.snapshot();
    }

    @GetMapping("/reset")
    public PgMqBenchStats.Snapshot reset() {
        stats.reset();
        return stats.snapshot();
    }

    @GetMapping("/config")
    public PgMqBenchStats.Snapshot config(
        @RequestParam(defaultValue = "0") int consumerSleepMs,
        @RequestParam(defaultValue = "0") int consumerFailRatePercent
    ) {
        stats.setConsumerSleepMs(consumerSleepMs);
        stats.setConsumerFailRatePercent(consumerFailRatePercent);
        return stats.snapshot();
    }
}

