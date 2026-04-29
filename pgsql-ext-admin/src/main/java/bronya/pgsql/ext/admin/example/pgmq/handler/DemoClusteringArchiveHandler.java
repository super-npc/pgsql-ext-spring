package bronya.pgsql.ext.admin.example.pgmq.handler;

import bronya.pgsql.ext.admin.example.pgmq.handler.dto.DemoPgMqClusteringArchiveDto;
import bronya.pgsql.ext.mq.annotation.PgMqDeadLetterConfig;
import bronya.pgsql.ext.mq.annotation.PgMqListener;
import bronya.pgsql.ext.mq.annotation.PgMqListener.SubscribeType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DemoClusteringArchiveHandler {

    @PgMqListener(
        subscribeType = SubscribeType.CLUSTERING,
        bindMsgDto = DemoPgMqClusteringArchiveDto.class,
        deadLetterConfig = @PgMqDeadLetterConfig(enableDlq = true, maxRetry = 1, retryDelaySeconds = 0)
    )
    public void listener(DemoPgMqClusteringArchiveDto msg) {
        log.info("集群.归档演示:{}", msg);
        throw new RuntimeException("模拟异常: 用于触发归档");
    }
}
