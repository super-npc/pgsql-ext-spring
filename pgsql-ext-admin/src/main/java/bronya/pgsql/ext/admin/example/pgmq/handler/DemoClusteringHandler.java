package bronya.pgsql.ext.admin.example.pgmq.handler;

import bronya.pgsql.ext.admin.example.pgmq.handler.dto.DemoPgMqClusteringDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import bronya.pgsql.ext.mq.annotation.PgMqListener;
import bronya.pgsql.ext.mq.annotation.PgMqListener.SubscribeType;
import org.springframework.stereotype.Component;

/**
 * 集群示例
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DemoClusteringHandler {

    @PgMqListener(subscribeType = SubscribeType.CLUSTERING, bindMsgDto = DemoPgMqClusteringDto.class)
    public void listener1(DemoPgMqClusteringDto msg) {
        log.info("集群.listener1:{}", msg);
    }

    @PgMqListener(subscribeType = SubscribeType.CLUSTERING, bindMsgDto = DemoPgMqClusteringDto.class)
    public void listener2(DemoPgMqClusteringDto msg) {
        log.info("集群.listener2:{}", msg);
    }
}
