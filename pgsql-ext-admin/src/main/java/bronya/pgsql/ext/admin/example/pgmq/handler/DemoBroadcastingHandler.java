package bronya.pgsql.ext.admin.example.pgmq.handler;

import bronya.pgsql.ext.admin.example.pgmq.handler.dto.DemoPgMqBroadcastingDto;
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
public class DemoBroadcastingHandler {

    @PgMqListener(subscribeType = SubscribeType.BROADCASTING, bindMsgDto = DemoPgMqBroadcastingDto.class)
    public void listener1(DemoPgMqBroadcastingDto msg){
        log.info("广播.listener1:{}",msg);
    }

    @PgMqListener(subscribeType = SubscribeType.BROADCASTING, bindMsgDto = DemoPgMqBroadcastingDto.class)
    public void listener2(DemoPgMqBroadcastingDto msg){
        log.info("广播.listener2:{}",msg);
    }
}
