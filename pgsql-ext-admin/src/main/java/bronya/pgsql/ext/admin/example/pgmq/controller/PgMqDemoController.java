package bronya.pgsql.ext.admin.example.pgmq.controller;

import bronya.pgsql.ext.admin.example.pgmq.handler.dto.DemoPgMqBroadcastingDto;
import bronya.pgsql.ext.admin.example.pgmq.handler.dto.DemoPgMqClusteringDto;
import cn.hutool.v7.core.util.RandomUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import bronya.pgsql.ext.mq.service.PgMqSender;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/demo/pgmq")
@RequiredArgsConstructor
public class PgMqDemoController {
    private final PgMqSender<DemoPgMqBroadcastingDto> broadcastingSender;
    private final PgMqSender<DemoPgMqClusteringDto> clusteringSender;

    @GetMapping("/send-broadcasting")
    public void sendBroadcasting(){
        List<Long> msgId = broadcastingSender.send(new DemoPgMqBroadcastingDto(RandomUtil.randomLetters(5)));
        log.info("测试发送广播:{}",msgId);
    }

    @GetMapping("/send-clustering")
    public void sendClustering(){
        List<Long> msgId = clusteringSender.send(new DemoPgMqClusteringDto(RandomUtil.randomLetters(5)));
        log.info("测试发送集群:{}",msgId);
    }
}
