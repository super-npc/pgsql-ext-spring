package bronya.pgsql.ext.mq.proxy;

import bronya.admin.module.db.amis.util.BronyaAdminBaseAmisUtil;
import bronya.pgsql.ext.mq.annotation.PgMqDeadLetterConfig;
import bronya.pgsql.ext.mq.annotation.PgMqListener.SubscribeType;
import bronya.pgsql.ext.mq.autoconfig.PgMqBeanPostProcessor;
import bronya.pgsql.ext.mq.autoconfig.PgMqBeanPostProcessor.ListenerMethodInfo;
import bronya.shared.util.Md;
import cn.hutool.v7.core.date.DateUtil;
import cn.hutool.v7.core.text.StrUtil;
import cn.hutool.v7.core.text.split.SplitUtil;
import com.google.common.collect.TreeBasedTable;
import net.steppschuh.markdowngenerator.text.quote.Quote;
import bronya.pgsql.ext.mq.domain.SysPqMeta;
import bronya.pgsql.ext.mq.domain.SysPqMeta.SysPqMetaExt;
import bronya.pgsql.ext.mq.repository.SysPqMetaRepository;
import bronya.core.base.dto.DataProxy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import cn.hutool.v7.core.bean.BeanUtil;
import bronya.pgsql.ext.mq.service.PgMqService;
import org.andreaesposito.pgmq.jdbc.client.MetricResult;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class SysPqMetaProxy extends DataProxy<SysPqMeta> {
    private final SysPqMetaRepository sysPqMetaRepository;
    private final PgMqService pgMqService;
    private final PgMqBeanPostProcessor pgMqBeanPostProcessor;

    @Override
    public void table(Map<String, Object> map) {
        SysPqMeta sysPqMeta = BeanUtil.toBean(BronyaAdminBaseAmisUtil.map2obj(map), SysPqMeta.class);
        SysPqMetaExt sysPqMetaExt = new SysPqMetaExt();
        // 统计
        sysPqMetaExt.setMetrics(this.getMetrics(sysPqMeta));
        // 队列信息
        sysPqMetaExt.setInfo(this.getQueueInfo(sysPqMeta));

        // 配置拓展属性信息
        map.putAll(BronyaAdminBaseAmisUtil.obj2map(SysPqMeta.class, sysPqMetaExt));
    }

    private String getQueueInfo(SysPqMeta sysPqMeta){
        TreeBasedTable<SubscribeType, String, List<ListenerMethodInfo>> listenerTable = pgMqBeanPostProcessor.getListenerTable();
        List<ListenerMethodInfo> allListeners = listenerTable.values().stream().flatMap(List::stream).toList();
        List<ListenerMethodInfo> queues = allListeners.stream().filter(a -> a.queueName().equalsIgnoreCase(sysPqMeta.getQueueName())).toList();
        Md md = new Md();
        md.appendLn(new Quote("监听者实现数:"+queues.size()));
        for (ListenerMethodInfo queue : queues) {
            md.appendLn(StrUtil.format("- {} # {}", SplitUtil.split(queue.className(),".").getLast(),queue.methodName()));
            md.appendLn("  - 类型:{}",queue.subscribeType().getDesc());
            md.appendLn("  - 承载类:{}",queue.msgDtoClass().getSimpleName());
            md.appendLn("  - 批量拉取量:{}",queue.batchSize());
            md.appendLn("  - 可见性超时时间:{}秒",queue.visibilityTimeout());
            PgMqDeadLetterConfig pgMqDeadLetterConfig = queue.deadLetterConfig();
            md.appendLn("  - 开启死信:{}",Md.emojiBool(pgMqDeadLetterConfig.enableDlq()));
            if(pgMqDeadLetterConfig.enableDlq()){
                md.appendLn("    - 最大重试次数:{}",pgMqDeadLetterConfig.maxRetry());
                md.appendLn("    - 重试延迟时间:{}秒 (0立即重试)",pgMqDeadLetterConfig.retryDelaySeconds());
            }
        }
        return md.toString();
    }

    private String getMetrics(SysPqMeta sysPqMeta) {
        Md md = new Md();
        try {
            MetricResult metrics = pgMqService.metrics(sysPqMeta.getQueueName());
            md.appendLn(new Quote("统计数据"));
            md.appendLn("- 名称:{}", metrics.queueName());
            md.appendLn("- 总消息数:{}", metrics.totalMessages());
            md.appendLn("- 可见消息数:{}", metrics.queueVisibleLength());
            md.appendLn("- 队列长度:{}", metrics.queueLength());
            md.appendLn("- 最新消息:{}", DateUtil.formatBetween(metrics.newestMsgAgeSec() * 1000L));
            md.appendLn("- 最旧消息:{}", DateUtil.formatBetween(metrics.oldestMsgAgeSec() * 1000L));
            md.appendLn("- 统计时间:{}", DateUtil.formatDateTime(Date.from(metrics.scrapeTime().toInstant())));
        } catch (SQLException e) {
            log.warn("获取队列指标异常", e);
        }
        return md.toString();
    }
}
