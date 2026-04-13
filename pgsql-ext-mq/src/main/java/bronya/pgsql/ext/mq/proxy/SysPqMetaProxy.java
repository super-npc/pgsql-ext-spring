package bronya.pgsql.ext.mq.proxy;

import bronya.admin.module.db.amis.util.BronyaAdminBaseAmisUtil;
import bronya.shared.util.Md;
import cn.hutool.v7.core.date.DateUtil;
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
import java.util.Date;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class SysPqMetaProxy extends DataProxy<SysPqMeta> {
    private final SysPqMetaRepository sysPqMetaRepository;
    private final PgMqService pgMqService;

    @Override
    public void table(Map<String, Object> map) {
        SysPqMeta sysPqMeta = BeanUtil.toBean(BronyaAdminBaseAmisUtil.map2obj(map), SysPqMeta.class);
        SysPqMetaExt sysPqMetaExt = new SysPqMetaExt();

        try {
            MetricResult metrics = pgMqService.metrics(sysPqMeta.getQueueName());
            Md md = new Md();
            md.appendLn(new Quote("统计数据"));
            md.appendLn("- 名称:{}",metrics.queueName());
            md.appendLn("- 队列长度:{}",metrics.queueLength());
            md.appendLn("- 最新消息年龄(秒):{}",DateUtil.formatBetween(metrics.newestMsgAgeSec() * 1000L));
            md.appendLn("- 最旧消息年龄(秒):{}",DateUtil.formatBetween(metrics.oldestMsgAgeSec() * 1000L));
            md.appendLn("- 消息总数:{}",metrics.totalMessages());
            md.appendLn("- 统计时间:{}",DateUtil.formatDateTime(Date.from(metrics.scrapeTime().toInstant())));
            md.appendLn("- 可见消息数:{}",metrics.queueVisibleLength());

            sysPqMetaExt.setMetrics(md.toString());
        } catch (SQLException e) {
            log.warn("获取队列指标异常",e);
        }

        // 配置拓展属性信息
        map.putAll(BronyaAdminBaseAmisUtil.obj2map(SysPqMeta.class, sysPqMetaExt));
    }
}
