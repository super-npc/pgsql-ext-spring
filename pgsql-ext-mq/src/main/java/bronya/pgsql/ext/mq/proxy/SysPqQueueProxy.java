package bronya.pgsql.ext.mq.proxy;

import bronya.admin.module.db.amis.util.BronyaAdminBaseAmisUtil;
import bronya.pgsql.ext.mq.util.SysPgMqUtil;
import bronya.pgsql.ext.mq.domain.SysPqQueue;
import bronya.pgsql.ext.mq.domain.SysPqQueue.SysPqQueueExt;
import bronya.pgsql.ext.mq.repository.SysPqQueueRepository;
import bronya.core.base.dto.DataProxy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import cn.hutool.v7.core.bean.BeanUtil;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class SysPqQueueProxy extends DataProxy<SysPqQueue> {
    private final SysPqQueueRepository sysPqQueueRepository;

    @Override
    public void table(Map<String, Object> map) {
        SysPqQueue sysPqQueue = BeanUtil.toBean(BronyaAdminBaseAmisUtil.map2obj(map), SysPqQueue.class);
        sysPqQueue.setMessage(SysPgMqUtil.parseValue(sysPqQueue.getMessage()));
        sysPqQueue.setHeaders(SysPgMqUtil.parseValue(sysPqQueue.getHeaders()));
        SysPqQueueExt sysPqQueueExt = new SysPqQueueExt();

        // 配置拓展属性信息
        map.putAll(BronyaAdminBaseAmisUtil.obj2map(SysPqQueue.class, sysPqQueue));
        map.putAll(BronyaAdminBaseAmisUtil.obj2map(SysPqQueue.class, sysPqQueueExt));
    }
}
