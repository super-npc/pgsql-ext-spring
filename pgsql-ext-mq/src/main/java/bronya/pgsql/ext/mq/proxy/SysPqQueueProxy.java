package bronya.pgsql.ext.mq.proxy;

import bronya.admin.module.amis.util.BronyaAdminBaseAmisUtil;
import bronya.pgsql.ext.mq.util.SysPgMqUtil;
import bronya.pgsql.ext.mq.domain.SysPqQueue;
import bronya.pgsql.ext.mq.domain.SysPqQueue.SysPqQueueExt;
import bronya.pgsql.ext.mq.repository.SysPqQueueRepository;
import bronya.core.base.dto.DataProxy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class SysPqQueueProxy extends DataProxy<SysPqQueue> {
    private final SysPqQueueRepository sysPqQueueRepository;

    @Override
    public void table(Map<String, Object> map) {
        BronyaAdminBaseAmisUtil.fillTableExt(map, SysPqQueue.class, SysPqQueueExt::new, (sysPqQueue, sysPqQueueExt) -> {
            sysPqQueue.setMessage(SysPgMqUtil.parseValue(sysPqQueue.getMessage()));
            sysPqQueue.setHeaders(SysPgMqUtil.parseValue(sysPqQueue.getHeaders()));
            BronyaAdminBaseAmisUtil.mergeExt(map, SysPqQueue.class, sysPqQueue);
        });
    }
}
