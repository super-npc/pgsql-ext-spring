package bronya.pgsql.ext.mq.dynamic;

import bronya.core.base.annotation.amis.db.req.One2ManyReq;
import bronya.shared.module.common.constant.ADynamicTablePlan;
import cn.hutool.v7.core.map.MapUtil;
import com.alibaba.cola.exception.BizException;
import bronya.pgsql.ext.mq.domain.SysPqArchive;
import bronya.pgsql.ext.mq.domain.SysPqQueue;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class PgMqADynamicTablePlan extends ADynamicTablePlan<PgMqCtx> {

    @Override
    public String getDynamicTableName(String tableName) {
        PgMqCtx pgMqCtx = super.getDynamicTableCtx().orElseThrow(() -> new BizException("未设置上下文队列名称"));
        return pgMqCtx.getQueueName();
    }

    @Override
    public void setDynamicTableCtxByAmisTable(Map<String, Object> map) {
        One2ManyReq one2ManyReq = MapUtil.get(map, "One2ManyReq", One2ManyReq.class);
        String entityField = one2ManyReq.getEntityField();
        PgMqCtx pgMqCtx = new PgMqCtx();
        if (SysPqArchive.Fields.pgmqArchive.equals(entityField)) {
            pgMqCtx.setQueueName("pgmq.a_"+one2ManyReq.getEntityFieldVal());
        }else if(SysPqQueue.Fields.pgQueue.equals(entityField)){
            pgMqCtx.setQueueName("pgmq.q_"+one2ManyReq.getEntityFieldVal());
        }
        super.setDynamicTableCtxByAmisTable(pgMqCtx);
    }

    @Override
    public boolean isWhereRef() {
        // 因为两张表没有关联的字段,强行关联会找不到字段
        return false;
    }
}

