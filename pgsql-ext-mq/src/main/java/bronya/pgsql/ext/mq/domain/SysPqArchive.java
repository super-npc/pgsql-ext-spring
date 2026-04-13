package bronya.pgsql.ext.mq.domain;

import bronya.core.base.annotation.amis.AmisField;
import bronya.core.base.annotation.amis.AmisFieldView;
import bronya.core.base.annotation.amis.gencode.table.BindMany2One;
import bronya.core.base.annotation.amis.page.Amis;
import bronya.core.base.annotation.amis.page.Btns;
import bronya.core.base.annotation.amis.page.Menu;
import bronya.core.base.constant.AmisPage;
import bronya.core.base.menu.group.postgres;
import bronya.core.base.menu.module.系统;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.annotation.IdType;
import com.mybatisflex.core.constant.SqlOperator;
import lombok.Data;
import lombok.experimental.FieldNameConstants;
import bronya.pgsql.ext.mq.dynamic.PgMqADynamicTablePlan;
import bronya.pgsql.ext.mq.proxy.SysPqArchiveProxy;
import org.dromara.autotable.annotation.Ignore;
import org.dromara.autotable.annotation.mysql.MysqlTypeConstant;
import org.dromara.mpe.autotable.annotation.Column;
import org.dromara.mpe.autotable.annotation.ColumnId;
import org.dromara.mpe.autotable.annotation.Table;

import java.util.Date;

@Data
@Table(comment = "pg归档")
@Ignore
@FieldNameConstants
@Amis(dynamicTablePlan = PgMqADynamicTablePlan.class,ext = @Amis.ExpandField(extBean = SysPqArchive.SysPqArchiveExt.class,dataProxy = SysPqArchiveProxy.class))
@AmisPage(menu = @Menu(show = false, module = 系统.class, group = postgres.class, menu = "pg队列",order = 1),
        btns = @Btns(add = false, edit = false, delete = false))
public class SysPqArchive{
    @ColumnId(mode = IdType.INPUT, comment = "消息id")
    @AmisField(search = @AmisField.Search(operator = SqlOperator.EQUALS),sortable = true, copyable = true)
    private Long msgId;
    @Column(comment = "读数")
    @AmisField(sortable = true)
    private Integer readCt;
    @Column(comment = "入队")
    @AmisField
    private Date enqueuedAt;
    @Column(comment = "最后读取")
    @AmisField
    private Date lastReadAt;
    @Column(comment = "可见时间")
    @AmisField
    private Date vt;
    @Column(comment = "消息主体", type = MysqlTypeConstant.JSON)
    @AmisField(table = @AmisFieldView(type = AmisFieldView.ViewType.jsonSchema),
            detail = @AmisFieldView(type = AmisFieldView.ViewType.jsonSchema)
    )
    private JSONObject message;
    @Column(comment = "元数据", type = MysqlTypeConstant.JSON)
    @AmisField(table = @AmisFieldView(type = AmisFieldView.ViewType.jsonSchema),
            detail = @AmisFieldView(type = AmisFieldView.ViewType.jsonSchema)
    )
    private JSONObject headers;
    @Column(comment = "归档时间")
    @AmisField
    private Date archivedAt;

    @Column(notNull = true, comment = "队列")
    @BindMany2One(entity = SysPqMeta.class, valueField = SysPqQueue.Fields.msgId, labelField = SysPqMeta.Fields.queueName)
    @AmisField
    private Long pgmqArchive;

    @Data
    public static class SysPqArchiveExt{

    }
}
