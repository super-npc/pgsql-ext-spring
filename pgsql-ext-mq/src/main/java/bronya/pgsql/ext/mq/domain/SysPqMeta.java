package bronya.pgsql.ext.mq.domain;

import bronya.core.base.annotation.amis.AmisField;
import bronya.core.base.annotation.amis.AmisFieldView;
import bronya.core.base.annotation.amis.gencode.table.BindOne2Many;
import bronya.core.base.annotation.amis.gencode.table.JoinCondition;
import bronya.core.base.annotation.amis.inputdata.AmisEditor;
import bronya.core.base.annotation.amis.inputdata.AmisSwitch;
import bronya.core.base.annotation.amis.page.Amis;
import bronya.core.base.annotation.amis.page.Btns;
import bronya.core.base.annotation.amis.page.Menu;
import bronya.core.base.annotation.amis.page.Operation;
import bronya.core.base.annotation.amis.type.editor.EditorLanguage;
import bronya.core.base.constant.AmisPage;
import bronya.core.base.menu.group.postgres;
import bronya.core.base.menu.module.系统;
import cn.hutool.v7.http.meta.Method;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.mybatisflex.core.constant.SqlOperator;
import lombok.Data;
import lombok.experimental.FieldNameConstants;
import bronya.pgsql.ext.mq.proxy.SysPqMetaProxy;
import org.dromara.autotable.annotation.Ignore;
import org.dromara.mpe.autotable.annotation.Column;
import org.dromara.mpe.autotable.annotation.ColumnId;
import org.dromara.mpe.autotable.annotation.Table;

import java.util.Date;
import java.util.List;

@Data
@Table(comment = "pg元数据", value = "pgmq.meta")
@Ignore
@FieldNameConstants
@Amis(ext = @Amis.ExpandField(extBean = SysPqMeta.SysPqMetaExt.class, dataProxy = SysPqMetaProxy.class))
@AmisPage(menu = @Menu(module = 系统.class, group = postgres.class, menu = "pg队列", order = 1),
        btns = @Btns(add = false, edit = false, delete = false),
        headerToolbar = @Operation(optBtns = {
                @Operation.OptBtn(name = "创建持久化队列", level = Operation.BtnLevelType.info, method = Method.POST, batch = true, confirmForm = SysPqMeta.SysPqMetaConfirm.class, url = "/admin/pqmq/meta/create-queue"),
                @Operation.OptBtn(name = "创建非持久化队列", level = Operation.BtnLevelType.info, method = Method.POST, batch = true, confirmForm = SysPqMeta.SysPqMetaConfirm.class,url = "/admin/pqmq/meta/create-unlogged-queue")
        }),
        operation = @Operation(optBtns = {
                @Operation.OptBtn(name = "删除队列", level = Operation.BtnLevelType.danger, method = Method.GET, batch = true, url = "/admin/pqmq/meta/drop"),
                @Operation.OptBtn(name = "清空队列", level = Operation.BtnLevelType.warning, method = Method.GET, batch = true, url = "/admin/pqmq/meta/purge")
        }))
public class SysPqMeta {
    @ColumnId(mode = IdType.INPUT, comment = "队列名称")
    @AmisField(search = @AmisField.Search(operator = SqlOperator.LIKE_LEFT), width = 150)
    private String queueName;

    @Column(comment = "分区")
    @AmisField(switchBool = @AmisSwitch.Switch(onText = "已分区", offText = "未分区"), search = @AmisField.Search(operator = SqlOperator.EQUALS), width = 150)
    private Boolean isPartitioned;

    @Column(comment = "类型")
    @AmisField(switchBool = @AmisSwitch.Switch(onText = "持久化", offText = "非持久化"), search = @AmisField.Search(operator = SqlOperator.EQUALS), width = 150)
    private Boolean isUnlogged;

    @Column(comment = "创建")
    @AmisField(width = 150)
    private Date createdAt;

    @Ignore
    @TableField(exist = false)
    @BindOne2Many(entity = SysPqArchive.class, condition = @JoinCondition(selfField = Fields.queueName,
            joinField = SysPqArchive.Fields.pgmqArchive, joinFieldLabel = SysPqArchive.Fields.msgId))
    private List<SysPqArchive> archives;

    @Ignore
    @TableField(exist = false)
    @BindOne2Many(entity = SysPqQueue.class, condition = @JoinCondition(selfField = Fields.queueName,
            joinField = SysPqQueue.Fields.pgQueue, joinFieldLabel = SysPqQueue.Fields.msgId))
    private List<SysPqQueue> queues;

    @Data
    public static class SysPqMetaExt {
        @AmisField(comment = "统计",
                editor = @AmisEditor.Editor(language = EditorLanguage.markdown, markdownRender = true),
                table = @AmisFieldView(type = AmisFieldView.ViewType.代码编辑器),
                detail = @AmisFieldView(type = AmisFieldView.ViewType.代码编辑器), width = 300)
        private String metrics;
    }


    @Data
    public static class SysPqMetaConfirm {
//        @AmisField(comment = "验证码", add = @AmisFieldView(type = AmisFieldView.ViewType.验证码))
//        private String code;

        @AmisField(comment = "队列名称")
        private String queueName;
    }
}
