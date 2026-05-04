package bronya.pgsql.ext.mq.constant;

import bronya.pgsql.ext.mq.annotation.PgMqListener.SubscribeType;
import bronya.pgsql.ext.mq.consumer.QueueConsumer;
import com.google.common.collect.TreeBasedTable;

import java.util.List;

public interface PgmqFinalConstant {

    /**
     * 消费者分组表
     * rowKey: SubscribeType (订阅类型)
     * columnKey: String (msgType 消息类型全限定名)
     * value: List<QueueConsumer> (封装后的客户端实例)
     */
    TreeBasedTable<SubscribeType, String, List<QueueConsumer>> consumerTable = TreeBasedTable.create();
}
