//package demo.one.module.pgmq.handler;
//
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import bronya.pgsql.ext.mq.annotation.PgMqDeadLetterConfig;
//import bronya.pgsql.ext.mq.annotation.PgMqListener;
//import org.springframework.stereotype.Component;
//
//import java.io.Serializable;
//
///**
// * PgMqListener 新配置方式演示
// */
//@Slf4j
//@Component
//@RequiredArgsConstructor
//public class CloudDemoPgMqHandler {
//
//    /**
//     * 场景1：启用死信队列，使用自定义重试配置（高可靠场景）
//     * - 启用死信队列（默认启用）
//     * - 最大重试 5 次
//     * - 第一次重试延迟 30 秒，采用指数退避策略
//     */
//    @PgMqListener(
//        subscribeType = PgMqListener.SubscribeType.CLUSTERING,
//        bindMsgDto = OrderMessage.class,
//        deadLetterConfig = @PgMqDeadLetterConfig(enableDlq = true, maxRetry = 5, retryDelaySeconds = 30)
//    )
//    public void processOrder(OrderMessage order) {
//        log.info("处理订单消息: {}", order);
//        // 实现订单处理逻辑
//    }
//
//    /**
//     * 场景2：启用死信队列，使用默认重试配置（普通场景）
//     * - 启用死信队列（默认启用）
//     * - 最大重试 3 次（默认值）
//     * - 立即重试（默认值）
//     */
//    @PgMqListener(
//        subscribeType = PgMqListener.SubscribeType.BROADCASTING,
//        bindMsgDto = NotificationMessage.class
//        // deadLetterConfig 使用所有默认值：enableDlq=true, maxRetry=3, retryDelaySeconds=0
//    )
//    public void sendNotification(NotificationMessage notification) {
//        log.info("发送通知消息: {}", notification);
//        // 实现通知发送逻辑
//    }
//
//    /**
//     * 场景3：禁用死信队列（简单场景）
//     * - 禁用死信队列
//     * - 重试配置不会生效
//     */
//    @PgMqListener(
//        subscribeType = PgMqListener.SubscribeType.CLUSTERING,
//        bindMsgDto = LogMessage.class,
//        deadLetterConfig = @PgMqDeadLetterConfig(enableDlq = false)
//    )
//    public void logMessage(LogMessage logMsg) {
//        log.info("记录日志消息: {}", logMsg);
//        // 实现日志记录逻辑
//    }
//
//    /**
//     * 场景4：实时处理，快速失败
//     * - 启用死信队列
//     * - 最大重试 2 次
//     * - 立即重试
//     * - 适合实时性要求高的场景
//     */
//    @PgMqListener(
//        subscribeType = PgMqListener.SubscribeType.CLUSTERING,
//        bindMsgDto = RealTimeMessage.class,
//        deadLetterConfig = @PgMqDeadLetterConfig(enableDlq = true, maxRetry = 2, retryDelaySeconds = 0)
//    )
//    public void processRealTime(RealTimeMessage message) {
//        log.info("处理实时消息: {}", message);
//        // 实现实时处理逻辑
//    }
//
//    // 消息DTO定义
//
//    public static class OrderMessage implements Serializable {
//        private String orderId;
//        private Double amount;
//        // getters/setters...
//        @Override
//        public String toString() {
//            return "OrderMessage{orderId='" + orderId + "', amount=" + amount + "}";
//        }
//    }
//
//    public static class NotificationMessage implements Serializable {
//        private String userId;
//        private String content;
//        // getters/setters...
//        @Override
//        public String toString() {
//            return "NotificationMessage{userId='" + userId + "', content='" + content + "'}";
//        }
//    }
//
//    public static class LogMessage implements Serializable {
//        private String level;
//        private String message;
//        // getters/setters...
//        @Override
//        public String toString() {
//            return "LogMessage{level='" + level + "', message='" + message + "'}";
//        }
//    }
//
//    public static class RealTimeMessage implements Serializable {
//        private String eventType;
//        private String data;
//        // getters/setters...
//        @Override
//        public String toString() {
//            return "RealTimeMessage{eventType='" + eventType + "', data='" + data + "'}";
//        }
//    }
//}