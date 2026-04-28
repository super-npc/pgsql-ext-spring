package bronya.pgsql.ext.mq.autoconfig;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

@Configuration
@ConfigurationProperties(prefix = "pgmq")
@Data
@Validated // 启用配置合法性校验
public class PgMqYaml {
    /**
     * 是否启用消费者，默认 true（启动时自动消费队列消息）
     */
    private boolean consumerEnabled = true;
}
