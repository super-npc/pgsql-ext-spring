package bronya.pgsql.ext.mq.dynamic;

import lombok.Data;

@Data
public class PgMqCtx {
    private String queueName;
}
