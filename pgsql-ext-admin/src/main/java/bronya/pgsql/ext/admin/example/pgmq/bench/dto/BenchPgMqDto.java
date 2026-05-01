package bronya.pgsql.ext.admin.example.pgmq.bench.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BenchPgMqDto implements Serializable {
    private String traceId;
    private long seq;
    private String payload;
}

