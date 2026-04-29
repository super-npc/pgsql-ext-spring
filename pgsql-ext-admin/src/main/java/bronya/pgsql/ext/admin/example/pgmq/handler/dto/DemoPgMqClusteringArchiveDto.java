package bronya.pgsql.ext.admin.example.pgmq.handler.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DemoPgMqClusteringArchiveDto implements Serializable {
    private String name;
}
