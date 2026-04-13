package bronya.pgsql.ext;

import bronya.admin.model.annotation.AmisScan;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@AmisScan(moduleName = "pg.ext.mq组件", basePackage = {"bronya"})
@MapperScan({"bronya.**.mapper"})
@ComponentScan({"bronya"})
@SpringBootApplication
public class PgsqlExtMqMain {
}
