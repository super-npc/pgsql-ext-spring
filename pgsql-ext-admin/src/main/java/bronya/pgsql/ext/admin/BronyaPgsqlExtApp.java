package bronya.pgsql.ext.admin;

import bronya.admin.common.util.MyLogUtil;
import bronya.admin.model.annotation.AmisScan;
import bronya.admin.model.annotation.EnableMySchedule;
import com.alibaba.arthas.spring.endpoints.ArthasEndPointAutoConfiguration;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.dromara.autotable.springboot.EnableAutoTable;
import org.jspecify.annotations.NonNull;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringApplicationRunListener;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;

import java.time.Duration;

@Slf4j
@AmisScan(moduleName = "furina模块", basePackage = {"bronya"})
@MapperScan({"bronya.**.mapper"})
@EnableAutoTable(basePackages = {"bronya.**.domain"})
@ComponentScan({"bronya"})
@RequiredArgsConstructor
@EnableMySchedule(excludeEnv = {"dev"})
@SpringBootApplication(exclude = ArthasEndPointAutoConfiguration.class)
@EnableAsync
public class BronyaPgsqlExtApp {

    static void main(String[] args) {
        Class<BronyaPgsqlExtApp> melodyAppPiAppClass = BronyaPgsqlExtApp.class;
        MyLogUtil.setLogPath(melodyAppPiAppClass);
        SpringApplication.withHook(springApplication -> new SpringApplicationRunListener() {
            @SneakyThrows
            @Override
            public void ready(@NonNull ConfigurableApplicationContext context, Duration timeTaken) {

            }

            @Override
            public void started(@NonNull ConfigurableApplicationContext context, Duration timeTaken) {
                SpringApplicationRunListener.super.started(context, timeTaken);
                log.info("--程序启动完成");
            }
        }, () -> {
            SpringApplication.run(melodyAppPiAppClass, args);
        });
    }
}
