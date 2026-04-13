package bronya.pgsql.ext.mq.controller;

import bronya.admin.model.annotation.AmisIds;
import bronya.admin.module.db.amis.dto.AmisIdsDto;
import cn.hutool.v7.core.lang.Assert;
import cn.hutool.v7.core.map.MapUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import bronya.pgsql.ext.mq.domain.SysPqMeta;
import bronya.pgsql.ext.mq.repository.SysPqMetaRepository;
import bronya.pgsql.ext.mq.service.PgMqService;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/admin/pqmq/meta")
@RequiredArgsConstructor
public class SysPqMetaController{
    private final SysPqMetaRepository metaRepository;
    private final PgMqService pgMqService;

    @GetMapping("/drop")
    public boolean drop(@AmisIds AmisIdsDto idsDto) {
        List<SysPqMeta> sysPqMetas = metaRepository.listByIds(idsDto.getIds());
        for (SysPqMeta sysPqMeta : sysPqMetas) {
            pgMqService.drop(sysPqMeta.getQueueName());
        }
        return true;
    }

    @GetMapping("/purge")
    public boolean purge(@AmisIds AmisIdsDto idsDto) {
        List<SysPqMeta> sysPqMetas = metaRepository.listByIds(idsDto.getIds());
        for (SysPqMeta sysPqMeta : sysPqMetas) {
            pgMqService.purge(sysPqMeta.getQueueName());
        }
        return true;
    }

    @PostMapping("/create-queue")
    public boolean createQueue(@AmisIds AmisIdsDto idsDto, @RequestBody Map<String,Object> body) {
        String queueName = MapUtil.getStr(body, "SysPqMeta__queueName");
        Assert.notBlank(queueName,"请输入队列名称");
        pgMqService.create(queueName);
        return true;
    }

    @PostMapping("/create-unlogged-queue")
    public boolean createUnloggedQueue(@AmisIds AmisIdsDto idsDto, @RequestBody Map<String,Object> body) {
        String queueName = MapUtil.getStr(body, "SysPqMeta__queueName");
        Assert.notBlank(queueName,"请输入队列名称");
        pgMqService.createUnlogged(queueName);
        return true;
    }
}
