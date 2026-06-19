package bronya.pgsql.ext.mq.controller;

import bronya.admin.model.annotation.AmisIds;
import bronya.admin.module.amis.dto.AmisIdsDto;
import bronya.pgsql.ext.mq.domain.SysPqArchive;
import bronya.pgsql.ext.mq.domain.SysPqMeta;
import bronya.pgsql.ext.mq.repository.SysPqArchiveRepository;
import bronya.pgsql.ext.mq.repository.SysPqMetaRepository;
import bronya.pgsql.ext.mq.service.PgMqService;
import cn.hutool.v7.core.lang.Assert;
import cn.hutool.v7.core.map.MapUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/admin/pqmq/archive")
@RequiredArgsConstructor
public class SysPqArchiveController {
    private final SysPqArchiveRepository archiveRepository;
    private final PgMqService pgMqService;

    /**
     * 重放
     */
    @GetMapping("/replay")
    public boolean replay(@AmisIds AmisIdsDto idsDto) {
        List<SysPqArchive> archives = archiveRepository.listByIds(idsDto.getIds());
        for (SysPqArchive archive : archives) {

        }
        return true;
    }
}
