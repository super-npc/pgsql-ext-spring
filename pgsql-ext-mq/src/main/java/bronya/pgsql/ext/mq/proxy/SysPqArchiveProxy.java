package bronya.pgsql.ext.mq.proxy;

import bronya.admin.module.amis.util.BronyaAdminBaseAmisUtil;
import bronya.pgsql.ext.mq.util.SysPgMqUtil;
import bronya.pgsql.ext.mq.domain.SysPqArchive;
import bronya.pgsql.ext.mq.domain.SysPqArchive.SysPqArchiveExt;
import bronya.pgsql.ext.mq.repository.SysPqArchiveRepository;
import bronya.core.base.dto.DataProxy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class SysPqArchiveProxy extends DataProxy<SysPqArchive> {
    private final SysPqArchiveRepository sysPqArchiveRepository;

    @Override
    public void table(Map<String, Object> map) {
        BronyaAdminBaseAmisUtil.fillTableExt(map, SysPqArchive.class, SysPqArchiveExt::new, (sysPqArchive, sysPqArchiveExt) -> {
            sysPqArchive.setMessage(SysPgMqUtil.parseValue(sysPqArchive.getMessage()));
            sysPqArchive.setHeaders(SysPgMqUtil.parseValue(sysPqArchive.getHeaders()));
            BronyaAdminBaseAmisUtil.mergeExt(map, SysPqArchive.class, sysPqArchive);
        });
    }
}
