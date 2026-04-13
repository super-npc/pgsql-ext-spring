package bronya.pgsql.ext.mq.repository;

import lombok.RequiredArgsConstructor;
import com.baomidou.mybatisplus.extension.repository.CrudRepository;
import bronya.pgsql.ext.mq.domain.SysPqArchive;
import bronya.pgsql.ext.mq.mapper.SysPqArchiveMapper;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class SysPqArchiveRepository extends CrudRepository<SysPqArchiveMapper, SysPqArchive> {
}
