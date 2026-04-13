package bronya.pgsql.ext.mq.repository;

import lombok.RequiredArgsConstructor;
import com.baomidou.mybatisplus.extension.repository.CrudRepository;
import bronya.pgsql.ext.mq.domain.SysPqMeta;
import bronya.pgsql.ext.mq.mapper.SysPqMetaMapper;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class SysPqMetaRepository extends CrudRepository<SysPqMetaMapper, SysPqMeta> {
}
