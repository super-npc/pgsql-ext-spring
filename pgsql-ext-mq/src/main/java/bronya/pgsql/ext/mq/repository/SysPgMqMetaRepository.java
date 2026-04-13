package bronya.pgsql.ext.mq.repository;

import lombok.RequiredArgsConstructor;
import com.baomidou.mybatisplus.extension.repository.CrudRepository;
import bronya.pgsql.ext.mq.domain.SysPqMeta;
import bronya.pgsql.ext.mq.mapper.SysPgMqMetaMapper;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class SysPgMqMetaRepository extends CrudRepository<SysPgMqMetaMapper, SysPqMeta> {
}
