package bronya.pgsql.ext.mq.repository;

import lombok.RequiredArgsConstructor;
import com.baomidou.mybatisplus.extension.repository.CrudRepository;
import bronya.pgsql.ext.mq.domain.SysPqQueue;
import bronya.pgsql.ext.mq.mapper.SysPqQueueMapper;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class SysPqQueueRepository extends CrudRepository<SysPqQueueMapper, SysPqQueue> {
}
