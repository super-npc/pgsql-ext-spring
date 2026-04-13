package bronya.pgsql.ext.mq.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import bronya.pgsql.ext.mq.domain.SysPqMeta;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface SysPqMetaMapper extends BaseMapper<SysPqMeta> {
}
