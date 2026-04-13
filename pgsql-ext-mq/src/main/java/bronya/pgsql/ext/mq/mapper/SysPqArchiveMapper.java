package bronya.pgsql.ext.mq.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import bronya.pgsql.ext.mq.domain.SysPqArchive;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface SysPqArchiveMapper extends BaseMapper<SysPqArchive> {
}
