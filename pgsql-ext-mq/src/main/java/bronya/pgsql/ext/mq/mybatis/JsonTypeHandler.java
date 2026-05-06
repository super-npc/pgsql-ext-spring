package bronya.pgsql.ext.mq.mybatis;

import bronya.pgsql.ext.mq.domain.Json;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class JsonTypeHandler extends BaseTypeHandler<Json> {
    @Override
    public void setNonNullParameter(PreparedStatement ps, int i, Json parameter, JdbcType jdbcType) throws SQLException {
        ps.setObject(i, parameter.value(), Types.OTHER);
    }

    @Override
    public Json getNullableResult(ResultSet rs, String columnName) throws SQLException {
        return toJson(rs.getObject(columnName));
    }

    @Override
    public Json getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
        return toJson(rs.getObject(columnIndex));
    }

    @Override
    public Json getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
        return toJson(cs.getObject(columnIndex));
    }

    private static Json toJson(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String s) {
            return new Json(s);
        }
        return new Json(value.toString());
    }
}
