package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.ck;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.type.*;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

@Slf4j
public class ClickhouseTypeMapper implements JdbcDialectTypeMapper {

    /* ============================ data types ===================== */
    private static final String CLICKHOUSE_SMALLINT = "SMALLINT";
    private static final String CLICKHOUSE_UINT8 = "UINT8";
    private static final String CLICKHOUSE_UINT16 = "UINT16";
    private static final String CLICKHOUSE_UINT32 = "UINT32";
    private static final String CLICKHOUSE_UINT64 = "UINT64";
    private static final String CLICKHOUSE_UINT128 = "UINT128";
    private static final String CLICKHOUSE_UINT256 = "UINT256";
    private static final String CLICKHOUSE_INT32 = "INT32";
    private static final String CLICKHOUSE_INT16 = "INT16";
    private static final String CLICKHOUSE_INT64 = "INT64";
    private static final String CLICKHOUSE_INT128 = "INT128";
    private static final String CLICKHOUSE_INT256 = "INT256";

    private static final String CLICKHOUSE_BIGINT = "BIGINT";
    private static final String CLICKHOUSE_INT8 = "INT8";

    private static final String CLICKHOUSE_DECIMAL = "DECIMAL";
    private static final String CLICKHOUSE_NUMERIC = "NUMERIC";
    private static final String CLICKHOUSE_REAL = "REAL";
    private static final String CLICKHOUSE_FLOAT32 = "FLOAT32";
    private static final String CLICKHOUSE_DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String CLICKHOUSE_FLOAT64 = "FLOAT64";
    private static final String CLICKHOUSE_FLOAT = "FLOAT";

    private static final String CLICKHOUSE_BOOLEAN = "BOOLEAN";
    private static final String CLICKHOUSE_BOOL = "BOOL";

    private static final String CLICKHOUSE_LONGBLOB = "LONGBLOB";
    private static final String CLICKHOUSE_LONGTEXT = "LONGTEXT";
    private static final String CLICKHOUSE_TINYTEXT = "TINYTEXT";
    private static final String CLICKHOUSE_TEXT = "TEXT";
    private static final String CLICKHOUSE_CHAR = "CHAR";

    private static final String CLICKHOUSE_STRING = "STRING";
    private static final String CLICKHOUSE_VARCHAR = "VARCHAR";
    private static final String CLICKHOUSE_MEDIUMBLOB = "MEDIUMBLOB";
    private static final String CLICKHOUSE_BLOB = "BLOB";
    private static final String CLICKHOUSE_TINYBLOB = "TINYBLOB";
    private static final String CLICKHOUSE_MEDIUMTEXT = "MEDIUMTEXT";

    private static final String CLICKHOUSE_DATE = "DATE";
    private static final String CLICKHOUSE_DATE32 = "DATE32";
    /*FIXME*/

    private static final String CLICKHOUSE_GEOMETRY = "GEOMETRY";
    private static final String CLICKHOUSE_OID = "OID";
    private static final String CLICKHOUSE_SUPER = "SUPER";

    private static final String CLICKHOUSE_TIME = "TIME";
    private static final String CLICKHOUSE_TIME_WITH_TIME_ZONE = "TIME WITH TIME ZONE";

    private static final String CLICKHOUSE_TIMETZ = "TIMETZ";
    private static final String CLICKHOUSE_TIMESTAMP = "TIMESTAMP";
    private static final String CLICKHOUSE_TIMESTAMP_WITH_OUT_TIME_ZONE =
            "TIMESTAMP WITHOUT TIME ZONE";

    private static final String CLICKHOUSE_TIMESTAMPTZ = "TIMESTAMPTZ";
    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String clichouseType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (clichouseType) {
            case CLICKHOUSE_SMALLINT:
                return BasicType.SHORT_TYPE;
            case CLICKHOUSE_UINT128:
            case CLICKHOUSE_UINT256:
            case CLICKHOUSE_UINT8:
            case CLICKHOUSE_UINT16:
            case CLICKHOUSE_UINT32:
            case CLICKHOUSE_UINT64:
            case CLICKHOUSE_INT256:
            case CLICKHOUSE_INT16:
            case CLICKHOUSE_INT32:
            case CLICKHOUSE_INT64:
            case CLICKHOUSE_INT128:
                return BasicType.INT_TYPE;
            case CLICKHOUSE_BIGINT:
            case CLICKHOUSE_INT8:
            case CLICKHOUSE_OID:
                return BasicType.LONG_TYPE;
            case CLICKHOUSE_DECIMAL:
                return new DecimalType(precision, scale);
            case CLICKHOUSE_FLOAT32:
                return BasicType.FLOAT_TYPE;
            case CLICKHOUSE_FLOAT64:
                return BasicType.DOUBLE_TYPE;
            case CLICKHOUSE_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case CLICKHOUSE_CHAR:
            case CLICKHOUSE_STRING:
            case CLICKHOUSE_LONGBLOB:
            case CLICKHOUSE_LONGTEXT:
            case CLICKHOUSE_TINYTEXT:
            case CLICKHOUSE_VARCHAR:
            case CLICKHOUSE_MEDIUMBLOB:
            case CLICKHOUSE_BLOB:
            case CLICKHOUSE_TINYBLOB:
            case CLICKHOUSE_MEDIUMTEXT:
            case CLICKHOUSE_TEXT:
            case CLICKHOUSE_SUPER:
                return BasicType.STRING_TYPE;
            case CLICKHOUSE_DATE:
            case CLICKHOUSE_DATE32:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case CLICKHOUSE_GEOMETRY:
                return PrimitiveByteArrayType.INSTANCE;
            case CLICKHOUSE_TIME:
            case CLICKHOUSE_TIME_WITH_TIME_ZONE:
            case CLICKHOUSE_TIMETZ:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case CLICKHOUSE_TIMESTAMP:
            case CLICKHOUSE_TIMESTAMP_WITH_OUT_TIME_ZONE:
            case CLICKHOUSE_TIMESTAMPTZ:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support CLICKHOUSE type '%s' on column '%s'  yet.",
                                clichouseType, jdbcColumnName));
        }
    }
}
