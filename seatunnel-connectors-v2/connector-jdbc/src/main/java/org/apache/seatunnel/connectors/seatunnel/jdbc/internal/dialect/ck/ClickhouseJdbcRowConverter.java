package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.ck;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;

public class ClickhouseJdbcRowConverter extends AbstractJdbcRowConverter {
    @Override
    public String converterName() {
        return "Clickhouse";
    }
}
