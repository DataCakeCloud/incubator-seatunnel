package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.ck;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectFactory;


@AutoService(JdbcDialectFactory.class)
public class ClickhouseDialectFactory implements JdbcDialectFactory {
    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:clickhouse:");
    }

    @Override
    public JdbcDialect create() {
        return new ClickhouseDialect();
    }
}
