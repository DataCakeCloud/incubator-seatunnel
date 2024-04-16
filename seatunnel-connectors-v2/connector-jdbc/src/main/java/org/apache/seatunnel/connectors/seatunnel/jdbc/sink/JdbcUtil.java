package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class JdbcUtil {
    public static void executeSqls(Connection conn, List<String> sqls) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            for (String sql : sqls) {
                executeSqlWithoutResultSet(stmt, sql);
            }
        } catch (Exception e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.PRESQL_EXECUTE_FAILED,
                    "presql execute failed",
                    e);
        } finally {
            closeDBResources(null, stmt, null);
        }
    }

    public static void executeSqlWithoutResultSet(Statement stmt, String sql)
            throws SQLException {
        stmt.execute(sql);
    }

    public static void closeDBResources(ResultSet rs, Statement stmt,
                                        Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException unused) {
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException unused) {
            }
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException unused) {
            }
        }
    }
}
