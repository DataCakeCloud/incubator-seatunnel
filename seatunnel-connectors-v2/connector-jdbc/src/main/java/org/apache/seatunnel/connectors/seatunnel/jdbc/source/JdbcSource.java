/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.common.ActionFailException;
import org.apache.seatunnel.api.common.SeaTunnelPluginAction;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcInputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@AutoService(SeaTunnelSource.class)
public class JdbcSource
        implements SeaTunnelSource<SeaTunnelRow, JdbcSourceSplit, JdbcSourceState>,
                SupportParallelism,
                SupportColumnProjection, SeaTunnelPluginAction {
    protected static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);

    private JdbcSourceConfig jdbcSourceConfig;
    private SeaTunnelRowType typeInfo;

    private JdbcDialect jdbcDialect;
    private JdbcInputFormat inputFormat;
    private PartitionParameter partitionParameter;
    private JdbcConnectionProvider jdbcConnectionProvider;

    private String query;

    private static final String tableRegex = "\\b(?:from)\\b\\s+([\\S.]+)";

    private static final int SAMPLE_SIZE = 5;
    private static final String POSTGRES_DRIVER = "org.postgresql.Driver";
    private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static final String SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final String HANA_DRIVER = "com.sap.db.jdbc.Driver";
    private static final String REDSHIFT_DRIVER = "com.amazon.redshift.jdbc.Driver";

    @Override
    public String getPluginName() {
        return "Jdbc";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        ReadonlyConfig config = ReadonlyConfig.fromConfig(pluginConfig);
        this.jdbcSourceConfig = JdbcSourceConfig.of(config);
        jdbcConnectionProvider =
                new SimpleJdbcConnectionProvider(jdbcSourceConfig.getJdbcConnectionConfig());
        query = jdbcSourceConfig.getQuery();
        jdbcDialect = JdbcDialectLoader.load(jdbcSourceConfig.getJdbcConnectionConfig().getUrl());
        try (Connection connection = jdbcConnectionProvider.getOrEstablishConnection()) {
            typeInfo = initTableField(connection);
            partitionParameter =
                    initPartitionParameterAndExtendSql(
                            jdbcConnectionProvider.getOrEstablishConnection());
        } catch (Exception e) {
            throw new PrepareFailException("jdbc", PluginType.SOURCE, e.toString());
        }

        inputFormat =
                new JdbcInputFormat(
                        jdbcConnectionProvider,
                        jdbcDialect,
                        typeInfo,
                        query,
                        jdbcSourceConfig.getFetchSize(),
                        jdbcSourceConfig.getJdbcConnectionConfig().isAutoCommit());
    }

    @Override
    public Boolean checkConnection() {
        return true;
    }

    private String getTableName(String query) {
        Pattern pattern = Pattern.compile(tableRegex, Pattern.CASE_INSENSITIVE);
        Matcher m = pattern.matcher(query);
        String table = "";
        while (m.find()) {
            if (m.group(1) != null) {
                table = m.group(1);
            }
        }
        return table;
    }

    @Override
    public List<Map<String, String>> getSchema() throws ActionFailException {
        ResultSet resultSet = null;
        ResultSet primaryKeys = null;
        ResultSet commentResultSet = null;
        List<Map<String, String>> schema = new ArrayList<>();
        try {
            jdbcConnectionProvider = new SimpleJdbcConnectionProvider(jdbcSourceConfig.getJdbcConnectionConfig());
            Connection connection = jdbcConnectionProvider.getOrEstablishConnection();

            String table = getTableName(query);
            String database = "";
            String driverName = jdbcSourceConfig.getJdbcConnectionConfig().getDriverName();
            if (driverName.equals(ORACLE_DRIVER) || driverName.equals(POSTGRES_DRIVER) || driverName.equals(HANA_DRIVER) || driverName.equals(REDSHIFT_DRIVER)) {
                String[] split = table.split("\\.");
                database = split[0];
                table = split[1];
            } else {
                database = connection.getCatalog();
            }

            HashMap<String, String> columnComment = new HashMap<>();
            if (driverName.equals(ORACLE_DRIVER)) {
                String sql = String.format("SELECT * FROM all_col_comments WHERE table_name = '%s'", table.toUpperCase());
                commentResultSet = connection.createStatement().executeQuery(sql);
                while (commentResultSet.next()) {
                    String columnName = commentResultSet.getString("COLUMN_NAME").toLowerCase();
                    String comment = commentResultSet.getString("COMMENTS");
                    columnComment.put(columnName, comment);
                }
            }
            if (driverName.equals(ORACLE_DRIVER) || driverName.equals(HANA_DRIVER)) {
                resultSet = connection.getMetaData().getColumns(database, null, table.toUpperCase(), null);
                primaryKeys = connection.getMetaData().getPrimaryKeys(database, null, table.toUpperCase());
            } else {
                if (driverName.equals(POSTGRES_DRIVER) || driverName.equals(REDSHIFT_DRIVER)) {
                    resultSet = connection.getMetaData().getColumns(connection.getCatalog(), database, table, null);
                }else {
                    resultSet = connection.getMetaData().getColumns(database, null, table, null);
                }

                try {
                    primaryKeys = connection.getMetaData().getPrimaryKeys(database, null, table);
                } catch (SQLException ex) {
                    int errorCode = ex.getErrorCode();
                    String sqlState = ex.getSQLState();
                    if (errorCode == 1105 || sqlState.equals("HY000")) {
                        // doris ENGINE = BROKER: errCode = 2, detailMessage = Table 'broker_test_create15' is not a OLAP table
                        LOG.error("doris failed to get schema:" + ex.getMessage());
                    }
                }
            }

            String primaryKey = null;
            if (primaryKeys != null) {
                while (primaryKeys.next()) {
                    primaryKey = primaryKeys.getString("COLUMN_NAME").toLowerCase();
                }
            }
            while (resultSet.next()) {
                Map<String, String> map = new HashMap<>();
                String columnName = resultSet.getString("COLUMN_NAME");
                map.put("name", columnName);
                map.put("type", resultSet.getString("TYPE_NAME"));
                map.put("size", String.valueOf(resultSet.getInt("COLUMN_SIZE")));

                String comment = "";
                if (driverName.equals(SQLSERVER_DRIVER)) {
                    commentResultSet = connection.createStatement().executeQuery(
                            "SELECT ep.value AS COMMENT " +
                                    "FROM sys.tables t " +
                                    "JOIN sys.columns c ON t.object_id = c.object_id " +
                                    "LEFT JOIN sys.extended_properties ep ON t.object_id = ep.major_id AND c.column_id = ep.minor_id AND ep.class = 1 AND ep.name = 'MS_Description' " +
                                    "WHERE t.name = '" + table + "' AND c.name = '" + columnName + "'"
                    );
                    if (commentResultSet.next()) {
                        comment = commentResultSet.getString("COMMENT");
                    }
                } else if (driverName.equals(ORACLE_DRIVER)) {
                    comment = columnComment.get(columnName);
                } else {
                     comment = resultSet.getString("REMARKS");
                }
                map.put("comment", comment);

                Boolean isPk = null;
                if (columnName.equalsIgnoreCase(primaryKey)){
                    isPk = true;
                }
                map.put("isPk", String.valueOf(isPk));
                schema.add(map);
            }
            return schema;
        } catch (Exception e) {
            throw new PrepareFailException("jdbc", PluginType.SOURCE, e.toString());
        } finally {
            try{
                if (primaryKeys != null) {
                    primaryKeys.close();
                }
                if (commentResultSet != null) {
                    commentResultSet.close();
                }

                assert resultSet != null;
                resultSet.close();
                jdbcConnectionProvider.closeConnection();
            } catch (Exception e) {
                throw new ActionFailException("jdbc", PluginType.SOURCE, e.toString());
            }
        }
    }

    @Override
    public List<Map<String, String>> getSample() throws ActionFailException {
        try{
            inputFormat.openInputFormat();
            inputFormat.open();
            List<Map<String, String>> sample = new ArrayList<>();
            int rowLength = typeInfo.getTotalFields();
            for(int i=0 ; i<SAMPLE_SIZE && !inputFormat.reachedEnd(); i++) {
                SeaTunnelRow seaTunnelRow = inputFormat.nextRecord();
                HashMap<String, String> row = new HashMap<>();
                for (int j=0; j<rowLength; j++) {
                    String column = null;
                    Object field = seaTunnelRow.getField(j);
                    if (field != null) {
                        column = field.toString();
                    }
                    row.put(typeInfo.getFieldName(j), column);
                }
                sample.add(row);
            }
            return sample;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try{
                inputFormat.closeInputFormat();
                jdbcConnectionProvider.closeConnection();
            } catch (Exception e) {
                throw new ActionFailException("jdbc", PluginType.SOURCE, e.toString());
            }
        }
    }

    @Override
    public List<String> getTables() throws ActionFailException {
        ResultSet resultSet = null;
        Statement statement = null;
        try {
            jdbcConnectionProvider = new SimpleJdbcConnectionProvider(jdbcSourceConfig.getJdbcConnectionConfig());
            Connection connection = jdbcConnectionProvider.getOrEstablishConnection();
            String driver = jdbcSourceConfig.getJdbcConnectionConfig().getDriverName();
            if (driver.equals(SQLSERVER_DRIVER) || driver.equals(POSTGRES_DRIVER)) {
                statement = connection.createStatement();
                String database = getTableName(query).split("\\.")[0];
                String query = String.format("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE  in ('BASE TABLE','VIEW') and table_schema = '%s'",database);
                resultSet = statement.executeQuery(query);
            } else if (driver.equals(ORACLE_DRIVER)) {
                statement = connection.createStatement();
                String database = getTableName(query).split("\\.")[0];
                String query = String.format("SELECT table_name FROM all_tables WHERE owner = '%s' ", database) +
                        " union " +
                        String.format("SELECT view_name as table_name FROM all_views WHERE owner = '%s' ", database);
                resultSet = statement.executeQuery(query);
            } else if(driver.equals(HANA_DRIVER)){
                statement = connection.createStatement();
                String database = getTableName(query).split("\\.")[0];
                query = String.format("SELECT TABLE_NAME FROM SYS.TABLES where SCHEMA_NAME = '%s' union " +
                        "SELECT view_name as table_name FROM SYS.VIEWS where  SCHEMA_NAME = '%s'",database,database);
                resultSet = statement.executeQuery(query);
            } else if (driver.equals(REDSHIFT_DRIVER)) {
                statement = connection.createStatement();
                String database = getTableName(query).split("\\.")[0];
                query = String.format("SELECT tablename as TABLE_NAME FROM pg_tables where schemaname = '%s' ",database);
                resultSet = statement.executeQuery(query);
            } else{
                resultSet = connection.getMetaData().getTables(connection.getCatalog(), null, null, new String[]{"TABLE","VIEW"});
            }

            List<String> tableNames = new ArrayList<>();
            while (resultSet.next()) {
                tableNames.add(resultSet.getString("TABLE_NAME"));
            }
            return tableNames;
        } catch (Exception e) {
            throw new PrepareFailException("jdbc", PluginType.SOURCE, e.toString());
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
                jdbcConnectionProvider.closeConnection();
            } catch (Exception e) {
                throw new ActionFailException("jdbc", PluginType.SOURCE, e.toString());
            }
        }
    }

    @Override
    public Boolean createTable() throws ActionFailException {
        try{
            jdbcConnectionProvider = new SimpleJdbcConnectionProvider(jdbcSourceConfig.getJdbcConnectionConfig());
            Connection connection = jdbcConnectionProvider.getOrEstablishConnection();
            connection.createStatement().execute(query);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try{
                inputFormat.closeInputFormat();
                jdbcConnectionProvider.closeConnection();
            } catch (Exception e) {
                throw new ActionFailException("jdbc", PluginType.SOURCE, e.toString());
            }
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return typeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, JdbcSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new JdbcSourceReader(inputFormat, readerContext);
    }

    @Override
    public Serializer<JdbcSourceSplit> getSplitSerializer() {
        return SeaTunnelSource.super.getSplitSerializer();
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> createEnumerator(
            SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext) throws Exception {
        return new JdbcSourceSplitEnumerator(
                enumeratorContext, jdbcSourceConfig, partitionParameter);
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext,
            JdbcSourceState checkpointState)
            throws Exception {
        return new JdbcSourceSplitEnumerator(
                enumeratorContext, jdbcSourceConfig, partitionParameter, checkpointState);
    }

    private SeaTunnelRowType initTableField(Connection conn) {
        JdbcDialectTypeMapper jdbcDialectTypeMapper = jdbcDialect.getJdbcDialectTypeMapper();
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {
            ResultSetMetaData resultSetMetaData =
                    jdbcDialect.getResultSetMetaData(conn, jdbcSourceConfig);
            if(resultSetMetaData == null) {
                return null;
            }
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                // Support AS syntax
                fieldNames.add(resultSetMetaData.getColumnLabel(i));
                seaTunnelDataTypes.add(jdbcDialectTypeMapper.mapping(resultSetMetaData, i));
            }
        } catch (Exception e) {
            LOG.warn("get row type info exception", e);
        }
        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]),
                seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[0]));
    }

    private PartitionParameter initPartitionParameter(String columnName, Connection connection)
            throws SQLException {
        long max = Long.MAX_VALUE;
        long min = Long.MIN_VALUE;
        if (jdbcSourceConfig.getPartitionLowerBound().isPresent()
                && jdbcSourceConfig.getPartitionUpperBound().isPresent()) {
            max = jdbcSourceConfig.getPartitionUpperBound().get();
            min = jdbcSourceConfig.getPartitionLowerBound().get();
            return new PartitionParameter(
                    columnName, min, max, jdbcSourceConfig.getPartitionNumber().orElse(null));
        }
        try (ResultSet rs =
                connection
                        .createStatement()
                        .executeQuery(
                                String.format(
                                        "SELECT MAX(%s),MIN(%s) " + "FROM (%s) tt",
                                        columnName, columnName, query))) {
            if (rs.next()) {
                max =
                        jdbcSourceConfig.getPartitionUpperBound().isPresent()
                                ? jdbcSourceConfig.getPartitionUpperBound().get()
                                : Long.parseLong(rs.getString(1));
                min =
                        jdbcSourceConfig.getPartitionLowerBound().isPresent()
                                ? jdbcSourceConfig.getPartitionLowerBound().get()
                                : Long.parseLong(rs.getString(2));
            }
        }
        return new PartitionParameter(
                columnName, min, max, jdbcSourceConfig.getPartitionNumber().orElse(null));
    }

    private PartitionParameter initPartitionParameterAndExtendSql(Connection connection)
            throws SQLException {
        if (jdbcSourceConfig.getPartitionColumn().isPresent()) {
            String partitionColumn = jdbcSourceConfig.getPartitionColumn().get();
            Map<String, SeaTunnelDataType<?>> fieldTypes = new HashMap<>();
            for (int i = 0; i < typeInfo.getFieldNames().length; i++) {
                fieldTypes.put(typeInfo.getFieldName(i), typeInfo.getFieldType(i));
            }
            if (!fieldTypes.containsKey(partitionColumn)) {
                throw new JdbcConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        String.format("field %s not contain in query %s", partitionColumn, query));
            }
            SeaTunnelDataType<?> partitionColumnType = fieldTypes.get(partitionColumn);
            if (!isNumericType(partitionColumnType)) {
                throw new JdbcConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        String.format("%s is not numeric type", partitionColumn));
            }
            PartitionParameter partitionParameter =
                    initPartitionParameter(partitionColumn, connection);
            query =
                    String.format(
                            "SELECT * FROM (%s) tt where "
                                    + partitionColumn
                                    + " >= ? AND "
                                    + partitionColumn
                                    + " <= ?",
                            query);

            return partitionParameter;
        } else {
            LOG.info(
                    "The partition_column parameter is not configured, and the source parallelism is set to 1");
        }

        return null;
    }

    private boolean isNumericType(SeaTunnelDataType<?> type) {
        return type.equals(BasicType.INT_TYPE) || type.equals(BasicType.LONG_TYPE);
    }
}
