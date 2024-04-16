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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import com.teradata.jdbc.jdbc_4.logging.Log;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@AutoService(SeaTunnelSink.class)
public class JdbcSink
        implements SeaTunnelSink<SeaTunnelRow, JdbcSinkState, XidInfo, JdbcAggregatedCommitInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleJdbcConnectionProvider.class);

    private Config pluginConfig;

    private SeaTunnelRowType seaTunnelRowType;

    private JobContext jobContext;

    private JdbcSinkConfig jdbcSinkConfig;

    private JdbcDialect dialect;

    @Override
    public String getPluginName() {
        return "Jdbc";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        ReadonlyConfig config = ReadonlyConfig.fromConfig(pluginConfig);
        this.jdbcSinkConfig = JdbcSinkConfig.of(config);
        this.pluginConfig = pluginConfig;
        this.dialect = JdbcDialectLoader.load(jdbcSinkConfig.getJdbcConnectionConfig().getUrl());
        List<String> preSql = jdbcSinkConfig.getPreSql();
        if(preSql != null && preSql.size() > 0){
            Connection connection;
            try{
                connection = new SimpleJdbcConnectionProvider(jdbcSinkConfig.getJdbcConnectionConfig()).getOrEstablishConnection();
            }catch (SQLException e){
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            JdbcUtil.executeSqls(connection,jdbcSinkConfig.getPreSql());
            LOG.info(String.format("execute presql %s success", StringUtils.join(preSql,",")));
        }
    }

    @Override
    public SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> createWriter(SinkWriter.Context context)
            throws IOException {
        SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> sinkWriter;
        if (jdbcSinkConfig.isExactlyOnce()) {
            sinkWriter =
                    new JdbcExactlyOnceSinkWriter(
                            context,
                            jobContext,
                            dialect,
                            jdbcSinkConfig,
                            seaTunnelRowType,
                            new ArrayList<>());
        } else {
            sinkWriter = new JdbcSinkWriter(context, dialect, jdbcSinkConfig, seaTunnelRowType);
        }

        return sinkWriter;
    }

    @Override
    public SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> restoreWriter(
            SinkWriter.Context context, List<JdbcSinkState> states) throws IOException {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return new JdbcExactlyOnceSinkWriter(
                    context, jobContext, dialect, jdbcSinkConfig, seaTunnelRowType, states);
        }
        return SeaTunnelSink.super.restoreWriter(context, states);
    }

    @Override
    public Optional<SinkAggregatedCommitter<XidInfo, JdbcAggregatedCommitInfo>>
            createAggregatedCommitter() {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return Optional.of(new JdbcSinkAggregatedCommitter(jdbcSinkConfig));
        }
        return Optional.empty();
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public Optional<Serializer<JdbcAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return Optional.of(new DefaultSerializer<>());
        }
        return Optional.empty();
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public Optional<Serializer<XidInfo>> getCommitInfoSerializer() {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return Optional.of(new DefaultSerializer<>());
        }
        return Optional.empty();
    }
}
