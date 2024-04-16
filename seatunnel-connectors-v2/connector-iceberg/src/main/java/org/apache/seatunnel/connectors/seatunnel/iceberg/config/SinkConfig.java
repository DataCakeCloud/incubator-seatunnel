/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.iceberg.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class SinkConfig extends CommonConfig {
    private static final long serialVersionUID = -196561967575264253L;

    private int maxRow = 10000;

    private int maxDataSize = 1;

    private Long commitInterval = 1800L;

    private boolean partitionedFanoutEnabled = false;

    private String fileFormat = "parquet";

    private String mode = "append";

    public static final Option<String> KEY_MODE = Options.key("mode")
            .stringType()
            .defaultValue("append")
            .withDescription(" the iceberg sync method");

    public static final Option<Long> COMMIT_INTERVSL = Options.key("commit_interval")
            .longType()
            .defaultValue(1800L)
            .withDescription("iceberg commit interval");

    public SinkConfig(Config pluginConfig) {
        super(pluginConfig);
        if (pluginConfig.hasPath(KEY_MODE.key())) {
            this.mode = pluginConfig.getString(KEY_MODE.key());
        }
        if(pluginConfig.hasPath(COMMIT_INTERVSL.key())){
            this.commitInterval = pluginConfig.getLong(COMMIT_INTERVSL.key());
        }
    }

    public static SinkConfig loadConfig(Config pluginConfig) {
        return new SinkConfig(pluginConfig);
    }
}
