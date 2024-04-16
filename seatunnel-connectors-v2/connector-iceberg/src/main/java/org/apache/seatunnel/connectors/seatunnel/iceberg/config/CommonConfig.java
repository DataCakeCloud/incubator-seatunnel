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

package org.apache.seatunnel.connectors.seatunnel.iceberg.config;

import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HADOOP;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HIVE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Getter;
import lombok.ToString;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigList;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import java.beans.Transient;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
@ToString
public class CommonConfig implements Serializable {
    private static final long serialVersionUID = 239821141534421580L;

    public static final Option<String> KEY_CATALOG_NAME = Options.key("catalog_name")
        .stringType()
        .noDefaultValue()
        .withDescription(" the iceberg catalog name");

    public static final Option<IcebergCatalogType> KEY_CATALOG_TYPE = Options.key("catalog_type")
        .enumType(IcebergCatalogType.class)
        .noDefaultValue()
        .withDescription(" the iceberg catalog type");

    public static final Option<String> KEY_NAMESPACE = Options.key("namespace")
        .stringType()
        .noDefaultValue()
        .withDescription(" the iceberg namespace");

    public static final Option<String> KEY_TABLE = Options.key("table")
        .stringType()
        .noDefaultValue()
        .withDescription(" the iceberg table");

    public static final Option<String> KEY_URI = Options.key("uri")
        .stringType()
        .noDefaultValue()
        .withDescription(" the iceberg server uri");

    public static final Option<String> KEY_WAREHOUSE = Options.key("warehouse")
        .stringType()
        .noDefaultValue()
        .withDescription(" the iceberg warehouse");

    public static final Option<Boolean> KEY_CASE_SENSITIVE = Options.key("case_sensitive")
        .booleanType()
        .defaultValue(false)
        .withDescription(" the iceberg case_sensitive");

    public static final Option<List<String>> KEY_FIELDS = Options.key("fields")
        .listType()
        .noDefaultValue()
        .withDescription(" the iceberg table fields");

    public static final Option<List<String>> KEY_HADOOP_CONF = Options.key("hadoopConf")
            .listType()
            .noDefaultValue()
            .withDescription("the hadoop config list");

    private String catalogName;
    private IcebergCatalogType catalogType;
    private String uri;
    private String warehouse;
    private String namespace;
    private String table;
    private SerializableConfiguration hadoopConf;
    private boolean caseSensitive;

    public CommonConfig(Config pluginConfig) {
        String catalogType = checkArgumentNotNull(pluginConfig.getString(KEY_CATALOG_TYPE.key()));
        checkArgument(
                HADOOP.getType().equals(catalogType)
                        || HIVE.getType().equals(catalogType),
                "Illegal catalogType: " + catalogType);

        this.catalogType = IcebergCatalogType.valueOf(catalogType.toUpperCase());
        this.catalogName = checkArgumentNotNull(pluginConfig.getString(KEY_CATALOG_NAME.key()));
        if (pluginConfig.hasPath(KEY_URI.key())) {
            this.uri = checkArgumentNotNull(pluginConfig.getString(KEY_URI.key()));
        }
        this.warehouse = checkArgumentNotNull(pluginConfig.getString(KEY_WAREHOUSE.key()));
        this.namespace = checkArgumentNotNull(pluginConfig.getString(KEY_NAMESPACE.key()));
        this.table = checkArgumentNotNull(pluginConfig.getString(KEY_TABLE.key()));

        if (pluginConfig.hasPath(KEY_CASE_SENSITIVE.key())) {
            this.caseSensitive = pluginConfig.getBoolean(KEY_CASE_SENSITIVE.key());
        }
        if (pluginConfig.hasPath(KEY_HADOOP_CONF.key())) {
            Config hadoopConfDetail = pluginConfig.getConfig(KEY_HADOOP_CONF.key());
            Iterator<Map.Entry<String, ConfigValue>> iterator = hadoopConfDetail.entrySet().iterator();
            Configuration conf = new Configuration();
            while (iterator.hasNext()){
                Map.Entry<String, ConfigValue> keyValue = iterator.next();
                conf.set(keyValue.getKey(),keyValue.getValue().unwrapped().toString());
            }
            this.hadoopConf = new SerializableConfiguration(conf);
        }else{
            this.hadoopConf = new SerializableConfiguration(new Configuration());
        }
    }

    protected <T> T checkArgumentNotNull(T argument) {
        checkNotNull(argument);
        return argument;
    }
}
