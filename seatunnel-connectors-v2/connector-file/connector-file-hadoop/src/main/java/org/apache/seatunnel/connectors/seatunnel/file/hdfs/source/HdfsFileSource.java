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

package org.apache.seatunnel.connectors.seatunnel.file.hdfs.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.seatunnel.api.common.ActionFailException;
import org.apache.seatunnel.api.common.SeaTunnelPluginAction;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.serialization.SerializableConfiguration;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.util.FileSystemUtil;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import java.beans.Transient;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@AutoService(SeaTunnelSource.class)
public class HdfsFileSource extends BaseHdfsFileSource implements SeaTunnelPluginAction{
    String fileFormat;
    String delimiter=null;
    SerializableConfiguration configuration;
    @Override
    public String getPluginName() {
        return FileSystemType.HDFS.getFileSystemPluginName();
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        super.prepare(pluginConfig);
        if(pluginConfig.hasPath(BaseSourceConfig.FILE_FORMAT_TYPE.key())){
            fileFormat = pluginConfig.getString(BaseSourceConfig.FILE_FORMAT_TYPE.key());
        }
        if (pluginConfig.hasPath(BaseSourceConfig.DELIMITER.key())) {
            delimiter = pluginConfig.getString(BaseSourceConfig.DELIMITER.key());
        }
        if (pluginConfig.hasPath(BaseSourceConfig.KEY_HADOOP_CONF.key())){
            Configuration hadoopConf = new Configuration();
            Config config = pluginConfig.getConfig(BaseSourceConfig.KEY_HADOOP_CONF.key());
            Iterator<Map.Entry<String, ConfigValue>> iterator = config.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String, ConfigValue> keyValue = iterator.next();
                hadoopConf.set(keyValue.getKey(),keyValue.getValue().unwrapped().toString());
            }
            configuration = new SerializableConfiguration(hadoopConf);
        }
    }

    @Override
    public Boolean checkConnection() {
        FileSystem fileSystem;
        try{
            fileSystem = FileSystem.get(configuration.get());
            fileSystem.close();
        } catch (IOException e) {
            log.error(e.getMessage());
            return false;
        }
        return true;
    }

    @Override
    public List<Map<String, String>> getSchema() throws ActionFailException {
        List<Map<String, String>> schema = new ArrayList<>();
        try {
            schema = FileSystemUtil.getSchema(fileFormat,readStrategy.getConfiguration(hadoopConf),
                        new Path(filePaths.get(0)), delimiter);
        }catch (Exception e) {
                log.error(e.getMessage());
                return new ArrayList<>();
        }
        return schema;
    }

    @Override
    public List<Map<String, String>> getSample() throws ActionFailException {
        SampleCollector sample = new SampleCollector(readStrategy,hadoopConf,filePaths.get(0));
        try{
            readStrategy.init(hadoopConf);
            readStrategy.read(filePaths.get(0),sample);
        }catch (Exception e){
            log.error(e.getMessage());
            log.error("读取数据异常！！！");
            return null;
        }
        List<Map<String, String>> result;
        List<Map<String, String>> rows = sample.getRows();
        if (rows.size()>5){
            result = rows.subList(0, 4);
        }else{
            result = rows;
        }
        return result;
    }

    @Override
    public List<String> getTables() throws ActionFailException {
        return null;
    }

    @Override
    public Boolean createTable() throws ActionFailException {
        return null;
    }

    public static class SampleCollector implements Collector<SeaTunnelRow> {
        SeaTunnelRowType seaTunnelRowTypeInfo;

        public SampleCollector(ReadStrategy readStrategy, HadoopConf hadoopConf,String path){
            seaTunnelRowTypeInfo = readStrategy.getSeaTunnelRowTypeInfo(
                    hadoopConf,path
            );
        }

        private final List<Map<String, String>> rows = new ArrayList<>();

        public List<Map<String, String>> getRows() {
            return rows;
        }

        @SuppressWarnings("checkstyle:RegexpSingleline")
        @Override
        public void collect(SeaTunnelRow record) {
            HashMap<String, String> row = new HashMap<>();
            for (int j=0; j<seaTunnelRowTypeInfo.getTotalFields(); j++) {
                String column = null;
                Object field = record.getField(j);
                if (field != null) {
                    column = field.toString();
                }
                row.put(seaTunnelRowTypeInfo.getFieldName(j), column);
            }
            rows.add(row);
        }

        @Override
        public Object getCheckpointLock() {
            return null;
        }
    }
}
