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

package org.apache.seatunnel.example.flink.v2;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.exception.CommandException;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.flink.command.*;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class SeaTunnelActionExample {

    public static void main(String[] args)
            throws CommandException, UnsupportedEncodingException, FileNotFoundException, URISyntaxException {
//        String configurePath = args.length > 0 ? args[0] : "/examples/mongodb2console.conf";
        String configurePath = args.length > 0 ? args[0] : "/examples/create_table.conf";
        String configFile = getTestConfigFile(configurePath);
        String jobInfo = "env {\n" +
                "    \"checkpoint.interval\"=10000\n" +
                "    \"execution.parallelism\"=1\n" +
                "    \"job.mode\"=BATCH\n" +
                "}\n" +
                "sink {\n" +
                "    Console {\n" +
                "        parallelism=1\n" +
                "    }\n" +
                "}\n" +
                "source {\n" +
                "    Jdbc {\n" +
                "        \"connection_check_timeout_sec\"=100\n" +
                "        driver=\"com.mysql.cj.jdbc.Driver\"\n" +
                "        password=\"\"\n" +
                "        query=\"select engine_key,id from engine\"\n" +
                "        url=\"\"\n" +
                "        user=\"\"\n" +
                "    }\n" +
                "}";
        String encodeIobInfo = new String(Base64.getEncoder().encode(URLEncoder.encode(jobInfo, "UTF-8").getBytes()));
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        flinkCommandArgs.setConfigFile(configFile);
        //flinkCommandArgs.setConfigStr(encodeIobInfo);
        //AbstractCommandArgs.adaptationConfig(flinkCommandArgs);
        Common.setDeployMode(DeployMode.RUN);
        flinkCommandArgs.setCheckConfig(false);
        flinkCommandArgs.setVariables(null);
//        flinkCommandArgs.setCt(true);
//        flinkCommandArgs.setGss(true);
//        flinkCommandArgs.setCheckConfig(true);
//        flinkCommandArgs.(true);
        flinkCommandArgs.setGss(true);
//        flinkCommandArgs.setGst(true);
        SeaTunnel.run(flinkCommandArgs.buildCommand());

//        Boolean checkResult = new FlinkConfValidateCommand(flinkCommandArgs).checkConnection();
//        System.out.println("checkResult:" + checkResult);

//        List<Map<String, String>> schema = new GetSourceSchemaCommand(flinkCommandArgs).getSchema();
//        System.out.println("schema:" + schema.toString());
//        List<Map<String, String>> sample = new GetSourceSampleCommand(flinkCommandArgs).getSample();  //BROKER、ODBC
//        System.out.println("sample:"+com.alibaba.fastjson.JSON.toJSONString(sample));

//        List<String> tables = new GetSourceTableCommand(flinkCommandArgs).getTables();
//        System.out.println("tables:"+com.alibaba.fastjson.JSON.toJSONString(tables));
////
//        Boolean createTable = new CreateTableCommand(flinkCommandArgs).createTable();
//        System.out.println("createTable:" + createTable);
    }

    public static String getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        URL resource = SeaTunnelActionExample.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}
