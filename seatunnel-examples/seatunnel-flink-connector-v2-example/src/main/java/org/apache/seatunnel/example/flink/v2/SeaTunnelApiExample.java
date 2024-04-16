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

import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.exception.CommandException;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SeaTunnelApiExample {

    public static void main(String[] args)
            throws FileNotFoundException, URISyntaxException, CommandException {
//        parseFile();
        String configurePath = args.length > 0 ? args[0] : "/examples/kafka2console.conf";
        String configFile = getTestConfigFile(configurePath);
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        flinkCommandArgs.setConfigFile(configFile);
        flinkCommandArgs.setCheckConfig(false);
        flinkCommandArgs.setVariables(null);
        SeaTunnel.run(flinkCommandArgs.buildCommand());
    }

//    public static void parseFile(){
//        File filePath = new File("/Users/shareit/seatunnel/incubator-seatunnel/seatunnel-examples/seatunnel-flink-connector-v2-example/target/classes/examples/mysql_to_console.conf");
//        Config config = ConfigFactory.parseFile(filePath);
//        System.out.println(config);
//    }

    public static String getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        URL resource = SeaTunnelApiExample.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}
