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

package org.apache.seatunnel.core.starter.flink.command;

import org.apache.seatunnel.api.common.ActionFailException;
import org.apache.seatunnel.api.common.SeaTunnelPluginAction;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.flink.execution.FlinkExecution;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.core.starter.utils.FileUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigUtil;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import java.nio.file.Path;

import static org.apache.seatunnel.core.starter.utils.FileUtils.checkConfigExist;

/** Use to validate the configuration of the SeaTunnel API. */
@Slf4j
public class FlinkConfValidateCommand implements Command<FlinkCommandArgs> {

    private final FlinkCommandArgs flinkCommandArgs;

    public FlinkConfValidateCommand(FlinkCommandArgs flinkCommandArgs) {
        this.flinkCommandArgs = flinkCommandArgs;
    }

    @Override
    public void execute() throws ConfigCheckException {
        try {
            Boolean checkResult = checkConnection();
            System.out.println("Seatunnel Action result:" + checkResult);
        }catch (Exception e){
            System.out.println("Seatunnel Action false:" + e.getMessage());
        }

    }

    public Boolean checkConnection() {
        Path configFile = FileUtils.getConfigPath(flinkCommandArgs);
        checkConfigExist(configFile);
        Config config = ConfigBuilder.of(configFile);
        if (!flinkCommandArgs.getJobName().equals(Constants.LOGO)) {
            config =
                    config.withValue(
                            ConfigUtil.joinPath("env", "job.name"),
                            ConfigValueFactory.fromAnyRef(flinkCommandArgs.getJobName()));
        }

        FlinkExecution seaTunnelTaskExecution = new FlinkExecution(config);
        SeaTunnelSource internalSource = seaTunnelTaskExecution.sourceExecuteProcessor.getPlugins().get(0);
        if(internalSource instanceof SeaTunnelPluginAction){
            SeaTunnelPluginAction actionSource = (SeaTunnelPluginAction) internalSource;
            return actionSource.checkConnection();
        }
        throw new ActionFailException(internalSource.getPluginName(), PluginType.SOURCE,"Not instanceof SeaTunnelPluginAction");
    }
}
