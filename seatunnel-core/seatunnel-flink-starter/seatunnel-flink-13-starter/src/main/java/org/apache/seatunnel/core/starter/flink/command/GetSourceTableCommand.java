package org.apache.seatunnel.core.starter.flink.command;

import com.alibaba.fastjson.JSON;
import org.apache.seatunnel.api.common.ActionFailException;
import org.apache.seatunnel.api.common.SeaTunnelPluginAction;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.core.starter.command.AbstractCommandArgs;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.flink.execution.FlinkExecution;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.core.starter.utils.FileUtils;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigUtil;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import java.nio.file.Path;
import java.util.List;

import static org.apache.seatunnel.core.starter.utils.FileUtils.checkConfigExist;

public class GetSourceTableCommand implements Command<AbstractCommandArgs> {
    private final FlinkCommandArgs flinkCommandArgs;

    public GetSourceTableCommand(FlinkCommandArgs flinkCommandArgs) {
        this.flinkCommandArgs = flinkCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException, ConfigCheckException {
        List<String> tables = getTables();
        System.out.println("Seatunnel Action result:" + JSON.toJSONString(tables) );
    }

    public List<String> getTables() throws ActionFailException {
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
            return actionSource.getTables();
        }
        throw new ActionFailException(internalSource.getPluginName(), PluginType.SOURCE, "Not instanceof SeaTunnelPluginAction");
    }
}
