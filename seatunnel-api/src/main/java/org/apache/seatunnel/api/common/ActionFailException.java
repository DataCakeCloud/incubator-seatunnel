package org.apache.seatunnel.api.common;

import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

/** This exception will throw when {@link SeaTunnelPluginAction#checkConnection/getSchema/getDemo(Config)} failed. */
public class ActionFailException extends SeaTunnelRuntimeException {
    public ActionFailException(String pluginName, PluginType type, String message) {
        super(
                SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                String.format(
                        "PluginName: %s, PluginType: %s, Message: %s",
                        pluginName, type.getType(), message));
    }

    public ActionFailException(
            String pluginName, PluginType type, String message, Throwable cause) {
        super(
                SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                String.format(
                        "PluginName: %s, PluginType: %s, Message: %s",
                        pluginName, type.getType(), message),
                cause);
    }
}
