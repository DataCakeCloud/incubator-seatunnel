package org.apache.seatunnel.api.common;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.util.List;
import java.util.Map;

public interface SeaTunnelPluginAction {
    /**
     * Use the pluginConfig to check Connection.
     *
     * @throws ActionFailException if plugin prepare failed, the {@link PrepareFailException} will
     *     throw.
     */
    Boolean checkConnection();

    /**
     * Use the pluginConfig to get stream schema.
     *
     * @throws ActionFailException if plugin prepare failed, the {@link PrepareFailException} will
     *     throw.
     */
    List<Map<String, String>> getSchema() throws ActionFailException;

    /**
     * Use the pluginConfig to get stream schema.
     *
     * @throws ActionFailException if plugin prepare failed, the {@link PrepareFailException} will
     *     throw.
     */
    List<Map<String, String>> getSample() throws ActionFailException;

    List<String> getTables() throws ActionFailException;

    Boolean createTable() throws ActionFailException;
}
