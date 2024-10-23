package org.zalando.nakadi.plugin.api;

import org.zalando.nakadi.plugin.api.exceptions.PluginException;

import java.util.Optional;

public interface ApplicationService {

    /**
     * @param applicationId Application Id to validate
     * @return true if application exists false if isn't
     * @throws PluginException if an error occurred while execute
     */
    boolean exists(String applicationId) throws PluginException;

    /**
     * @param applicationId Application Id to validate
     * @return Official SAP team id
     * @throws PluginException if an error occurred while execute
     */
    Optional<String> getOwningTeamId(String applicationId) throws PluginException;
}
