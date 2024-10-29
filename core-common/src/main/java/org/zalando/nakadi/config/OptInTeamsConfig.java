package org.zalando.nakadi.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Component
@EnableScheduling
public class OptInTeamsConfig {

    private final Resource resource;
    private final ObjectMapper objectMapper;
    private volatile Set<String> optInTeams;

    private static final Logger LOG = LoggerFactory.getLogger(OptInTeamsConfig.class);

    static class TeamIds {

        @NotNull
        private List<String> teamIds;

        @JsonCreator
        TeamIds(@JsonProperty("team_ids") final List<String> teamIds) {
            this.teamIds = teamIds;
        }

        public List<String> getTeamIds() {
            return teamIds;
        }
    }

    @Autowired
    public OptInTeamsConfig(
            @Value("${nakadi.aspd.opt.in.teams:classpath:aspd-opt-in-teams.json}") final Resource configuration,
            final ObjectMapper objectMapper
    ) {
        this.resource = configuration;
        this.optInTeams = Collections.emptySet();
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void postConstruct() {
        reloadConfigurationIfChanged();
    }

    @Scheduled(fixedDelay = 10_000)
    public void checkConfigurationChange() {
        LOG.trace("Checking if configuration is changed");
        reloadConfigurationIfChanged();
    }

    public Set<String> getOptInTeams() {
        return optInTeams;
    }

    private void reloadConfigurationIfChanged() {
        try (InputStream in = resource.getInputStream()) {
            final TeamIds teamIds = objectMapper.readValue(in, TeamIds.class);
            optInTeams = Set.copyOf(teamIds.getTeamIds());
        } catch (IOException ex) {
            LOG.warn("Failed to read opt-in configuration from resource {}", resource, ex);
        }
    }
}