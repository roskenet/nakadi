package org.zalando.nakadi.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Component
public class ProspectedFormatValidatorsConfig {

    private final Resource resource;
    private final ObjectMapper objectMapper;
    private volatile Set<String> ignoredEventTypes;

    private static final Logger LOG = LoggerFactory.getLogger(ProspectedFormatValidatorsConfig.class);

    static class EventTypes {

        @NotNull
        private List<String> ignoredEventTypes;

        @JsonCreator
        EventTypes(@JsonProperty("ignored_event_types") final List<String> ignoredEventTypes) {
            this.ignoredEventTypes = ignoredEventTypes;
        }

        public List<String> getIgnoredEventTypes() {
            return ignoredEventTypes;
        }
    }

    @Autowired
    public ProspectedFormatValidatorsConfig(
            @Value("${nakadi.prospected.validators.event.types.list"
                    + ":classpath:prospected-validators-event-types-lists.json}") final Resource configuration,
            final ObjectMapper objectMapper
    ) {
        this.resource = configuration;
        this.ignoredEventTypes = Collections.emptySet();
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void postConstruct() {
        reloadConfigurationIfChanged();
    }

    @Scheduled(fixedDelay = 10, timeUnit = TimeUnit.SECONDS)
    public void checkConfigurationChange() {
        LOG.trace("Checking if configuration is changed");
        reloadConfigurationIfChanged();
    }

    public Set<String> getIgnoredEventTypes() {
        return ignoredEventTypes;
    }

    private void reloadConfigurationIfChanged() {
        try (InputStream in = resource.getInputStream()) {
            final ProspectedFormatValidatorsConfig.EventTypes ets =
                    objectMapper.readValue(in, ProspectedFormatValidatorsConfig.EventTypes.class);
            ignoredEventTypes = Set.copyOf(ets.getIgnoredEventTypes());
        } catch (IOException|RuntimeException ex) {
            LOG.warn("Failed to read prospected-format configuration from resource {}", resource, ex);
        }
    }
}
