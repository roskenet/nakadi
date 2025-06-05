package org.zalando.nakadi.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.util.Collections;

class ProspectedFormatValidatorsConfigTest {

    private ProspectedFormatValidatorsConfig prospectedFormatValidatorsConfig;

    @Test
    void getIgnoredEventTypesShouldBeLoadedFromClasspath() {
        final Resource resource = new ClassPathResource("prospected-format-validators-event-types-lists.json");

        prospectedFormatValidatorsConfig = new ProspectedFormatValidatorsConfig(resource, new ObjectMapper());
        prospectedFormatValidatorsConfig.postConstruct();

        Assertions.assertEquals(prospectedFormatValidatorsConfig.getIgnoredEventTypes(), Collections.singleton("test"));
    }

    @Test
    void getIgnoredEventTypesShouldBeEmptyIfFileDoesntExist() {
        final Resource resource = new ClassPathResource("doesnt-exist-resource.json");

        prospectedFormatValidatorsConfig = new ProspectedFormatValidatorsConfig(resource, new ObjectMapper());
        prospectedFormatValidatorsConfig.postConstruct();

        Assertions.assertEquals(prospectedFormatValidatorsConfig.getIgnoredEventTypes(), Collections.emptySet());
    }
}