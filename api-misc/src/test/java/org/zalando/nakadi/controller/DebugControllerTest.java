package org.zalando.nakadi.controller;

import com.google.common.collect.ImmutableList;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.controller.advice.NakadiProblemExceptionHandler;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.view.TimelineView;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.mockito.Mockito.when;
import static org.zalando.nakadi.config.SecuritySettings.AuthMode.OFF;


public class DebugControllerTest {

    private MockMvc mockMvc;

    public DebugControllerTest() {
        final DebugController controller = new DebugController();
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), TestUtils.JACKSON_2_HTTP_MESSAGE_CONVERTER)
                .setControllerAdvice(new NakadiProblemExceptionHandler())
                .build();
    }

    @Test
    public void debugEvalFilterCanReturnTrue() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/debug/eval-filter")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(new JSONObject()
                                .put("event", new JSONObject().put("full_name", "Joe Black"))
                                .put("filter", "e.full_name LIKE 'Joe%'")
                                .toString()))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().json("{\"result\": true}"));
    }

    @Test
    public void debugEvalFilterCanReturnFalse() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/debug/eval-filter")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(new JSONObject()
                                .put("event", new JSONObject().put("full_name", "Bugs Bunny"))
                                .put("filter", "e.full_name LIKE 'Joe%'")
                                .toString()))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().json("{\"result\": false}"));
    }

}
