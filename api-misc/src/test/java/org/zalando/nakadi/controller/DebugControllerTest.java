package org.zalando.nakadi.controller;

import org.json.JSONObject;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.zalando.nakadi.controller.advice.NakadiProblemExceptionHandler;
import org.zalando.nakadi.utils.TestUtils;


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
