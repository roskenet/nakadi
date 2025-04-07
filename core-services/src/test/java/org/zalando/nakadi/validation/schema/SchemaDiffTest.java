package org.zalando.nakadi.validation.schema;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.zalando.nakadi.domain.SchemaChange;
import org.zalando.nakadi.validation.schema.diff.SchemaDiff;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zalando.nakadi.utils.TestUtils.readFile;

public class SchemaDiffTest {
    private SchemaDiff service;

    @BeforeEach
    public void setUp() {
        this.service = new SchemaDiff();
    }

    @ParameterizedTest
    @MethodSource("getInvalidChanges")
    public void checkJsonSchemaCompatibility(
            final String description,
            final Schema original,
            final Schema update,
            final List<String> errorMessages) {
        assertThat(description,
                service.collectChanges(original, update).stream()
                        .map(change -> change.getType().toString() + " " + change.getJsonPath())
                        .collect(toList()),
                is(errorMessages));
    }

    @ParameterizedTest
    @MethodSource("getAllowedChanges")
    public void checkJsonSchemaAllowedChanges(
            final String description,
            final Schema original,
            final Schema updated) {
        assertTrue(
                service.collectChanges(original, updated).isEmpty(),
                description);
    }
    static Stream<Arguments> getInvalidChanges() throws IOException {
        return loadTestExamples("schema-evolution-examples.invalid.json", true);
    }

    static Stream<Arguments> getAllowedChanges() throws IOException {
        return loadTestExamples("schema-evolution-examples.allowed.json", false);
    }

    static Stream<Arguments> loadTestExamples(final String filename, final boolean withErrors) throws IOException {
        final JSONArray testCases = new JSONArray(readFile(filename));

        final ArrayList<Arguments> tests = new ArrayList<>();
        for (final Object testCaseObject : testCases) {
            final JSONObject testCase = (JSONObject) testCaseObject;
            final String description = testCase.getString("description");
            final Schema original = SchemaLoader.load(testCase.getJSONObject("original_schema"));
            final Schema update = SchemaLoader.load(testCase.getJSONObject("update_schema"));
            if (withErrors) {
                final List<String> errorMessages = testCase
                        .getJSONArray("errors")
                        .toList()
                        .stream()
                        .map(Object::toString)
                        .collect(toList());
                tests.add(Arguments.of(description, original, update, errorMessages));
            } else {
                tests.add(Arguments.of(description, original, update));
            }
        }
        return tests.stream();
    }

    @Test
    public void testRecursiveCheck() throws IOException {
        final Schema original = SchemaLoader.load(new JSONObject(
                readFile("recursive-schema.json")));
        final Schema newOne = SchemaLoader.load(new JSONObject(
                readFile("recursive-schema.json")));
        assertTrue(service.collectChanges(original, newOne).isEmpty());
    }

    @Test
    public void testSchemaAddsProperties() {
        final Schema first = SchemaLoader.load(new JSONObject("{}"));

        final Schema second = SchemaLoader.load(new JSONObject("{\"properties\": {}}"));
        final List<SchemaChange> changes = service.collectChanges(first, second);
        assertTrue(changes.isEmpty());
    }
}
