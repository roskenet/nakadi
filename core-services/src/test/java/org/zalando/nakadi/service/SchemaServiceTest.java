package org.zalando.nakadi.service;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.EventTypeSchemaBase;
import org.zalando.nakadi.domain.PaginationWrapper;
import org.zalando.nakadi.domain.Version;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSchemaException;
import org.zalando.nakadi.exceptions.runtime.SchemaEvolutionException;
import org.zalando.nakadi.exceptions.runtime.SchemaValidationException;
import org.zalando.nakadi.kpi.event.NakadiBatchPublished;
import org.zalando.nakadi.repository.db.EventTypeRepository;
import org.zalando.nakadi.repository.db.SchemaRepository;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.util.JsonUtils;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.validation.JsonSchemaEnrichment;
import org.zalando.nakadi.validation.schema.SchemaEvolutionConstraint;
import org.zalando.nakadi.validation.schema.diff.SchemaDiff;
import org.zalando.nakadi.view.EventOwnerSelector;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.zalando.nakadi.domain.EventCategory.BUSINESS;

public class SchemaServiceTest {

    private SchemaRepository schemaRepository;
    private PaginationService paginationService;
    private SchemaService schemaService;
    private SchemaEvolutionService schemaEvolutionService;
    private EventTypeRepository eventTypeRepository;
    private EventTypeCache eventTypeCache;
    private EventType eventType;
    private TimelineSync timelineSync;
    private NakadiSettings nakadiSettings;

    @BeforeEach
    public void setUp() throws IOException {
        schemaRepository = Mockito.mock(SchemaRepository.class);
        paginationService = Mockito.mock(PaginationService.class);
        schemaEvolutionService = createSchemaEvolutionServiceSpy();
        eventTypeRepository = Mockito.mock(EventTypeRepository.class);
        eventTypeCache = Mockito.mock(EventTypeCache.class);
        eventType = TestUtils.buildDefaultEventType();
        Mockito.when(eventTypeRepository.findByName(any())).thenReturn(eventType);
        timelineSync = Mockito.mock(TimelineSync.class);
        nakadiSettings = Mockito.mock(NakadiSettings.class);

        schemaService = new SchemaService(schemaRepository, paginationService,
                new JsonSchemaEnrichment(new DefaultResourceLoader(), "classpath:schema_metadata.json"),
                schemaEvolutionService, eventTypeRepository, eventTypeCache, timelineSync, nakadiSettings);
    }

    private SchemaEvolutionService createSchemaEvolutionServiceSpy() throws IOException {
        final List<SchemaEvolutionConstraint> evolutionConstraints =
                Lists.newArrayList(Mockito.mock(SchemaEvolutionConstraint.class));
        return Mockito.spy(new SchemaEvolutionService(JsonUtils.loadJsonSchema("schema_compatible.json"),
                JsonUtils.loadJsonSchema("schema_non_compatible.json"),
                evolutionConstraints,
                Mockito.mock(SchemaDiff.class), Mockito.mock(BiFunction.class),
                new HashMap<>(), Mockito.mock(AvroSchemaCompatibility.class)));
    }

    @Test
    public void testOffsetBounds() {
        assertThrows(InvalidLimitException.class, () -> schemaService.getSchemas("name", -1, 1));
    }

    @Test
    public void testLimitLowerBounds() {
        assertThrows(InvalidLimitException.class, () -> schemaService.getSchemas("name", 0, 0));
    }

    @Test
    public void testLimitUpperBounds() {
        assertThrows(InvalidLimitException.class, () -> schemaService.getSchemas("name", 0, 1001));
    }

    @Test
    public void testSuccess() {
        final PaginationWrapper result = schemaService.getSchemas("name", 0, 1000);
        Assertions.assertTrue(true);
    }

    @Test
    public void testIllegalVersionNumber() throws Exception {
        Mockito.when(schemaRepository.getSchemaVersion(eventType.getName() + "wrong",
                eventType.getSchema().getVersion().toString()))
                .thenThrow(NoSuchSchemaException.class);
        assertThrows(NoSuchSchemaException.class, () -> schemaService.getSchemaVersion(eventType.getName() + "wrong",
                eventType.getSchema().getVersion().toString()));
    }

    @Test
    public void testNonExistingVersionNumber() throws Exception {
        final var newVersion =
                new Version(eventType.getSchema().getVersion()).bump(Version.Level.MINOR).toString();
        Mockito.when(schemaRepository.getSchemaVersion(eventType.getName(), newVersion))
                .thenThrow(NoSuchSchemaException.class);
        assertThrows(NoSuchSchemaException.class,
                () -> schemaService.getSchemaVersion(eventType.getName(), newVersion));
    }

    @Test
    public void testGetSchemaSuccess() throws Exception {
        Mockito.when(schemaRepository.getSchemaVersion(eventType.getName(),
                eventType.getSchema().getVersion().toString()))
                .thenReturn(eventType.getSchema());
        final EventTypeSchema result =
                schemaService.getSchemaVersion(eventType.getName(), eventType.getSchema().getVersion().toString());
        Assertions.assertTrue(true);
    }
    @Test
    public void invalidEventTypeSchemaJsonSchemaThenThrows() throws Exception {
        final String jsonSchemaString = Resources.toString(
                Resources.getResource("sample-invalid-json-schema.json"),
                Charsets.UTF_8);
        eventType.getSchema().setSchema(jsonSchemaString);

        assertThrows(SchemaValidationException.class, () -> schemaService.validateSchema(eventType, false));
    }

    @Test
    public void whenCompatibleModeAndInvalidJsonSchemaAlwaysThrows() throws Exception {
        final String jsonSchemaString = Resources.toString(
                Resources.getResource("invalid-json-schema-structure.json"),
                Charsets.UTF_8);
        eventType.getSchema().setSchema(jsonSchemaString);
        eventType.setCompatibilityMode(CompatibilityMode.COMPATIBLE);
        assertThrows(SchemaValidationException.class,
                () -> schemaService.validateSchema(eventType, false));
        assertThrows(SchemaValidationException.class,
                () -> schemaService.validateSchema(eventType, true));
    }

    @EnumSource(value = CompatibilityMode.class, names = {"NONE", "FORWARD"})
    @ParameterizedTest
    public void whenNewEventTypeWithInvalidJsonSchemaThenThrows(final CompatibilityMode mode)
            throws Exception {
        final String jsonSchemaString = Resources.toString(
                Resources.getResource("invalid-json-schema-structure.json"),
                Charsets.UTF_8);
        eventType.getSchema().setSchema(jsonSchemaString);
        eventType.setCompatibilityMode(mode);
        assertThrows(SchemaValidationException.class,
                () -> schemaService.validateSchema(eventType, false));
    }

    @EnumSource(value = CompatibilityMode.class, names = {"NONE", "FORWARD"})
    @ParameterizedTest
    public void whenExistingEventTypeWithInvalidJsonSchemaThenDontThrow(final CompatibilityMode mode)
            throws Exception {
        final String jsonSchemaString = Resources.toString(
                Resources.getResource("invalid-json-schema-structure.json"),
                Charsets.UTF_8);
        eventType.getSchema().setSchema(jsonSchemaString);
        eventType.setCompatibilityMode(mode);
        assertDoesNotThrow(() -> schemaService.validateSchema(eventType, true));
    }

    @EnumSource(value = CompatibilityMode.class, names = {"NONE", "FORWARD"})
    @ParameterizedTest
    public void whenNewNonCompatibleEventTypeAndExtensibleEnumThenDontThrow(final CompatibilityMode mode)
            throws Exception {
        final String jsonSchemaString = Resources.toString(
                Resources.getResource("sample-extensible-enum.json"),
                Charsets.UTF_8);
        eventType.getSchema().setSchema(jsonSchemaString);
        eventType.setCompatibilityMode(mode);
        assertDoesNotThrow(() -> schemaService.validateSchema(eventType, false));
    }

    @EnumSource(value = CompatibilityMode.class, names = {"NONE", "FORWARD"})
    @ParameterizedTest
    public void whenNewNonCompatibleEventTypeAndInvalidExtensibleEnumThenThrows(final CompatibilityMode mode)
            throws Exception {
        final String jsonSchemaString = Resources.toString(
                Resources.getResource("sample-invalid-extensible-enum.json"),
                Charsets.UTF_8);
        eventType.getSchema().setSchema(jsonSchemaString);
        eventType.setCompatibilityMode(mode);
        assertThrows(SchemaValidationException.class, () -> schemaService.validateSchema(eventType, false));
    }

    @Test
    public void whenCompatibleEventTypeWithExtensibleEnumThenThrows()
            throws Exception {
        final String jsonSchemaString = Resources.toString(
                Resources.getResource("sample-extensible-enum.json"),
                Charsets.UTF_8);
        eventType.getSchema().setSchema(jsonSchemaString);
        eventType.setCompatibilityMode(CompatibilityMode.COMPATIBLE);
        assertThrows(SchemaValidationException.class, () -> schemaService.validateSchema(eventType, false));
        assertThrows(SchemaValidationException.class, () -> schemaService.validateSchema(eventType, true));
    }

    @Test
    public void whenPOSTBusinessEventTypeMetadataThenThrows() throws Exception {
        eventType.getSchema().setSchema(
                "{\"type\": \"object\", \"properties\": {\"metadata\": {\"type\": \"object\"} }}");
        eventType.setCategory(BUSINESS);

        assertThrows(SchemaValidationException.class, () -> schemaService.validateSchema(eventType, false));
    }

    @Test
    public void whenEventTypeSchemaJsonIsMalformedThenThrows() throws Exception {
        eventType.getSchema().setSchema("invalid-json");

        assertThrows(SchemaValidationException.class, () -> schemaService.validateSchema(eventType, false));
    }

    @Test
    public void whenSchemaHasIncompatibilitiesThenThrows() throws Exception {
        Mockito.doThrow(SchemaEvolutionException.class)
                .when(schemaEvolutionService).collectMetaSchemaIncompatibilities(any(), any());

        assertThrows(SchemaEvolutionException.class,
                () -> schemaService.validateSchema(eventType, false));
    }

    @Test
    public void whenPostWithUnsupportedMetaSchemaThenThrows() throws Exception {
        eventType.getSchema().setSchema(
                "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\", \"type\":\"object\"}");
        eventType.setCategory(BUSINESS);

        assertThrows(SchemaValidationException.class, () -> schemaService.validateSchema(eventType, false));
    }

    @Test
    public void whenPostWithRootElementOfTypeArrayThenThrows() throws Exception {
        eventType.getSchema().setSchema(
                "{\\\"type\\\":\\\"array\\\" }");
        eventType.setCategory(BUSINESS);

        assertThrows(SchemaValidationException.class, () -> schemaService.validateSchema(eventType, false));
    }

    @Test
    public void throwsInvalidSchemaOnInvalidRegex() throws Exception {
        eventType.getSchema().setSchema("{\n" +
                "      \"properties\": {\n" +
                "        \"foo\": {\n" +
                "          \"type\": \"string\",\n" +
                "          \"pattern\": \"^(?!\\\\s*$).+\"\n" +
                "        }\n" +
                "      }\n" +
                "    }");

        assertThrows(SchemaValidationException.class, () -> schemaService.validateSchema(eventType, false));
    }

    @Test
    public void doNotSupportSchemaWithExternalRef() {
        eventType.getSchema().setSchema("{\n" +
                "    \"properties\": {\n" +
                "      \"foo\": {\n" +
                "        \"$ref\": \"/invalid/url\"\n" +
                "      }\n" +
                "    }\n" +
                "  }");

        assertThrows(SchemaValidationException.class, () -> schemaService.validateSchema(eventType, false));
    }

    @Test
    public void testValidateSchemaEndingBracket() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaService.parseJsonSchema("{\"additionalProperties\": true}}"));
    }

    @Test
    public void testValidateSchemaMultipleRoots() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaService.
                        parseJsonSchema("{\"additionalProperties\": true}{\"additionalProperties\": true}"));
    }

    @Test
    public void testValidateSchemaArbitraryEnding() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaService.
                        parseJsonSchema("{\"additionalProperties\": true}NakadiRocks"));
    }

    @Test
    public void testValidateSchemaArrayEnding() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaService.
                        parseJsonSchema("[{\"additionalProperties\": true}]]"));
    }

    @Test
    public void testValidateSchemaEndingCommaArray() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaService.parseJsonSchema("[{\"test\": true},]"));
    }

    @Test
    public void testValidateSchemaEndingCommaArray2() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaService.parseJsonSchema("[\"test\",]"));
    }

    @Test
    public void testValidateSchemaEndingCommaObject() {
        assertThrows(SchemaValidationException.class,
                () -> SchemaService.parseJsonSchema("{\"test\": true,}"));
    }

    @Test
    public void testValidateSchemaFormattedJson() {
        SchemaService.parseJsonSchema("{\"properties\":{\"event_class\":{\"type\":\"string\"}," +
                "\"app_domain_id\":{\"type\":\"integer\"},\"event_type\":{\"type\":\"string\"},\"time\"" +
                ":{\"type\":\"number\"},\"partitioning_key\":{\"type\":\"string\"},\"body\":{\"type\"" +
                ":\"object\"}},\"additionalProperties\":true}");
    }

    @Test
    public void testSchemaVersionFoundInRepository() {
        Mockito.when(schemaRepository.getAllSchemas("nakadi.batch.published"))
                .thenReturn(Collections.singletonList(
                        new EventTypeSchema(new EventTypeSchemaBase(
                                EventTypeSchemaBase.Type.AVRO_SCHEMA,
                                NakadiBatchPublished.getClassSchema().toString()), "1.0.0", new DateTime())));

        final String avroSchemaVersion = schemaService.getAvroSchemaVersion(
                "nakadi.batch.published", NakadiBatchPublished.getClassSchema());

        Assertions.assertEquals("1.0.0", avroSchemaVersion);

        Mockito.reset(schemaRepository);
    }

    @Test
    public void testSchemaVersionFoundInRepositoryTwoSchemas() {
        Mockito.when(schemaRepository.getAllSchemas("nakadi.batch.published"))
                .thenReturn(Arrays.asList(
                        new EventTypeSchema(new EventTypeSchemaBase(
                                EventTypeSchemaBase.Type.JSON_SCHEMA,
                                "{}"), "1.0.0", new DateTime()),
                        new EventTypeSchema(new EventTypeSchemaBase(
                                EventTypeSchemaBase.Type.AVRO_SCHEMA,
                                NakadiBatchPublished.getClassSchema().toString()), "2.0.0", new DateTime()))
                );

        final String avroSchemaVersion = schemaService.getAvroSchemaVersion(
                "nakadi.batch.published", NakadiBatchPublished.getClassSchema());

        Assertions.assertEquals("2.0.0", avroSchemaVersion);

        Mockito.reset(schemaRepository);
    }

    @Test
    public void testSchemaVersionNotFoundForEventType() {
        Mockito.when(schemaRepository.getAllSchemas("nakadi.batch.published"))
                .thenReturn(Collections.emptyList());

        assertThrows(NoSuchSchemaException.class, () -> schemaService.
                getAvroSchemaVersion("nakadi.batch.published", NakadiBatchPublished.getClassSchema()));
        Mockito.reset(schemaRepository);
    }

    @Test
    public void testNoExceptionThrownWhenSchemaHasArrayItems() throws Exception {
        final String jsonSchemaString = Resources.toString(
                Resources.getResource("compatible-additional-item-schema.json"),
                Charsets.UTF_8);

        eventType.getSchema().setSchema(jsonSchemaString);
        eventType.setCategory(BUSINESS);
        eventType.setCompatibilityMode(CompatibilityMode.COMPATIBLE);
        assertDoesNotThrow(() -> schemaService.validateSchema(eventType, false));
    }

    @Test
    public void whenEventOwnerSelectorFieldMissingInSchemaThenThrows() {
        eventType.getSchema().setSchema("{}");
        eventType.setEventOwnerSelector(
                new EventOwnerSelector(EventOwnerSelector.Type.PATH, "selector_name", "retailer_id"));

        assertThrows(SchemaValidationException.class, () -> schemaService.validateSchema(eventType, false));
    }

    @Test
    public void whenEventOwnerSelectorFieldPresentInSchemaThenOK() throws IOException {
        final String jsonSchemaString = Resources.toString(
                Resources.getResource("schema-with-retailer-id.json"),
                Charsets.UTF_8);

        eventType.getSchema().setSchema(jsonSchemaString);
        eventType.setEventOwnerSelector(
                new EventOwnerSelector(EventOwnerSelector.Type.PATH, "selector_name", "info.retailer_id"));

        assertDoesNotThrow(() -> schemaService.validateSchema(eventType, false));
    }
}
