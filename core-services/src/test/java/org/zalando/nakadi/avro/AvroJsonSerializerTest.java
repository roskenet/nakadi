package org.zalando.nakadi.avro;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.util.AvroUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;


public class AvroJsonSerializerTest {

    private final Schema schemaWithBytes;
    private final AvroJsonSerializer jsonSerializer;

    public AvroJsonSerializerTest() throws IOException {
        schemaWithBytes = AvroUtils.getParsedSchema(new DefaultResourceLoader()
                .getResource("test.bytes.avro.avsc").getInputStream());
        jsonSerializer = new AvroJsonSerializer(new JsonFactory());
    }

    @Test
    public void testBasicAvroToJson() throws IOException {
        final Schema friendListSchema = schemaWithBytes.getField("friends").schema();
        final Schema magicSchema = schemaWithBytes.getField("magic").schema();
        final Schema suitSchema = schemaWithBytes.getField("suit").schema();
        final Schema friendSchema = friendListSchema.getElementType();
        final GenericRecord inputRecord = new GenericRecordBuilder(schemaWithBytes)
                .set("foo", "bar")
                .set("bar", ByteBuffer.wrap(new byte[] { (byte) 0xC0, (byte) 0xAF, (byte) 0xC0, (byte) 0xAF }))
                .set("magic", new GenericData.Fixed(
                        magicSchema, new byte[] { (byte) 0xC0, (byte) 0xAE, (byte) 0xC0, (byte) 0xAE }))
                .set("int_str", "hello")
                .set("dimensions", Map.of("width", 100L, "height", 200L))
                .set("suit", new GenericData.EnumSymbol(suitSchema, "HEARTS"))
                .set("friends", new GenericData.Array<>(
                        friendListSchema,
                        java.util.List.of(
                                new GenericRecordBuilder(friendSchema)
                                        .set("name", "Joe")
                                        .set("age", 42).build(),
                                new GenericRecordBuilder(friendSchema)
                                        .set("name", "Mike")
                                        .set("age", 102).build())))
                .build();

        // We serialize and deserialize the record to ensure that the structure matches the one of
        // records returned by the parser.
        final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schemaWithBytes);
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        writer.write(inputRecord, encoder);
        final DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schemaWithBytes);
        encoder.flush();
        outputStream.close();
        final GenericRecord decodedRecord = reader.read(null,
                new DecoderFactory().binaryDecoder(outputStream.toByteArray(), null));

        final String result = jsonSerializer.toJson(decodedRecord);
        final JsonNode resultJson = new ObjectMapper().readTree(result);
        final JsonNode expectedJson = new ObjectMapper()
                .readTree("{\"foo\":\"bar\",\"bar\":\"wK/Arw==\",\"magic\":\"wK7Arg==\",\"int_str\":\"hello\","+
                        "\"dimensions\":{\"width\":100,\"height\":200},"+
                        "\"friends\":[{\"name\":\"Joe\",\"age\":42},{\"name\":\"Mike\",\"age\":102}]"+
                        ",\"suit\": \"HEARTS\"}\n");
        Assert.assertEquals(expectedJson,resultJson);
    }

}