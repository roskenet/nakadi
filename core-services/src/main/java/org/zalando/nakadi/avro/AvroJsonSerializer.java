package org.zalando.nakadi.avro;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;

// With the Avro library, you're supposed to use the JsonEncoder [1] to serialize Avro records to JSON.
// This encoder encodes byte arrays as ASCII strings and not base64 strings.
//
// [1] - https://github.com/apache/avro/blob/c143258a49a03de0c77e9f1a7532c33f11c6bab7/lang/
//             java/avro/src/main/java/org/apache/avro/io/JsonEncoder.java#L52
public class AvroJsonSerializer {

    final JsonFactory jsonFactory;

    public AvroJsonSerializer(final JsonFactory jsonFactory) {
        this.jsonFactory = jsonFactory;
    }

    // Unlike Avro's GenericDatumWriter, this method does not perform any schema validation.
    // The output JSON structure is inferred purely from the input runtime value.
    public String toJson(final GenericRecord record) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final JsonGenerator generator = jsonFactory.createGenerator(out);
        valueToJson(record, generator);
        generator.flush();
        return out.toString();
    }

    private void valueToJson(final Object value, final JsonGenerator generator) throws IOException {
        if (value == null) {
            generator.writeNull();
        } else if (value instanceof CharSequence) {
            generator.writeString(((CharSequence) value).toString());
        } else if (value instanceof GenericEnumSymbol) {
            generator.writeString(value.toString());
        } else if (value instanceof Integer) {
            generator.writeNumber((Integer) value);
        } else if (value instanceof Long) {
            generator.writeNumber((Long) value);
        } else if (value instanceof Float) {
            generator.writeNumber((Float) value);
        } else if (value instanceof Double) {
            generator.writeNumber((Double) value);
        } else if (value instanceof Short) {
            generator.writeNumber((Short) value);
        } else if (value instanceof Byte) {
            generator.writeNumber((Byte) value);
        } else if (value instanceof Boolean) {
            generator.writeBoolean((Boolean) value);
        } else if (value instanceof ByteBuffer) {
            generator.writeString(Base64.getEncoder().encodeToString(((ByteBuffer) value).array()));
        } else if (value instanceof GenericFixed) {
            generator.writeString(Base64.getEncoder().encodeToString(((GenericFixed) value).bytes()));
        } else if (value instanceof GenericArray<?>) {
            final GenericArray<?> array = (GenericArray<?>) value;
            generator.writeStartArray();
            for (final Object item : array) {
                valueToJson(item, generator);
            }
            generator.writeEndArray();
        } else if (value instanceof GenericRecord) {
            final GenericRecord gRecord = (GenericRecord) value;
            generator.writeStartObject();
            for (final Schema.Field field : gRecord.getSchema().getFields()) {
                final Object fieldValue = gRecord.get(field.name());
                generator.writeFieldName(field.name());
                valueToJson(fieldValue, generator);
            }
            generator.writeEndObject();
        } else if (value instanceof Map) {
            final Map<?,?> m = (Map<?,?>) value;
            generator.writeStartObject();
            for (final Map.Entry e : m.entrySet()) {
                generator.writeFieldName(e.getKey().toString());
                valueToJson(e.getValue(), generator);
            }
            generator.writeEndObject();
        } else {
            throw new IllegalArgumentException("Unsupported Avro type: " + value.getClass().getName());
        }
    }

}
