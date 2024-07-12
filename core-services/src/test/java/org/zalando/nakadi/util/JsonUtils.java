package org.zalando.nakadi.util;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

import java.io.IOException;

public class JsonUtils {

    public static Schema loadJsonSchema(final String jsonSchemaFileName) throws IOException {
        final JSONObject metaSchemaJson = new JSONObject(Resources.toString(Resources.getResource(jsonSchemaFileName),
                Charsets.UTF_8));
        return SchemaLoader.load(metaSchemaJson);
    }
}
