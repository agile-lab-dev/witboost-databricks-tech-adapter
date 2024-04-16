package it.agilelab.witboost.provisioning.databricks.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ResourceUtils {

    public static String getContentFromResource(String resourcePath) throws IOException {
        try (var in = ResourceUtils.class.getResourceAsStream(resourcePath)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
