package it.agilelab.witboost.provisioning.databricks.model.reverseprovisioningrequest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksComponentSpecific;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class CatalogInfo {

    private Spec spec;

    @Getter
    @Setter
    @ToString
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Spec {

        private String instanceOf;
        private String type;
        private String lifecycle;
        private String owner;
        private String system;
        private String domain;
        private Mesh mesh;

        private List<JsonNode> components;

        @Getter
        @Setter
        @ToString
        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class Mesh {

            private String name;
            private DatabricksComponentSpecific specific;
        }
    }
}
