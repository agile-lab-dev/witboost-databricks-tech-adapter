package it.agilelab.witboost.provisioning.databricks.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.control.Option;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataProduct {

    private String id;
    private String name;
    private Optional<String> fullyQualifiedName;
    private String description;
    private String kind;
    private String domain;
    private String version;
    private String environment;
    private String dataProductOwner;
    private String dataProductOwnerDisplayName;
    private Optional<String> email;
    private String devGroup;
    private String ownerGroup;
    private Optional<String> informationSLA;
    private Optional<String> status;
    private Optional<String> maturity;
    private Optional<JsonNode> billing;
    private List<JsonNode> tags;
    private JsonNode specific;
    private List<JsonNode> components;

    public Option<JsonNode> getComponentToProvision(String componentId) {
        return Option.ofOptional(Optional.ofNullable(componentId).flatMap(comp -> components.stream()
                .filter(c -> comp.equals(c.get("id").textValue()))
                .findFirst()));
    }

    public Option<String> getComponentKindToProvision(String componentId) {
        return Option.ofOptional(Optional.ofNullable(componentId).flatMap(comp -> components.stream()
                .filter(c -> comp.equals(c.get("id").textValue()))
                .findFirst()
                .flatMap(c -> Optional.ofNullable(c.get("kind")))
                .map(JsonNode::textValue)));
    }
}
