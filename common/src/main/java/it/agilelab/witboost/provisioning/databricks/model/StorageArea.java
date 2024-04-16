package it.agilelab.witboost.provisioning.databricks.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class StorageArea<T> extends Component<T> {

    private List<String> owners;
    private String infrastructureTemplateId;
    private Optional<String> useCaseTemplateId;
    private List<String> dependsOn;
    private Optional<String> platform;
    private Optional<String> technology;
    private Optional<String> storageType;
    private List<JsonNode> tags;
}
