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
public class OutputPort<T> extends Component<T> {

    private String version;
    private String infrastructureTemplateId;
    private Optional<String> useCaseTemplateId;
    private List<String> dependsOn;
    private Optional<String> platform;
    private Optional<String> technology;
    private String outputPortType;
    private Optional<String> creationDate;
    private Optional<String> startDate;
    private Optional<String> retentionTime;
    private Optional<String> processDescription;
    private JsonNode dataContract;
    private JsonNode dataSharingAgreement;
    private List<JsonNode> tags;
    private Optional<JsonNode> sampleData;
    private Optional<JsonNode> semanticLinking;
}
