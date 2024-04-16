package it.agilelab.witboost.provisioning.databricks.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
@ToString
public class Descriptor {

    @JsonProperty("dataProduct")
    private DataProduct dataProduct;

    @JsonProperty("componentIdToProvision")
    private String componentIdToProvision;
}
