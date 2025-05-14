package it.agilelab.witboost.provisioning.databricks.model.databricks;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class DatabricksComponentSpecific extends Specific {

    @NotBlank
    private String workspace;
}
