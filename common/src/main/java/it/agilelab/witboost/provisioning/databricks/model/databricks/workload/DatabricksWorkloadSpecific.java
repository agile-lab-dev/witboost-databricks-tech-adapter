package it.agilelab.witboost.provisioning.databricks.model.databricks.workload;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksComponentSpecific;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class DatabricksWorkloadSpecific extends DatabricksComponentSpecific {

    @Valid
    private GitSpecific git;

    @Getter
    @Setter
    public static class GitSpecific {
        @NotBlank
        private String gitRepoUrl;
    }

    @NotBlank
    private String repoPath;
}
