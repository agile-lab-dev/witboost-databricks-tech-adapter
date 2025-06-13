package it.agilelab.witboost.provisioning.databricks.model.databricks.workload;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksComponentSpecific;
import jakarta.annotation.Nullable;
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

    /**
     * Optional field that defines the principal to be used when running a job.
     * When configured, the 'runAs' property of the job or pipeline will be set to this principal.
     */
    @Nullable
    private String runAsPrincipalName;
}
