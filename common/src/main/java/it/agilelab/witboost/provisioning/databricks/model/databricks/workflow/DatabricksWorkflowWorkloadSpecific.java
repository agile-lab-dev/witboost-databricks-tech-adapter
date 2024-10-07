package it.agilelab.witboost.provisioning.databricks.model.databricks.workflow;

import com.databricks.sdk.service.jobs.Job;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.GitSpecific;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class DatabricksWorkflowWorkloadSpecific extends Specific {

    @NotBlank
    private String workspace;

    @NotBlank
    private String repoPath;

    @Valid
    @NotNull
    private GitSpecific git;

    private Job workflow;

    @NotNull
    private boolean override;
}
