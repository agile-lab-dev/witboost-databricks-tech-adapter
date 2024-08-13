package it.agilelab.witboost.provisioning.databricks.model.databricks.job;

import it.agilelab.witboost.provisioning.databricks.model.Specific;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class DatabricksJobWorkloadSpecific extends Specific {

    @NotBlank
    private String workspace;

    @NotBlank
    private String jobName;

    private String description;

    @NotBlank
    private String repoPath;

    @NotBlank
    private String metastore;

    @Valid
    @NotNull
    private GitSpecific git;

    private SchedulingSpecific scheduling;

    @Valid
    @NotNull
    private JobClusterSpecific cluster;
}
