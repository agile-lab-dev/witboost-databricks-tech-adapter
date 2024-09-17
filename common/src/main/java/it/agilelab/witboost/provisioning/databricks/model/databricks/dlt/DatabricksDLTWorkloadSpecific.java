package it.agilelab.witboost.provisioning.databricks.model.databricks.dlt;

import it.agilelab.witboost.provisioning.databricks.model.Specific;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@Valid
public class DatabricksDLTWorkloadSpecific extends Specific {

    @NotBlank
    private String workspace;

    @NotBlank
    private String pipelineName;

    @NotNull
    private ProductEdition productEdition;

    @NotNull
    private Boolean continuous;

    @NotBlank
    private String repoPath;

    @Valid
    @NotNull
    private DLTGitSpecific git;

    // Paths of notebooks to be executed
    @NotNull
    private List<@NotBlank String> notebooks;

    private List<@NotBlank String> files;

    @NotBlank
    private String metastore;

    @NotBlank
    private String catalog;

    private String target;

    @NotNull
    private Boolean photon;

    private List<PipelineNotification> notifications;

    @NotNull
    private PipelineChannel channel;

    @NotNull
    private DLTClusterSpecific cluster;
}
