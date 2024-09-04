package it.agilelab.witboost.provisioning.databricks.model.databricks.dlt;

import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class DLTGitSpecific {

    /**
     * The URL or identifier of the Git repository.
     * Must not be blank.
     */
    @NotBlank
    private String gitRepoUrl;
}
