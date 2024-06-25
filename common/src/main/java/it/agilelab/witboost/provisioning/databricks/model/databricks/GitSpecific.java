package it.agilelab.witboost.provisioning.databricks.model.databricks;

import com.databricks.sdk.service.jobs.GitProvider;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class GitSpecific {

    /**
     * The reference (branch, tag, etc.) in the Git repository.
     * Must not be blank.
     */
    @NotBlank
    private String gitReference;

    /**
     * The type of the Git reference (branch or tag).
     * Must not be null.
     */
    @NotNull
    private GitReferenceType gitReferenceType;

    /**
     * The path within the Git repository.
     * Must be a valid path and not be blank.
     */
    @Valid
    @NotBlank
    private String gitPath;

    /**
     * The URL or identifier of the Git repository.
     * Must not be blank.
     */
    @NotBlank
    private String gitRepoUrl;

    /**
     * The provider of the Git repository (e.g., GitHub, GitLab).
     */
    private GitProvider gitProvider;
}