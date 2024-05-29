package it.agilelab.witboost.provisioning.databricks.model.databricks;

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
     */
    @NotBlank
    private String gitReference;

    /**
     * The type of the Git reference (branch|tag).
     */
    @NotNull
    private GitReferenceType gitReferenceType;

    /**
     * The path within the Git repository.
     */
    @Valid
    @NotBlank
    private String gitPath;

    /**
     * The URL or identifier of the Git repository.
     */
    @NotBlank
    private String gitRepoUrl;

    //    @Override
    //    public boolean equals(Object o) {
    //        if (this == o) return true;
    //        if (o == null || getClass() != o.getClass()) return false;
    //        GitSpecific that = (GitSpecific) o;
    //        return Objects.equals(gitReference, that.gitReference)
    //                && gitReferenceType == that.gitReferenceType
    //                && Objects.equals(gitPath, that.gitPath)
    //                && Objects.equals(gitRepoUrl, that.gitRepoUrl);
    //    }
}
