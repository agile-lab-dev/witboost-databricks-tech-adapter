package it.agilelab.witboost.provisioning.databricks.model.databricks.workload.job;

import com.databricks.sdk.service.jobs.GitProvider;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import it.agilelab.witboost.provisioning.databricks.model.MandatoryFields;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.DatabricksWorkloadSpecific;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DatabricksJobWorkloadSpecific extends DatabricksWorkloadSpecific {

    @NotBlank
    private String jobName;

    private String description;

    @NotBlank
    private String metastore;

    @Valid
    @NotNull
    private JobGitSpecific git;

    private SchedulingSpecific scheduling;

    @Valid
    @NotNull
    private JobClusterSpecific cluster;

    /**
     * Represents Git-specific configurations for a Databricks job.
     * This class extends the `GitSpecific` class and includes additional attributes
     * required for specifying details about the Git-based workspace for the job.
     *
     * Fields:
     * - gitReference: Specifies the reference in the Git repository, such as a branch or tag.
     * - gitReferenceType: Defines the type of the Git reference, specifying whether it is a branch or tag.
     * - gitPath: Indicates the path of the notebook within the Git repository.
     * - gitProvider: Identifies the Git repository provider, such as GitHub or GitLab.
     */
    @Getter
    @Setter
    @NoArgsConstructor
    public static class JobGitSpecific extends GitSpecific {
        @NotBlank
        private String gitReference;

        @NotNull
        private GitReferenceType gitReferenceType;

        @NotBlank
        private String gitPath;

        private GitProvider gitProvider;
    }

    /**
     * Represents the scheduling-specific parameters for a Databricks job workload.
     * This class is used to configure the timing and timezone of a job via a cron expression.
     *
     * Fields:
     * - cronExpression: Specifies the schedule of the job using a cron pattern.
     * - javaTimezoneId: Specifies the timezone in which the cron expression is interpreted.
     */
    @Getter
    @Setter
    @NoArgsConstructor
    public static class SchedulingSpecific {

        @NotNull(groups = {MandatoryFields.class})
        private String cronExpression;

        @NotNull(groups = {MandatoryFields.class})
        private String javaTimezoneId;

        @Override
        public String toString() {
            return "SchedulingSpecific(cronExpression=" + cronExpression + ", javaTimezoneId=" + javaTimezoneId + ')';
        }
    }

    public enum GitReferenceType {
        BRANCH,
        TAG;

        public static GitReferenceType fromString(String referenceType) {
            for (GitReferenceType type : GitReferenceType.values()) {
                if (type.name().equalsIgnoreCase(referenceType)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown reference type: " + referenceType);
        }

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }
}
