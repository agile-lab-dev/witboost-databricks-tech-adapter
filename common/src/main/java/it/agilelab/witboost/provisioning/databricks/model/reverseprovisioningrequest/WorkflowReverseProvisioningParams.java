package it.agilelab.witboost.provisioning.databricks.model.reverseprovisioningrequest;

import com.databricks.sdk.service.jobs.Job;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkflowReverseProvisioningParams {
    private String catalogName;
    private String schemaName;
    private String tableName;
    private String reverseProvisioningOption;
    private EnvironmentSpecificConfig<WorkflowReverseProvisioningSpecific> environmentSpecificConfig;

    @Getter
    @Setter
    @NoArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class WorkflowReverseProvisioningSpecific {
        private String workspace;
        private Job workflow;
    }
}
