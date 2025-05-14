package it.agilelab.witboost.provisioning.databricks.model.databricks.workflow;

import com.databricks.sdk.service.jobs.Job;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.DatabricksWorkloadSpecific;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
public class DatabricksWorkflowWorkloadSpecific extends DatabricksWorkloadSpecific {

    private Job workflow;

    private List<WorkflowTasksInfo> workflowTasksInfoList;

    @NotNull
    private boolean override;

    @Getter
    @Setter
    @ToString
    public static class WorkflowTasksInfo {
        private String taskKey;
        private String referencedTaskType; // job, pipeline, notebook
        private String referencedTaskId;
        private String referencedTaskName;
        private String referencedClusterName; // for notebook task
        private String referencedClusterId; // for notebook task
    }
}
