package it.agilelab.witboost.provisioning.databricks.service.reverseprovision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.jobs.BaseJob;
import com.databricks.sdk.service.jobs.Job;
import com.databricks.sdk.service.jobs.Task;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.client.JobManager;
import it.agilelab.witboost.provisioning.databricks.client.WorkflowManager;
import it.agilelab.witboost.provisioning.databricks.client.WorkspaceLevelManagerFactory;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workflow.DatabricksWorkflowWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.reverseprovisioningrequest.CatalogInfo;
import it.agilelab.witboost.provisioning.databricks.model.reverseprovisioningrequest.WorkflowReverseProvisioningParams;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.service.WorkspaceHandler;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class WorkflowReverseProvisionHandler {

    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);
    private final WorkspaceHandler workspaceHandler;
    private final ObjectMapper mapper;
    private final WorkspaceLevelManagerFactory workspaceLevelManagerFactory;

    public WorkflowReverseProvisionHandler(
            WorkspaceHandler workspaceHandler, WorkspaceLevelManagerFactory workspaceLevelManagerFactory) {
        this.workspaceHandler = workspaceHandler;
        this.mapper = new ObjectMapper().registerModule(new Jdk8Module());
        this.workspaceLevelManagerFactory = workspaceLevelManagerFactory;
    }

    public Either<FailedOperation, LinkedHashMap<Object, Object>> reverseProvision(
            ReverseProvisioningRequest reverseProvisioningRequest) {

        Object catalogInfoObj = reverseProvisioningRequest.getCatalogInfo();
        ObjectMapper objectMapper = new ObjectMapper();
        CatalogInfo catalogInfo = objectMapper.convertValue(catalogInfoObj, CatalogInfo.class);
        String componentName = catalogInfo.getSpec().getMesh().getName();

        log.info(String.format("(%s) Started reverse provisioning.", componentName));

        Object paramsObj = reverseProvisioningRequest.getParams();
        WorkflowReverseProvisioningParams params =
                mapper.convertValue(paramsObj, WorkflowReverseProvisioningParams.class);

        String workspace = params.getEnvironmentSpecificConfig().getSpecific().getWorkspace();
        String workflowName = params.getEnvironmentSpecificConfig()
                .getSpecific()
                .getWorkflow()
                .getSettings()
                .getName();

        Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> eitherWorkspaceExists =
                workspaceHandler.getWorkspaceInfo(workspace);
        if (eitherWorkspaceExists.isLeft())
            if (eitherWorkspaceExists.isLeft()) return left(eitherWorkspaceExists.getLeft());

        Optional<DatabricksWorkspaceInfo> databricksWorkspaceInfoOptional = eitherWorkspaceExists.get();
        if (databricksWorkspaceInfoOptional.isEmpty()) {
            String errorMessage = String.format("Validation failed. Workspace '%s' not found.", workspace);
            return left(FailedOperation.singleProblemFailedOperation(errorMessage));
        }
        DatabricksWorkspaceInfo databricksWorkspaceInfo = databricksWorkspaceInfoOptional.get();

        Either<FailedOperation, WorkspaceClient> eitherWorkspaceClient =
                workspaceHandler.getWorkspaceClient(databricksWorkspaceInfo);
        if (eitherWorkspaceClient.isLeft()) return left(eitherWorkspaceClient.getLeft());

        WorkspaceClient workspaceClient = eitherWorkspaceClient.get();

        // Validation of Reverse Provisioning request
        var eitherValidRequest = validateProvisionRequest(workspaceClient, databricksWorkspaceInfo, workflowName);

        if (eitherValidRequest.isLeft()) return left(eitherValidRequest.getLeft());

        Long workflowId = eitherValidRequest.get();

        // Validation ok. Start reverse provisioning
        Job workflow = workspaceClient.jobs().get(workflowId);

        // Prepare array of mapping info to add to updates
        Collection<Task> tasks = workflow.getSettings().getTasks();

        ArrayList<DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo> workflowTasksInfoList = new ArrayList<>();

        var workflowManager = new WorkflowManager(workspaceClient, workspace, workspaceLevelManagerFactory);

        for (Task task : Optional.ofNullable(tasks).orElse(Collections.emptyList())) {
            Either<FailedOperation, Optional<DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo>>
                    eitherOptionalTaskInfo = workflowManager.getWorkflowTaskInfoFromId(task);
            if (eitherOptionalTaskInfo.isLeft()) return left(eitherOptionalTaskInfo.getLeft());
            Optional<DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo> optionalTaskInfo =
                    eitherOptionalTaskInfo.get();
            if (optionalTaskInfo.isPresent()) {
                workflowTasksInfoList.add(optionalTaskInfo.get());
            }
        }

        LinkedHashMap<Object, Object> updates = prepareUpdates(workflow, workflowTasksInfoList);
        log.info(String.format("(%s) Reverse Provision updates are ready: %s", componentName, updates));
        return right(updates);
    }

    private Either<FailedOperation, Long> validateProvisionRequest(
            WorkspaceClient workspaceClient, DatabricksWorkspaceInfo databricksWorkspaceInfo, String workflowName) {

        JobManager jobManager = new JobManager(workspaceClient, databricksWorkspaceInfo.getName());

        Either<FailedOperation, Iterable<BaseJob>> eitherGetWorkflows = jobManager.listJobsWithGivenName(workflowName);
        if (eitherGetWorkflows.isLeft()) return left(eitherGetWorkflows.getLeft());

        Iterable<BaseJob> workflows = eitherGetWorkflows.get();
        List<BaseJob> workflowList = new ArrayList<>();
        workflows.forEach(workflowList::add);

        if (workflowList.isEmpty())
            return left(FailedOperation.singleProblemFailedOperation(
                    String.format("Workflow %s not found in %s", workflowName, databricksWorkspaceInfo.getName())));

        if (workflowList.size() != 1) {
            return left(FailedOperation.singleProblemFailedOperation(String.format(
                    "Workflow %s is not unique in %s.", workflowName, databricksWorkspaceInfo.getName())));
        }

        return Either.right(workflowList.get(0).getJobId());
    }

    private LinkedHashMap<Object, Object> prepareUpdates(
            Job workflow, List<DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo> workflowTasksInfoList) {
        LinkedHashMap<Object, Object> updates = new LinkedHashMap<>();
        updates.put("spec.mesh.specific.workflowTasksInfoList", workflowTasksInfoList);
        updates.put("spec.mesh.specific.workflow.job_id", workflow.getJobId());
        updates.put("spec.mesh.specific.workflow.created_time", workflow.getCreatedTime());
        updates.put("spec.mesh.specific.workflow.creator_user_name", workflow.getCreatorUserName());
        updates.put("spec.mesh.specific.workflow.run_as_user_name", workflow.getRunAsUserName());
        updates.put("spec.mesh.specific.workflow.settings", workflow.getSettings());
        updates.put("spec.mesh.specific.workflow.settings.name", "IGNORED");
        updates.put(
                "witboost.parameters.modifiedByRef", "databricks-workload-workflow-reverse-provisioning-template.1");
        updates.put("witboost.parameters.updatedDate", formatter.format(Instant.now()));
        return updates;
    }
}
