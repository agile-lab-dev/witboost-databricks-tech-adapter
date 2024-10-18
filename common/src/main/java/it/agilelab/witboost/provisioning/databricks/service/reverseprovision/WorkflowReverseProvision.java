package it.agilelab.witboost.provisioning.databricks.service.reverseprovision;

import static it.agilelab.witboost.provisioning.databricks.service.reverseprovision.ReverseProvisionStatusHandler.handleReverseProvisioningStatusCompleted;
import static it.agilelab.witboost.provisioning.databricks.service.reverseprovision.ReverseProvisionStatusHandler.handleReverseProvisioningStatusFailed;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.jobs.BaseJob;
import com.databricks.sdk.service.jobs.Job;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.client.JobManager;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.reverseprovisioningrequest.WorkflowReverseProvisioningParams;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningStatus;
import it.agilelab.witboost.provisioning.databricks.service.WorkspaceHandler;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class WorkflowReverseProvision {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowReverseProvision.class);
    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);
    private final WorkspaceHandler workspaceHandler;
    private final ObjectMapper mapper;

    public WorkflowReverseProvision(WorkspaceHandler workspaceHandler) {
        this.workspaceHandler = workspaceHandler;
        this.mapper = new ObjectMapper().registerModule(new Jdk8Module());
    }

    public ReverseProvisioningStatus reverseProvision(ReverseProvisioningRequest reverseProvisioningRequest) {

        logger.info(String.format("Started reverse provisioning. Request: %s", reverseProvisioningRequest.toString()));

        Object paramsObj = reverseProvisioningRequest.getParams();
        WorkflowReverseProvisioningParams params =
                mapper.convertValue(paramsObj, WorkflowReverseProvisioningParams.class);

        String workspaceName =
                params.getEnvironmentSpecificConfig().getSpecific().getWorkspace();
        String workflowName = params.getEnvironmentSpecificConfig()
                .getSpecific()
                .getWorkflow()
                .getSettings()
                .getName();

        Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> eitherWorkspaceExists =
                workspaceHandler.getWorkspaceInfo(workspaceName);
        if (eitherWorkspaceExists.isLeft())
            return handleReverseProvisioningStatusFailed(String.format(
                    "Error while retrieving workspace info of %s. Details: %s",
                    workspaceName, buildErrorDetails(eitherWorkspaceExists.getLeft())));

        Optional<DatabricksWorkspaceInfo> databricksWorkspaceInfoOptional = eitherWorkspaceExists.get();
        if (databricksWorkspaceInfoOptional.isEmpty()) {
            return handleReverseProvisioningStatusFailed(
                    String.format("Validation failed. Workspace '%s' not found.", workspaceName));
        }

        DatabricksWorkspaceInfo databricksWorkspaceInfo = databricksWorkspaceInfoOptional.get();

        Either<FailedOperation, WorkspaceClient> eitherWorkspaceClient =
                workspaceHandler.getWorkspaceClient(databricksWorkspaceInfo);
        if (eitherWorkspaceClient.isLeft())
            return handleReverseProvisioningStatusFailed(String.format(
                    "Error while retrieving workspace client of %s. Details: %s",
                    workspaceName, buildErrorDetails(eitherWorkspaceClient.getLeft())));

        WorkspaceClient workspaceClient = eitherWorkspaceClient.get();

        // Validation of Reverse Provisioning request
        Either<ReverseProvisioningStatus, Long> eitherValidRequest =
                validateProvisionRequest(workspaceClient, databricksWorkspaceInfo, workflowName);

        if (eitherValidRequest.isLeft()) return eitherValidRequest.getLeft();

        Long workspaceId = eitherValidRequest.get();

        // Validation ok. Start reverse provisioning
        Job workflow = workspaceClient.jobs().get(workspaceId);

        HashMap<Object, Object> updates = prepareUpdates(workflow);

        return handleReverseProvisioningStatusCompleted(
                String.format(
                        "Reverse provisioning of workflow %s successfully completed.",
                        workflow.getSettings().getName()),
                updates);
    }

    private Either<ReverseProvisioningStatus, Long> validateProvisionRequest(
            WorkspaceClient workspaceClient, DatabricksWorkspaceInfo databricksWorkspaceInfo, String workflowName) {

        JobManager jobManager = new JobManager(workspaceClient, databricksWorkspaceInfo.getName());

        Either<FailedOperation, Iterable<BaseJob>> eitherGetWorkflows = jobManager.listJobsWithGivenName(workflowName);
        if (eitherGetWorkflows.isLeft())
            return Either.left(handleReverseProvisioningStatusFailed(String.format(
                    "Error while checking workflow %s existence in %s. Details %s",
                    workflowName, databricksWorkspaceInfo.getName(), buildErrorDetails(eitherGetWorkflows.getLeft()))));

        Iterable<BaseJob> workflows = eitherGetWorkflows.get();
        List<BaseJob> workflowList = new ArrayList<>();
        workflows.forEach(workflowList::add);

        if (workflowList.isEmpty())
            return Either.left(handleReverseProvisioningStatusFailed(
                    String.format("Workflow %s not found in %s", workflowName, databricksWorkspaceInfo.getName())));

        if (workflowList.size() != 1)
            return Either.left(handleReverseProvisioningStatusFailed(String.format(
                    "Workflow %s is not unique in %s.", workflowName, databricksWorkspaceInfo.getName())));

        return Either.right(workflowList.get(0).getJobId());
    }

    private String buildErrorDetails(FailedOperation failure) {
        return failure.problems().stream().map(Problem::description).collect(Collectors.joining(", "));
    }

    private HashMap<Object, Object> prepareUpdates(Job workflow) {
        HashMap<Object, Object> updates = new HashMap<>();
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
