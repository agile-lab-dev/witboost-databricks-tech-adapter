package it.agilelab.witboost.provisioning.databricks.service.provision;

import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.DatabricksDLTWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.DatabricksJobWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.openapi.model.*;
import it.agilelab.witboost.provisioning.databricks.service.validation.ValidationService;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ProvisionServiceImpl implements ProvisionService {

    private final ConcurrentHashMap<String, ProvisioningStatus> statusMap = new ConcurrentHashMap<>();
    private final ForkJoinPool forkJoinPool;
    private final ValidationService validationService;
    private final JobWorkloadHandler jobWorkloadHandler;
    private final DLTWorkloadHandler dltWorkloadHandler;
    private final WorkspaceHandler workspaceHandler;
    private final String WORKLOAD_KIND = "workload";
    private final Logger logger = LoggerFactory.getLogger(ProvisionServiceImpl.class);

    public ProvisionServiceImpl(
            ValidationService validationService,
            JobWorkloadHandler jobWorkloadHandler,
            DLTWorkloadHandler dltWorkloadHandler,
            WorkspaceHandler workspaceHandler,
            ForkJoinPool forkJoinPool) {
        this.validationService = validationService;
        this.jobWorkloadHandler = jobWorkloadHandler;
        this.workspaceHandler = workspaceHandler;
        this.forkJoinPool = forkJoinPool;
        this.dltWorkloadHandler = dltWorkloadHandler;
    }

    @Override
    public ValidationResult validate(ProvisioningRequest provisioningRequest) {
        return validationService
                .validate(provisioningRequest)
                .fold(
                        l -> new ValidationResult(false)
                                .error(new ValidationError(l.problems().stream()
                                        .map(Problem::description)
                                        .collect(Collectors.toList()))),
                        r -> new ValidationResult(true));
    }

    @Override
    public String provision(ProvisioningRequest provisioningRequest) {
        return startProvisioning(provisioningRequest, true);
    }

    @Override
    public ProvisioningStatus getStatus(String token) {
        return statusMap.getOrDefault(
                token, new ProvisioningStatus(ProvisioningStatus.StatusEnum.FAILED, "Token not found"));
    }

    @Override
    public String unprovision(ProvisioningRequest provisioningRequest) {
        return startProvisioning(provisioningRequest, false);
    }

    private String startProvisioning(ProvisioningRequest provisioningRequest, boolean isProvisioning) {
        String token = generateToken();
        ProvisioningStatus response = new ProvisioningStatus(
                ProvisioningStatus.StatusEnum.RUNNING,
                (isProvisioning ? "Provisioning" : "Unprovisioning") + " in progress");
        statusMap.put(token, response);

        forkJoinPool.submit(() -> {
            var eitherValidation = validationService.validate(provisioningRequest);
            if (eitherValidation.isLeft()) {
                handleValidationFailure(token, eitherValidation.getLeft());
                return;
            }

            var provisionRequest = eitherValidation.get();

            if (provisionRequest.component().getKind().equalsIgnoreCase(WORKLOAD_KIND)) {
                handleWorkload(token, provisionRequest, isProvisioning);
            } else {
                updateStatus(
                        token,
                        ProvisioningStatus.StatusEnum.FAILED,
                        String.format(
                                "The kind '%s' of the component is not supported by this Specific Provisioner",
                                provisionRequest.component().getKind()));
            }
        });

        return token;
    }

    private void handleValidationFailure(String token, FailedOperation validationFailure) {
        StringBuilder errors = new StringBuilder("Errors: ");
        validationFailure.problems().forEach(problem -> errors.append("-")
                .append(problem.description())
                .append("\n"));
        updateStatus(token, ProvisioningStatus.StatusEnum.FAILED, errors.toString());
    }

    private void handleWorkload(String token, ProvisionRequest provisionRequest, boolean isProvisioning) {
        if (provisionRequest.component().getSpecific().getClass().equals(DatabricksJobWorkloadSpecific.class)) {
            if (isProvisioning) {
                provisionJob(provisionRequest, token);
            } else {
                unprovisionJob(provisionRequest, token);
            }
        } else if (provisionRequest.component().getSpecific().getClass().equals(DatabricksDLTWorkloadSpecific.class)) {
            if (isProvisioning) {
                provisionDLT(provisionRequest, token);
            } else {
                unprovisionDLT(provisionRequest, token);
            }
        } else {
            updateStatus(
                    token,
                    ProvisioningStatus.StatusEnum.FAILED,
                    String.format(
                            "The specific section of the component %s is not of type DatabricksJobWorkloadSpecific or DatabricksDLTWorkloadSpecific",
                            provisionRequest.component().getName()));
        }
    }

    private String generateToken() {
        return java.util.UUID.randomUUID().toString();
    }

    private void updateStatus(String token, ProvisioningStatus.StatusEnum status, String result) {
        updateStatus(token, status, result, null);
    }

    private void updateStatus(String token, ProvisioningStatus.StatusEnum status, String result, Info info) {
        ProvisioningStatus response = new ProvisioningStatus(status, result);
        response.setInfo(info);
        statusMap.put(token, response);
    }

    private void provisionJob(ProvisionRequest provisionRequest, String token) {
        Either<FailedOperation, DatabricksWorkspaceInfo> eitherCreatedWorkspace =
                workspaceHandler.provisionWorkspace(provisionRequest);
        if (eitherCreatedWorkspace.isLeft()) {
            handleFailure(token, eitherCreatedWorkspace.getLeft());
            return;
        }

        var databricksWorkspaceInfo = eitherCreatedWorkspace.get();
        if (!databricksWorkspaceInfo.getProvisioningState().equals(ProvisioningState.SUCCEEDED)) {
            var specific =
                    (DatabricksJobWorkloadSpecific) provisionRequest.component().getSpecific();
            String errorMessage = String.format(
                    "Provision of %s skipped. The status of %s workspace is different from 'ACTIVE'. Please try again and if the error persists contact the platform team. ",
                    provisionRequest.component().getName(), specific.getWorkspace());
            logger.info(errorMessage);
            updateStatus(token, ProvisioningStatus.StatusEnum.FAILED, errorMessage);
            return;
        }

        var eitherWorkspaceClient = workspaceHandler.getWorkspaceClient(databricksWorkspaceInfo);
        if (eitherWorkspaceClient.isLeft()) {
            handleFailure(token, eitherWorkspaceClient.getLeft());
            return;
        }

        Either<FailedOperation, String> eitherNewJob = jobWorkloadHandler.provisionWorkload(
                provisionRequest, eitherWorkspaceClient.get(), databricksWorkspaceInfo);
        if (eitherNewJob.isLeft()) {
            handleFailure(token, eitherNewJob.getLeft());
            return;
        }

        String jobUrl = "https://" + databricksWorkspaceInfo.getDatabricksHost() + "/jobs/" + eitherNewJob.get();
        Map<String, String> provisionResult = new HashMap<>();
        provisionResult.put("workspace path", databricksWorkspaceInfo.getAzureResourceUrl());
        provisionResult.put("job path", jobUrl);

        logger.info(String.format(
                "Provisioning of %s completed", provisionRequest.component().getName()));
        updateStatus(
                token,
                ProvisioningStatus.StatusEnum.COMPLETED,
                "",
                new Info(JsonNodeFactory.instance.objectNode(), provisionResult).privateInfo(provisionResult));
    }

    private void provisionDLT(ProvisionRequest provisionRequest, String token) {
        Either<FailedOperation, DatabricksWorkspaceInfo> eitherCreatedWorkspace =
                workspaceHandler.provisionWorkspace(provisionRequest);
        if (eitherCreatedWorkspace.isLeft()) {
            handleFailure(token, eitherCreatedWorkspace.getLeft());
            return;
        }

        var databricksWorkspaceInfo = eitherCreatedWorkspace.get();
        if (!databricksWorkspaceInfo.getProvisioningState().equals(ProvisioningState.SUCCEEDED)) {
            var specific =
                    (DatabricksDLTWorkloadSpecific) provisionRequest.component().getSpecific();
            String errorMessage = String.format(
                    "Provision of %s skipped. The status of %s workspace is different from 'ACTIVE'. Please try again and if the error persists contact the platform team. ",
                    provisionRequest.component().getName(), specific.getWorkspace());
            logger.info(errorMessage);
            updateStatus(token, ProvisioningStatus.StatusEnum.FAILED, errorMessage);
            return;
        }

        var eitherWorkspaceClient = workspaceHandler.getWorkspaceClient(databricksWorkspaceInfo);
        if (eitherWorkspaceClient.isLeft()) {
            handleFailure(token, eitherWorkspaceClient.getLeft());
            return;
        }

        Either<FailedOperation, String> eitherNewPipeline = dltWorkloadHandler.provisionWorkload(
                provisionRequest, eitherWorkspaceClient.get(), databricksWorkspaceInfo);
        if (eitherNewPipeline.isLeft()) {
            handleFailure(token, eitherNewPipeline.getLeft());
            return;
        }

        String pipelineUrl =
                "https://" + databricksWorkspaceInfo.getDatabricksHost() + "/pipelines/" + eitherNewPipeline.get();
        Map<String, String> provisionResult = new HashMap<>();
        provisionResult.put("Workspace path", databricksWorkspaceInfo.getAzureResourceUrl());
        provisionResult.put("Pipeline path", pipelineUrl);

        logger.info(String.format(
                "Provisioning of %s completed", provisionRequest.component().getName()));
        updateStatus(
                token,
                ProvisioningStatus.StatusEnum.COMPLETED,
                "",
                new Info(JsonNodeFactory.instance.objectNode(), provisionResult).privateInfo(provisionResult));
    }

    private void unprovisionJob(ProvisionRequest provisionRequest, String token) {
        Either<FailedOperation, String> workspaceName = workspaceHandler.getWorkspaceName(provisionRequest);
        if (workspaceName.isLeft()) {
            handleFailure(token, workspaceName.getLeft());
            return;
        }

        Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> eitherDatabricksWorkspaceInfo =
                workspaceHandler.getWorkspaceInfo(provisionRequest);
        if (eitherDatabricksWorkspaceInfo.isLeft()) {
            handleFailure(token, eitherDatabricksWorkspaceInfo.getLeft());
            return;
        }

        var workspaceInfoOpt = eitherDatabricksWorkspaceInfo.get();
        if (workspaceInfoOpt.isEmpty()) {
            var specific =
                    (DatabricksJobWorkloadSpecific) provisionRequest.component().getSpecific();
            logger.info(String.format("Unprovision skipped. Workspace %s not found.", specific.getWorkspace()));
            updateStatus(
                    token,
                    ProvisioningStatus.StatusEnum.COMPLETED,
                    String.format("Unprovision skipped. Workspace %s not found.", specific.getWorkspace()));
            return;
        }
        if (!workspaceInfoOpt.get().getProvisioningState().equals(ProvisioningState.SUCCEEDED)) {
            var specific =
                    (DatabricksJobWorkloadSpecific) provisionRequest.component().getSpecific();
            String errorMessage = String.format(
                    "The status of %s workspace is different from 'ACTIVE'. Please try again and if the error persists contact the platform team. ",
                    specific.getWorkspace());
            logger.info(errorMessage);
            updateStatus(token, ProvisioningStatus.StatusEnum.FAILED, errorMessage);
            return;
        }

        var databricksWorkspaceInfo = workspaceInfoOpt.get();

        var eitherWorkspaceClient = workspaceHandler.getWorkspaceClient(databricksWorkspaceInfo);
        if (eitherWorkspaceClient.isLeft()) {
            handleFailure(token, eitherWorkspaceClient.getLeft());
            return;
        }

        Either<FailedOperation, Void> eitherDeleteJob = jobWorkloadHandler.unprovisionWorkload(
                provisionRequest, eitherWorkspaceClient.get(), databricksWorkspaceInfo);
        if (eitherDeleteJob.isLeft()) {
            handleFailure(token, eitherDeleteJob.getLeft());
            return;
        }

        logger.info(String.format(
                "Unprovisioning of %s completed", provisionRequest.component().getName()));
        updateStatus(token, ProvisioningStatus.StatusEnum.COMPLETED, "");
    }

    private void unprovisionDLT(ProvisionRequest provisionRequest, String token) {
        Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> eitherWorkspaceExists =
                workspaceHandler.getWorkspaceInfo(provisionRequest);
        if (eitherWorkspaceExists.isLeft()) {
            handleFailure(token, eitherWorkspaceExists.getLeft());
            return;
        }

        var workspaceInfoOpt = eitherWorkspaceExists.get();
        if (workspaceInfoOpt.isEmpty()) {
            var specific =
                    (DatabricksDLTWorkloadSpecific) provisionRequest.component().getSpecific();
            logger.info(String.format("Unprovision skipped. Workspace %s not found.", specific.getWorkspace()));
            updateStatus(
                    token,
                    ProvisioningStatus.StatusEnum.COMPLETED,
                    String.format("Unprovision skipped. Workspace %s not found.", specific.getWorkspace()));
            return;
        }
        if (!workspaceInfoOpt.get().getProvisioningState().equals(ProvisioningState.SUCCEEDED)) {
            var specific =
                    (DatabricksDLTWorkloadSpecific) provisionRequest.component().getSpecific();
            String errorMessage = String.format(
                    "The status of %s workspace is different from 'ACTIVE'. Please try again and if the error persists contact the platform team. ",
                    specific.getWorkspace());
            logger.info(errorMessage);
            updateStatus(token, ProvisioningStatus.StatusEnum.FAILED, errorMessage);
            return;
        }

        var databricksWorkspaceInfo = workspaceInfoOpt.get();

        var eitherWorkspaceClient = workspaceHandler.getWorkspaceClient(databricksWorkspaceInfo);
        if (eitherWorkspaceClient.isLeft()) {
            handleFailure(token, eitherWorkspaceClient.getLeft());
            return;
        }

        Either<FailedOperation, Void> eitherDeletePipeline = dltWorkloadHandler.unprovisionWorkload(
                provisionRequest, eitherWorkspaceClient.get(), databricksWorkspaceInfo);
        if (eitherDeletePipeline.isLeft()) {
            handleFailure(token, eitherDeletePipeline.getLeft());
            return;
        }

        logger.info(String.format(
                "Unprovisioning of %s completed", provisionRequest.component().getName()));
        updateStatus(token, ProvisioningStatus.StatusEnum.COMPLETED, "");
    }

    private void handleFailure(String token, FailedOperation failure) {
        StringBuilder errors = new StringBuilder("Errors: ");
        failure.problems()
                .forEach(problem ->
                        errors.append("-").append(problem.description()).append("\n"));
        logger.error(errors.toString());
        updateStatus(token, ProvisioningStatus.StatusEnum.FAILED, errors.toString());
    }
}
