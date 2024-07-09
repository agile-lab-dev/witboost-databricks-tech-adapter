package it.agilelab.witboost.provisioning.databricks.service.provision;

import com.databricks.sdk.WorkspaceClient;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
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
    private final WorkloadHandler workloadHandler;
    private final WorkspaceHandler workspaceHandler;
    private final String WORKLOAD_KIND = "workload";
    private final Logger logger = LoggerFactory.getLogger(ProvisionServiceImpl.class);

    public ProvisionServiceImpl(
            ValidationService validationService,
            WorkloadHandler workloadHandler,
            WorkspaceHandler workspaceHandler,
            ForkJoinPool forkJoinPool) {
        this.validationService = validationService;
        this.workloadHandler = workloadHandler;
        this.workspaceHandler = workspaceHandler;
        this.forkJoinPool = forkJoinPool;
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
        String token = startProvisioning(provisioningRequest);
        return token;
    }

    @Override
    public ProvisioningStatus getStatus(String token) {
        return statusMap.getOrDefault(
                token, new ProvisioningStatus(ProvisioningStatus.StatusEnum.FAILED, "Token not found"));
    }

    @Override
    public String unprovision(ProvisioningRequest provisioningRequest) {
        String token = startUnprovisioning(provisioningRequest);
        return token;
    }

    private String startProvisioning(ProvisioningRequest provisioningRequest) {
        String token = generateToken();
        ProvisioningStatus response =
                new ProvisioningStatus(ProvisioningStatus.StatusEnum.RUNNING, "Provisioning in progress");
        statusMap.put(token, response);

        forkJoinPool.submit(() -> {
            var eitherValidation = validationService.validate(provisioningRequest);
            if (eitherValidation.isLeft()) {
                StringBuilder errors = new StringBuilder();
                errors.append("Errors: ");
                eitherValidation.getLeft().problems().forEach(problem -> {
                    errors.append("-").append(problem.description()).append("\n");
                });
                updateStatus(token, ProvisioningStatus.StatusEnum.FAILED, errors.toString());
                return;
            }

            var provisionRequest = eitherValidation.get();

            if (provisionRequest.component().getKind().equalsIgnoreCase(WORKLOAD_KIND)) {
                Either<FailedOperation, DatabricksWorkspaceInfo> eitherCreatedWorkspace =
                        workspaceHandler.provisionWorkspace(provisionRequest);
                if (eitherCreatedWorkspace.isLeft()) {
                    StringBuilder errors = new StringBuilder();
                    errors.append("Errors: ");
                    eitherCreatedWorkspace.getLeft().problems().forEach(problem -> {
                        errors.append("-").append(problem.description()).append("\n");
                    });
                    updateStatus(token, ProvisioningStatus.StatusEnum.FAILED, errors.toString());
                    return;
                }

                DatabricksWorkspaceInfo databricksWorkspaceInfo = eitherCreatedWorkspace.get();

                Either<FailedOperation, WorkspaceClient> eitherWorkspaceClient =
                        workspaceHandler.getWorkspaceClient(databricksWorkspaceInfo);
                if (eitherWorkspaceClient.isLeft()) {
                    StringBuilder errors = new StringBuilder();
                    errors.append("Errors: ");
                    eitherWorkspaceClient.getLeft().problems().forEach(problem -> {
                        errors.append("-").append(problem.description()).append("\n");
                    });
                    updateStatus(token, ProvisioningStatus.StatusEnum.FAILED, errors.toString());
                    return;
                }

                Either<FailedOperation, String> eitherNewJob = workloadHandler.provisionWorkload(
                        provisionRequest, eitherWorkspaceClient.get(), databricksWorkspaceInfo);
                if (eitherNewJob.isLeft()) {
                    StringBuilder errors = new StringBuilder();
                    errors.append("Errors: ");
                    eitherNewJob.getLeft().problems().forEach(problem -> {
                        errors.append("-").append(problem.description()).append("\n");
                    });
                    updateStatus(token, ProvisioningStatus.StatusEnum.FAILED, errors.toString());
                    return;
                }

                String jobUrl =
                        "https://" + databricksWorkspaceInfo.getDatabricksHost() + "/jobs/" + eitherNewJob.get();
                Map<String, String> provisionResult = new HashMap<>();
                provisionResult.put("workspace path", databricksWorkspaceInfo.getAzureResourceUrl());
                provisionResult.put("job path", jobUrl);

                updateStatus(
                        token,
                        ProvisioningStatus.StatusEnum.COMPLETED,
                        "",
                        new Info(JsonNodeFactory.instance.objectNode(), provisionResult).publicInfo(provisionResult));

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

    private String startUnprovisioning(ProvisioningRequest provisioningRequest) {
        String token = generateToken();
        ProvisioningStatus response =
                new ProvisioningStatus(ProvisioningStatus.StatusEnum.RUNNING, "Unprovisioning in progress");
        statusMap.put(token, response);

        forkJoinPool.submit(() -> {
            var eitherValidation = validationService.validate(provisioningRequest);
            if (eitherValidation.isLeft()) {
                StringBuilder errors = new StringBuilder();
                errors.append("Errors: ");
                eitherValidation.getLeft().problems().forEach(problem -> {
                    errors.append("-").append(problem.description()).append("\n");
                });
                updateStatus(token, ProvisioningStatus.StatusEnum.FAILED, errors.toString());
                return;
            }
            var provisionRequest = eitherValidation.get();

            if (provisionRequest.component().getKind().equalsIgnoreCase(WORKLOAD_KIND)) {
                var workspaceName = workspaceHandler.getWorkspaceName(provisionRequest);
                if (workspaceName.isLeft()) {
                    StringBuilder errors = new StringBuilder();
                    errors.append("Errors: ");
                    workspaceName.getLeft().problems().forEach(problem -> {
                        errors.append("-").append(problem.description()).append("\n");
                    });
                    updateStatus(token, ProvisioningStatus.StatusEnum.FAILED, errors.toString());
                    return;
                }
                Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> eitherGetWorkspaceInfo =
                        workspaceHandler.getWorkspaceInfo(provisionRequest);
                if (eitherGetWorkspaceInfo.isLeft()) {
                    StringBuilder errors = new StringBuilder();
                    errors.append("Errors: ");
                    eitherGetWorkspaceInfo.getLeft().problems().forEach(problem -> {
                        errors.append("-").append(problem.description()).append("\n");
                    });
                    updateStatus(token, ProvisioningStatus.StatusEnum.FAILED, errors.toString());
                    return;
                }

                Optional<DatabricksWorkspaceInfo> optionalDatabricksWorkspaceInfo = eitherGetWorkspaceInfo.get();
                if (optionalDatabricksWorkspaceInfo.isEmpty()) {
                    updateStatus(
                            token,
                            ProvisioningStatus.StatusEnum.COMPLETED,
                            String.format("Unprovision skipped. Workspace %s does not exists", workspaceName.get()));
                    return;
                }
                DatabricksWorkspaceInfo databricksWorkspaceInfo = optionalDatabricksWorkspaceInfo.get();

                Either<FailedOperation, WorkspaceClient> eitherWorkspaceClient =
                        workspaceHandler.getWorkspaceClient(databricksWorkspaceInfo);
                if (eitherWorkspaceClient.isLeft()) {
                    StringBuilder errors = new StringBuilder();
                    errors.append("Errors: ");
                    eitherWorkspaceClient.getLeft().problems().forEach(problem -> {
                        errors.append("-").append(problem.description()).append("\n");
                    });
                    updateStatus(token, ProvisioningStatus.StatusEnum.FAILED, errors.toString());
                    return;
                }
                WorkspaceClient workspaceClient = eitherWorkspaceClient.get();

                Either<FailedOperation, Void> eitherDeletedJob =
                        workloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, databricksWorkspaceInfo);

                if (eitherDeletedJob.isLeft()) {
                    StringBuilder errors = new StringBuilder();
                    errors.append("Errors: ");
                    eitherDeletedJob.getLeft().problems().forEach(problem -> {
                        errors.append("-").append(problem.description()).append("\n");
                    });
                    updateStatus(token, ProvisioningStatus.StatusEnum.FAILED, errors.toString());
                    return;
                }

                updateStatus(token, ProvisioningStatus.StatusEnum.COMPLETED, "");

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
}
