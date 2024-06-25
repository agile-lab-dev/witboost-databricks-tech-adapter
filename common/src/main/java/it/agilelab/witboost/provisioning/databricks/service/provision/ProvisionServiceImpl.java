package it.agilelab.witboost.provisioning.databricks.service.provision;

import com.databricks.sdk.WorkspaceClient;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.common.SpecificProvisionerValidationException;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.openapi.model.Info;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningStatus;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ValidationError;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ValidationResult;
import it.agilelab.witboost.provisioning.databricks.service.validation.ValidationService;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ProvisionServiceImpl implements ProvisionService {

    private final ValidationService validationService;
    private final WorkloadHandler workloadHandler;
    private final WorkspaceHandler workspaceHandler;
    private final String WORKLOAD_KIND = "workload";
    private final Logger logger = LoggerFactory.getLogger(ProvisionServiceImpl.class);

    public ProvisionServiceImpl(
            ValidationService validationService, WorkloadHandler workloadHandler, WorkspaceHandler workspaceHandler) {
        this.validationService = validationService;
        this.workloadHandler = workloadHandler;
        this.workspaceHandler = workspaceHandler;
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
    public ProvisioningStatus provision(ProvisioningRequest provisioningRequest) {
        var eitherValidation = validationService.validate(provisioningRequest);
        if (eitherValidation.isLeft()) throw new SpecificProvisionerValidationException(eitherValidation.getLeft());

        var provisionRequest = eitherValidation.get();

        switch (provisionRequest.component().getKind()) {
            case WORKLOAD_KIND: {
                Either<FailedOperation, DatabricksWorkspaceInfo> eitherCreatedWorkspace =
                        workspaceHandler.provisionWorkspace(provisionRequest);
                if (eitherCreatedWorkspace.isLeft())
                    throw new SpecificProvisionerValidationException(eitherCreatedWorkspace.getLeft());

                DatabricksWorkspaceInfo databricksWorkspaceInfo = eitherCreatedWorkspace.get();

                Either<FailedOperation, WorkspaceClient> eitherWorkspaceClient =
                        workspaceHandler.getWorkspaceClient(databricksWorkspaceInfo);
                if (eitherWorkspaceClient.isLeft())
                    throw new SpecificProvisionerValidationException(eitherWorkspaceClient.getLeft());

                Either<FailedOperation, String> eitherNewJob = workloadHandler.provisionWorkload(
                        provisionRequest, eitherWorkspaceClient.get(), databricksWorkspaceInfo);
                if (eitherNewJob.isLeft())
                    throw new SpecificProvisionerValidationException(eitherWorkspaceClient.getLeft());

                String jobUrl =
                        "https://" + databricksWorkspaceInfo.getDatabricksHost() + "/jobs/" + eitherNewJob.get();
                Map<String, String> provisionResult = new HashMap<>();
                provisionResult.put("workspace path", databricksWorkspaceInfo.getAzureResourceUrl());
                provisionResult.put("job path", jobUrl);

                return new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "")
                        .info(new Info(JsonNodeFactory.instance.objectNode(), provisionResult)
                                .publicInfo(provisionResult));
            }
            default:
                throw new SpecificProvisionerValidationException(
                        unsupportedKind(provisionRequest.component().getKind()));
        }
    }

    @Override
    public ProvisioningStatus unprovision(ProvisioningRequest provisioningRequest) {
        var eitherValidation = validationService.validate(provisioningRequest);
        if (eitherValidation.isLeft()) throw new SpecificProvisionerValidationException(eitherValidation.getLeft());

        var provisionRequest = eitherValidation.get();

        switch (provisionRequest.component().getKind()) {
            case WORKLOAD_KIND: {
                var workspaceName = workspaceHandler.getWorkspaceName(provisionRequest);
                if (workspaceName.isLeft()) throw new SpecificProvisionerValidationException(workspaceName.getLeft());

                Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> eitherGetWorkspaceInfo =
                        workspaceHandler.getWorkspaceInfo(provisionRequest);
                if (eitherGetWorkspaceInfo.isLeft())
                    throw new SpecificProvisionerValidationException(eitherGetWorkspaceInfo.getLeft());

                Optional<DatabricksWorkspaceInfo> optionalDatabricksWorkspaceInfo = eitherGetWorkspaceInfo.get();
                if (optionalDatabricksWorkspaceInfo.isEmpty())
                    return new ProvisioningStatus(
                            ProvisioningStatus.StatusEnum.COMPLETED,
                            String.format("Unprovision skipped. Workspace %s does not exists", workspaceName.get()));

                DatabricksWorkspaceInfo databricksWorkspaceInfo = optionalDatabricksWorkspaceInfo.get();

                Either<FailedOperation, WorkspaceClient> eitherWorkspaceClient =
                        workspaceHandler.getWorkspaceClient(databricksWorkspaceInfo);
                if (eitherWorkspaceClient.isLeft())
                    throw new SpecificProvisionerValidationException(eitherWorkspaceClient.getLeft());

                WorkspaceClient workspaceClient = eitherWorkspaceClient.get();

                Either<FailedOperation, Void> eitherDeletedJob =
                        workloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, databricksWorkspaceInfo);
                if (eitherDeletedJob.isLeft())
                    throw new SpecificProvisionerValidationException(eitherDeletedJob.getLeft());

                return new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "");
            }
            default:
                throw new SpecificProvisionerValidationException(
                        unsupportedKind(provisionRequest.component().getKind()));
        }
    }

    private FailedOperation unsupportedKind(String kind) {
        return new FailedOperation(Collections.singletonList(new Problem(
                String.format("The kind '%s' of the component is not supported by this Specific Provisioner", kind))));
    }
}
