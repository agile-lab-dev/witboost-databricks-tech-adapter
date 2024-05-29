package it.agilelab.witboost.provisioning.databricks.service.provision;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.common.SpecificProvisionerValidationException;
import it.agilelab.witboost.provisioning.databricks.openapi.model.Info;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningStatus;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ValidationError;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ValidationResult;
import it.agilelab.witboost.provisioning.databricks.service.validation.ValidationService;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ProvisionServiceImpl implements ProvisionService {

    private final ValidationService validationService;
    private final WorkloadHandler workloadHandler;

    private final String WORKLOAD_KIND = "workload";
    private final Logger logger = LoggerFactory.getLogger(ProvisionServiceImpl.class);

    public ProvisionServiceImpl(ValidationService validationService, WorkloadHandler workloadHandler) {
        this.validationService = validationService;
        this.workloadHandler = workloadHandler;
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
                Either<FailedOperation, String> eitherCreatedWorkspace =
                        workloadHandler.createNewWorkspaceWithPermissions(provisionRequest);
                if (eitherCreatedWorkspace.isLeft())
                    throw new SpecificProvisionerValidationException(eitherCreatedWorkspace.getLeft());
                String workspacePath = eitherCreatedWorkspace.get();
                var workspaceInfo = Map.of("path", workspacePath);
                return new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "")
                        .info(new Info(JsonNodeFactory.instance.objectNode(), workspaceInfo).publicInfo(workspaceInfo));
            }
            default:
                throw new SpecificProvisionerValidationException(
                        unsupportedKind(provisionRequest.component().getKind()));
        }
    }

    @Override
    public ProvisioningStatus unprovision(ProvisioningRequest provisioningRequest) {
        return null;
    }

    private FailedOperation unsupportedKind(String kind) {
        return new FailedOperation(Collections.singletonList(new Problem(
                String.format("The kind '%s' of the component is not supported by this Specific Provisioner", kind))));
    }
}
