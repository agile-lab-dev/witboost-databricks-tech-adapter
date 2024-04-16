package it.agilelab.witboost.provisioning.databricks.api;

import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.common.SpecificProvisionerValidationException;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningStatus;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ValidationError;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ValidationResult;
import it.agilelab.witboost.provisioning.databricks.service.validation.ValidationService;
import java.util.Collections;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;

@Service
public class ApiServiceImpl {

    private final ValidationService service;

    public ApiServiceImpl(ValidationService validationService) {
        this.service = validationService;
    }

    public ValidationResult validate(ProvisioningRequest provisioningRequest) {
        Either<FailedOperation, ProvisionRequest<? extends Specific>> validate = service.validate(provisioningRequest);
        return validate.fold(
                failedOperation -> new ValidationResult(false)
                        .error(new ValidationError(failedOperation.problems().stream()
                                .map(Problem::description)
                                .collect(Collectors.toList()))),
                provisionRequest -> new ValidationResult(true));
    }

    public ProvisioningStatus provision(ProvisioningRequest provisioningRequest) {
        throw new SpecificProvisionerValidationException(new FailedOperation(
                Collections.singletonList(new Problem("Implement the provision logic based on your requirements!"))));
    }

    public ProvisioningStatus unprovision(ProvisioningRequest provisioningRequest) {
        throw new SpecificProvisionerValidationException(new FailedOperation(
                Collections.singletonList(new Problem("Implement the unprovision logic based on your requirements!"))));
    }
}
