package it.agilelab.witboost.provisioning.databricks.service.validation;

import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningRequest;

public interface ValidationService {

    Either<FailedOperation, ProvisionRequest<? extends Specific>> validate(ProvisioningRequest provisioningRequest);
}
