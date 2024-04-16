package it.agilelab.witboost.provisioning.databricks.service.validation;

import static io.vavr.control.Either.left;

import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.Component;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import java.util.Collections;

public class StorageAreaValidation {

    public static Either<FailedOperation, Void> validate(Component<? extends Specific> component) {
        // TODO Remember to implement the validation for the storage area.
        return left(new FailedOperation(Collections.singletonList(
                new Problem("Implement the validation for storage area based on your requirements!"))));
    }
}
