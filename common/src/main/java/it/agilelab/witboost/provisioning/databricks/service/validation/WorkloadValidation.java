package it.agilelab.witboost.provisioning.databricks.service.validation;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.Component;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.Workload;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkloadSpecific;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkloadValidation {

    private static final Logger logger = LoggerFactory.getLogger(WorkloadValidation.class);

    public static Either<FailedOperation, Void> validate(Component<? extends Specific> component) {
        logger.info("Checking component with ID {} is of type Workload", component.getId());
        if (!(component instanceof Workload<? extends Specific>)) {
            String errorMessage = String.format("The component %s is not of type Workload", component.getId());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }

        logger.info("Checking specific section of component {} is of type StorageSpecific", component.getId());
        if (!(component.getSpecific() instanceof DatabricksWorkloadSpecific)) {
            String errorMessage = String.format(
                    "The specific section of the component %s is not of type DatabricksWorkloadSpecific",
                    component.getId());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }

        logger.info("Validation of Workload {} completed successfully", component.getId());
        return right(null);
    }
}
