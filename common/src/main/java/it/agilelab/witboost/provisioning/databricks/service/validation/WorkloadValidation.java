package it.agilelab.witboost.provisioning.databricks.service.validation;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.TemplatesConfig;
import it.agilelab.witboost.provisioning.databricks.model.Component;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.Workload;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.DatabricksDLTWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.DatabricksJobWorkloadSpecific;
import jakarta.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

@org.springframework.stereotype.Component
public class WorkloadValidation {

    private static final Logger logger = LoggerFactory.getLogger(WorkloadValidation.class);
    private static TemplatesConfig templatesConfig;

    @Autowired
    public WorkloadValidation(TemplatesConfig templatesConfig) {
        this.templatesConfig = templatesConfig;
    }

    public static Either<FailedOperation, Void> validate(Component<? extends Specific> component) {
        logger.info("Checking component with ID {} is of type Workload", component.getName());
        if (!(component instanceof Workload<? extends Specific>)) {
            String errorMessage = String.format("The component %s is not of type Workload", component.getName());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }

        var useCaseTemplateIdOptional = ((Workload<? extends Specific>) component).getUseCaseTemplateId();
        if (useCaseTemplateIdOptional == null || useCaseTemplateIdOptional.isEmpty())
            return left(new FailedOperation(Collections.singletonList(
                    new Problem("useCaseTemplateId is mandatory to detect the workload kind (job or dlt pipeline)"))));

        String useCaseTemplateId = getUseCaseTemplateId(useCaseTemplateIdOptional.get());

        if (templatesConfig.getJob().contains(useCaseTemplateId.toString())) {
            logger.info(
                    "Checking specific section of component {} is of type DatabricksJobWorkloadSpecific",
                    component.getName());
            if (!(component.getSpecific() instanceof DatabricksJobWorkloadSpecific)) {
                String errorMessage = String.format(
                        "The specific section of the component %s is not of type DatabricksJobWorkloadSpecific",
                        component.getName());
                logger.error(errorMessage);
                return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
            }
            logger.info("Validation of Workload {} completed successfully", component.getName());
            return right(null);
        } else if (templatesConfig.getDlt().contains(useCaseTemplateId)) {
            logger.info(
                    "Checking specific section of component {} is of type DatabricksDLTWorkloadSpecific",
                    component.getName());
            if (!(component.getSpecific() instanceof DatabricksDLTWorkloadSpecific)) {
                String errorMessage = String.format(
                        "The specific section of the component %s is not of type DatabricksDLTWorkloadSpecific",
                        component.getName());
                logger.error(errorMessage);
                return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
            }
            logger.info("Validation of Workload {} completed successfully", component.getName());
            return right(null);
        }

        String errorMessage = String.format(
                "%s (component %s) is not an accepted useCaseTemplateId for Databricks jobs or DLT pipelines.",
                ((Workload<? extends Specific>) component).getUseCaseTemplateId(), component.getName());
        logger.error(errorMessage);
        return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
    }

    @NotNull
    private static String getUseCaseTemplateId(String useCaseTemplateIdFull) {
        String[] parts = useCaseTemplateIdFull.split(":");
        String[] useCaseTemplateIdParts = Arrays.copyOfRange(parts, 0, parts.length - 1);
        return String.join(":", useCaseTemplateIdParts);
    }
}
