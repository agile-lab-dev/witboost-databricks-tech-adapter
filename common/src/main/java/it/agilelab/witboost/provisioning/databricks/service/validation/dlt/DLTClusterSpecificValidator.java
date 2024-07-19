package it.agilelab.witboost.provisioning.databricks.service.validation.dlt;

import com.databricks.sdk.service.pipelines.PipelineClusterAutoscaleMode;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.DLTClusterSpecific;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

public class DLTClusterSpecificValidator implements ConstraintValidator<ValidDLTClusterSpecific, DLTClusterSpecific> {

    @Override
    public void initialize(ValidDLTClusterSpecific constraintAnnotation) {}

    @Override
    public boolean isValid(DLTClusterSpecific dltClusterSpecific, ConstraintValidatorContext context) {

        PipelineClusterAutoscaleMode mode = dltClusterSpecific.getMode();
        Long minWorkers = dltClusterSpecific.getMinWorkers();
        Long maxWorkers = dltClusterSpecific.getMaxWorkers();
        Long numWorkers = dltClusterSpecific.getNumWorkers();

        if (mode == PipelineClusterAutoscaleMode.ENHANCED || mode == PipelineClusterAutoscaleMode.LEGACY) {
            return minWorkers != null && maxWorkers != null && numWorkers == null;
        } else {
            return minWorkers == null && maxWorkers == null && numWorkers != null;
        }
    }
}
