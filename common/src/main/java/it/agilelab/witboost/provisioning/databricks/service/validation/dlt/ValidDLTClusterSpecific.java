package it.agilelab.witboost.provisioning.databricks.service.validation.dlt;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Constraint(validatedBy = DLTClusterSpecificValidator.class)
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidDLTClusterSpecific {
    String message() default
            "Invalid configuration for DLTClusterSpecific. If the cluster mode is 'ENHANCED' or 'LEGACY', minWorkers and maxWorkers must be specified, otherwise only numWorkers must be specified";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
