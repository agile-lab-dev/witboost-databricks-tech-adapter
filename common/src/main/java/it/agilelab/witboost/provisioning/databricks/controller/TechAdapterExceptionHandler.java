package it.agilelab.witboost.provisioning.databricks.controller;

import it.agilelab.witboost.provisioning.databricks.common.ErrorBuilder;
import it.agilelab.witboost.provisioning.databricks.common.TechAdapterValidationException;
import it.agilelab.witboost.provisioning.databricks.openapi.model.RequestValidationError;
import it.agilelab.witboost.provisioning.databricks.openapi.model.SystemError;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Exception handler for the API layer.
 *
 * <p>The following methods wrap generic exceptions into 400 and 500 errors. Implement your own
 * exception handlers based on the business exception that the provisioner throws. No further
 * modifications need to be done outside this file to make it work, as Spring identifies at startup
 * the handlers with the @ExceptionHandler annotation
 */
@RestControllerAdvice
public class TechAdapterExceptionHandler {

    private final Logger logger = LoggerFactory.getLogger(TechAdapterExceptionHandler.class);

    @ExceptionHandler({TechAdapterValidationException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    protected RequestValidationError handleValidationException(TechAdapterValidationException ex) {
        logger.error("Tech adapter validation error", ex);
        return ErrorBuilder.buildRequestValidationError(
                Optional.ofNullable(ex.getMessage()), ex.getFailedOperation(), ex.getInput(), ex.getInputErrorField());
    }

    @ExceptionHandler({RuntimeException.class})
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    protected SystemError handleSystemError(RuntimeException ex) {
        String errorMessage = String.format(
                "An unexpected error occurred while processing the request. Please try again later. If the issue still persists, contact the platform team for assistance! Details: %s",
                ex.getMessage());
        logger.error(errorMessage, ex);
        return ErrorBuilder.buildSystemError(Optional.of(errorMessage), ex);
    }
}
