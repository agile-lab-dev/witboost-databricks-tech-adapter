package it.agilelab.witboost.provisioning.databricks.controller;

import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.common.SpecificProvisionerValidationException;
import it.agilelab.witboost.provisioning.databricks.openapi.model.RequestValidationError;
import it.agilelab.witboost.provisioning.databricks.openapi.model.SystemError;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;

@ExtendWith(MockitoExtension.class)
@WebMvcTest(SpecificProvisionerExceptionHandler.class)
public class SpecificProvisionerExceptionHandlerTest {

    @InjectMocks
    SpecificProvisionerExceptionHandler specificProvisionerExceptionHandler;

    @Test
    void testHandleConflictSystemError() {
        RuntimeException runtimeException = new RuntimeException();
        String expectedError =
                "An unexpected error occurred while processing the request. Please try again later. If the issue still persists, contact the platform team for assistance! Details: ";

        SystemError error = specificProvisionerExceptionHandler.handleSystemError(runtimeException);

        Assertions.assertTrue(error.getError().startsWith(expectedError));
    }

    @Test
    void testHandleConflictRequestValidationError() {
        String expectedError = "Validation error";
        SpecificProvisionerValidationException specificProvisionerValidationException =
                new SpecificProvisionerValidationException(
                        new FailedOperation(Collections.singletonList(new Problem(expectedError))));

        RequestValidationError requestValidationError =
                specificProvisionerExceptionHandler.handleValidationException(specificProvisionerValidationException);

        Assertions.assertEquals(1, requestValidationError.getErrors().size());
        requestValidationError.getErrors().forEach(e -> Assertions.assertEquals(expectedError, e));
    }
}
