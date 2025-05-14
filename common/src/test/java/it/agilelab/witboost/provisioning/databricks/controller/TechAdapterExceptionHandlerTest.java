package it.agilelab.witboost.provisioning.databricks.controller;

import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.common.TechAdapterValidationException;
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
@WebMvcTest(TechAdapterExceptionHandler.class)
public class TechAdapterExceptionHandlerTest {

    @InjectMocks
    TechAdapterExceptionHandler techAdapterExceptionHandler;

    @Test
    void testHandleConflictSystemError() {
        RuntimeException runtimeException = new RuntimeException("exception message");
        String expectedError =
                "An unexpected error occurred while processing the request. Please try again later. If the issue still persists, contact the platform team for assistance! Details: ";

        SystemError error = techAdapterExceptionHandler.handleSystemError(runtimeException);

        Assertions.assertTrue(error.getUserMessage().startsWith(expectedError));
    }

    @Test
    void testHandleConflictRequestValidationError() {
        String expectedError = "Validation error";
        TechAdapterValidationException techAdapterValidationException = new TechAdapterValidationException(
                "error", new FailedOperation(Collections.singletonList(new Problem(expectedError))));

        RequestValidationError requestValidationError =
                techAdapterExceptionHandler.handleValidationException(techAdapterValidationException);

        Assertions.assertEquals(1, requestValidationError.getErrors().size());
        requestValidationError.getErrors().forEach(e -> Assertions.assertEquals(expectedError, e));
    }
}
