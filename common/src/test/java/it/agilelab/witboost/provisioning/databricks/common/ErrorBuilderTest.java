package it.agilelab.witboost.provisioning.databricks.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import it.agilelab.witboost.provisioning.databricks.openapi.model.ErrorMoreInfo;
import it.agilelab.witboost.provisioning.databricks.openapi.model.RequestValidationError;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ErrorBuilderTest {

    @Test
    void testBuildRequestValidationErrorWithAllParams() {
        String customMessage = "Custom user message";
        String input = "someInput.json";
        String inputErrorField = "field.path.error";

        Problem problem1 = mock(Problem.class);
        when(problem1.getMessage()).thenReturn("Problem 1");
        when(problem1.solutions()).thenReturn(Set.of("Solution for problem 1"));

        Problem problem2 = mock(Problem.class);
        when(problem2.getMessage()).thenReturn("Problem 2");
        when(problem2.solutions()).thenReturn(Set.of("Solution for problem 2"));

        FailedOperation failedOperation = new FailedOperation(List.of(problem1, problem2));

        RequestValidationError result = ErrorBuilder.buildRequestValidationError(
                Optional.of(customMessage), failedOperation, Optional.of(input), Optional.of(inputErrorField));

        assertEquals(List.of("Problem 1", "Problem 2"), result.getErrors());
        assertEquals(customMessage, result.getUserMessage());
        assertEquals(input, result.getInput());
        assertEquals(inputErrorField, result.getInputErrorField());
        ErrorMoreInfo moreInfo = result.getMoreInfo();
        assertEquals(List.of("Problem 1", "Problem 2"), moreInfo.getProblems());
        assertEquals(
                List.of(
                        "Solution for problem 1",
                        "Solution for problem 2",
                        "If the problem persists, contact the platform team"),
                moreInfo.getSolutions());
    }

    @Test
    void testBuildRequestValidationErrorWithDefaults() {
        Problem problem = mock(Problem.class);
        when(problem.getMessage()).thenReturn("Default Problem");
        when(problem.solutions()).thenReturn(Set.of("Default Solution"));

        FailedOperation failedOperation = new FailedOperation(List.of(problem));

        RequestValidationError result = ErrorBuilder.buildRequestValidationError(
                Optional.empty(), failedOperation, Optional.empty(), Optional.empty());

        assertEquals(List.of("Default Problem"), result.getErrors());
        assertEquals(
                "Validation on the received descriptor failed, check the error details for more information",
                result.getUserMessage());
        assertNull(result.getInput());
        assertNull(result.getInputErrorField());
        ErrorMoreInfo moreInfo = result.getMoreInfo();
        assertEquals(List.of("Default Problem"), moreInfo.getProblems());
        assertEquals(
                List.of("Default Solution", "If the problem persists, contact the platform team"),
                moreInfo.getSolutions());
    }

    @Test
    void testBuildRequestValidationErrorWithEmptyProblemList() {
        FailedOperation failedOperation = new FailedOperation(List.of());

        RequestValidationError result = ErrorBuilder.buildRequestValidationError(
                Optional.of("No problems reported."),
                failedOperation,
                Optional.of("emptyInput.json"),
                Optional.of("emptyField.path"));

        assertEquals(List.of(), result.getErrors());
        assertEquals("No problems reported.", result.getUserMessage());
        assertEquals("emptyInput.json", result.getInput());
        assertEquals("emptyField.path", result.getInputErrorField());
        ErrorMoreInfo moreInfo = result.getMoreInfo();
        assertEquals(List.of(), moreInfo.getProblems());
        assertEquals(List.of("If the problem persists, contact the platform team"), moreInfo.getSolutions());
    }
}
