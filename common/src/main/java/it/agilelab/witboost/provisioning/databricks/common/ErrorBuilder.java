package it.agilelab.witboost.provisioning.databricks.common;

import it.agilelab.witboost.provisioning.databricks.openapi.model.ErrorMoreInfo;
import it.agilelab.witboost.provisioning.databricks.openapi.model.RequestValidationError;
import it.agilelab.witboost.provisioning.databricks.openapi.model.SystemError;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ErrorBuilder {

    public static RequestValidationError buildRequestValidationError(
            Optional<String> message,
            FailedOperation failedOperation,
            Optional<String> input,
            Optional<String> inputErrorField) {

        List<String> problems =
                failedOperation.problems().stream().map(Problem::getMessage).collect(Collectors.toList());

        ArrayList<String> solutions = new ArrayList<>(failedOperation.problems().stream()
                .flatMap(p -> p.solutions().stream())
                .toList());
        solutions.add("If the problem persists, contact the platform team");

        return new RequestValidationError(problems)
                .userMessage(message.orElse(
                        "Validation on the received descriptor failed, check the error details for more information"))
                .input(input.orElse(null))
                .inputErrorField(inputErrorField.orElse(null))
                .moreInfo(new ErrorMoreInfo(problems, solutions));
    }

    public static SystemError buildSystemError(Optional<String> message, Throwable throwable) {

        List<String> problems = List.of(throwable.getMessage());

        List<String> solutions = List.of("Please try again and if the problem persists contact the platform team.");

        return new SystemError(throwable.getMessage())
                .userMessage(
                        message.orElse(
                                "An unexpected error occurred while processing the request. Check the error details for more information"))
                .moreInfo(new ErrorMoreInfo(problems, solutions));
    }
}
