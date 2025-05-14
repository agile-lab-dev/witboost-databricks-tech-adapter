package it.agilelab.witboost.provisioning.databricks.common;

import java.util.Optional;
import lombok.Getter;

@Getter
public class TechAdapterValidationException extends RuntimeException {

    private final FailedOperation failedOperation;
    private final Optional<String> input;
    private final Optional<String> inputErrorField;

    public TechAdapterValidationException(String message, FailedOperation failedOperation) {
        this(message, failedOperation, null, null);
    }

    public TechAdapterValidationException(
            String message, FailedOperation failedOperation, String input, String inputErrorField) {
        super(message);
        this.failedOperation = failedOperation;
        this.input = Optional.ofNullable(input);
        this.inputErrorField = Optional.ofNullable(inputErrorField);
    }
}
