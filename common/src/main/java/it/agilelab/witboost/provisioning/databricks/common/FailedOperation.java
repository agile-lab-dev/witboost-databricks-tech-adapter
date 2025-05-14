package it.agilelab.witboost.provisioning.databricks.common;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public record FailedOperation(List<Problem> problems) {
    public FailedOperation {
        Objects.requireNonNull(problems);
    }

    public static FailedOperation singleProblemFailedOperation(String errorMessage) {
        return new FailedOperation(Collections.singletonList(new Problem(errorMessage)));
    }

    public static FailedOperation singleProblemFailedOperation(String errorMessage, Throwable cause) {
        return new FailedOperation(Collections.singletonList(new Problem(errorMessage, cause)));
    }
}
