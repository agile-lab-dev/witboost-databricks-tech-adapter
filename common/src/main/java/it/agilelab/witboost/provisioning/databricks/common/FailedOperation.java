package it.agilelab.witboost.provisioning.databricks.common;

import java.util.List;
import java.util.Objects;

public record FailedOperation(List<Problem> problems) {
    public FailedOperation {
        Objects.requireNonNull(problems);
    }
}
