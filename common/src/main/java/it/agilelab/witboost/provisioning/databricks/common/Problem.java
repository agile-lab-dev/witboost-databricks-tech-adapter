package it.agilelab.witboost.provisioning.databricks.common;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public record Problem(String description, Optional<Throwable> cause, Set<String> solutions) {

    public Problem {
        Objects.requireNonNull(description);
        Objects.requireNonNull(cause);
        Objects.requireNonNull(solutions);
    }

    public Problem(String description) {
        this(description, Optional.empty(), new HashSet<>());
    }

    public Problem(String description, Throwable cause) {
        this(description, Optional.of(cause), new HashSet<>());
    }
}
