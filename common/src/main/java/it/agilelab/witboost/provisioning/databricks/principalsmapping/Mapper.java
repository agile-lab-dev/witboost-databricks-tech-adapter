package it.agilelab.witboost.provisioning.databricks.principalsmapping;

import io.vavr.control.Either;
import java.util.Map;
import java.util.Set;

public interface Mapper {

    /**
     * This method defines the main mapping logic
     *
     * @param subjects set of subjects, i.e. witboost users and groups
     * @return the mapping. For each subject, we can return either Throwable, or the successfully mapped principal
     */
    Map<String, Either<Throwable, String>> map(Set<String> subjects);
}
