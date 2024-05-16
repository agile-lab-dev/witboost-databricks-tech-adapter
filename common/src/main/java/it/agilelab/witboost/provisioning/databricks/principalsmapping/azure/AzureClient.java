package it.agilelab.witboost.provisioning.databricks.principalsmapping.azure;

import io.vavr.control.Either;

public interface AzureClient {

    /**
     * Retrieve the corresponding Azure objectId for the given mail address
     * @param mail user mail address
     * @return either an error or the corresponding objectId
     */
    Either<Throwable, String> getUserId(String mail);

    /**
     * Retrieve the corresponding Azure objectId for the given group name
     * @param group group name
     * @return either an error or the corresponding objectId
     */
    Either<Throwable, String> getGroupId(String group);
}
