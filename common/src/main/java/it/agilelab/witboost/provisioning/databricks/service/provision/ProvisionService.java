package it.agilelab.witboost.provisioning.databricks.service.provision;

import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningStatus;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ValidationResult;

/***
 * Provision services
 */
public interface ProvisionService {
    /**
     * Validate the provisioning request
     *
     * @param provisioningRequest request to validate
     * @return the outcome of the validation
     */
    ValidationResult validate(ProvisioningRequest provisioningRequest);

    /**
     * Provision the component present in the request
     *
     * @param provisioningRequest the request
     * @return a token that can be used for polling the request status
     */
    String provision(ProvisioningRequest provisioningRequest);

    /**
     * Unprovision the component present in the request
     *
     * @param provisioningRequest the request
     * @return a token that can be used for polling the request status
     */
    String unprovision(ProvisioningRequest provisioningRequest);

    /**
     * Get the provisioning status of a previous request
     *
     * @param token the token returned by the previous asynchronous request
     * @return the outcome of the request
     */
    ProvisioningStatus getStatus(String token);
}
