package it.agilelab.witboost.provisioning.databricks.service.reverseprovision;

import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningStatus;

/***
 * Reverse Provisioning services
 */
public interface ReverseProvisionService {

    /**
     * Reverse provision of Databricks components
     *
     * @param reverseProvisioningRequest A reverse provision request object
     * @return It synchronously returns the output of the reverse provision
     */
    ReverseProvisioningStatus runReverseProvisioning(ReverseProvisioningRequest reverseProvisioningRequest);
}
