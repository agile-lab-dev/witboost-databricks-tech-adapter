package it.agilelab.witboost.provisioning.databricks.service.updateacl;

import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningStatus;
import it.agilelab.witboost.provisioning.databricks.openapi.model.UpdateAclRequest;

/***
 * Update ACL services
 */
public interface UpdateAclService {

    /**
     * Request access to a specific provisioner component
     *
     * @param updateAclRequest An access request object
     * @return It synchronously returns the access request response
     */
    ProvisioningStatus updateAcl(UpdateAclRequest updateAclRequest);
}
