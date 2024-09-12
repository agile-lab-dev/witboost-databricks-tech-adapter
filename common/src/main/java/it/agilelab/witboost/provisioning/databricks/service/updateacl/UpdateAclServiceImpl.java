package it.agilelab.witboost.provisioning.databricks.service.updateacl;

import com.databricks.sdk.WorkspaceClient;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.client.UnityCatalogManager;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.common.SpecificProvisionerValidationException;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksOutputPortSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.openapi.model.*;
import it.agilelab.witboost.provisioning.databricks.service.WorkspaceHandler;
import it.agilelab.witboost.provisioning.databricks.service.provision.OutputPortHandler;
import it.agilelab.witboost.provisioning.databricks.service.validation.ValidationService;
import java.util.Collections;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class UpdateAclServiceImpl implements UpdateAclService {

    private final ValidationService validationService;
    private final WorkspaceHandler workspaceHandler;
    private final OutputPortHandler outputPortHandler;
    private final String OUTPUTPORT_KIND = "outputport";
    private final Logger logger = LoggerFactory.getLogger(UpdateAclServiceImpl.class);

    public UpdateAclServiceImpl(
            ValidationService validationService,
            WorkspaceHandler workspaceHandler,
            OutputPortHandler outputPortHandler) {
        this.validationService = validationService;
        this.workspaceHandler = workspaceHandler;
        this.outputPortHandler = outputPortHandler;
    }

    @Override
    public ProvisioningStatus updateAcl(UpdateAclRequest updateAclRequest) {

        // Retrieve the ProvisioningRequest from the UpdateAclRequest in order to reuse the validationService
        ProvisioningRequest provisioningRequest = new ProvisioningRequest(
                DescriptorKind.COMPONENT_DESCRIPTOR,
                updateAclRequest.getProvisionInfo().getRequest(),
                Boolean.FALSE);

        Either<FailedOperation, ProvisionRequest<? extends Specific>> eitherValidation =
                validationService.validate(provisioningRequest);
        if (eitherValidation.isLeft()) throw new SpecificProvisionerValidationException(eitherValidation.getLeft());

        ProvisionRequest<? extends Specific> provisionRequest = eitherValidation.get();

        String kind = provisionRequest.component().getKind();

        // Creation of workspaceClient
        DatabricksOutputPortSpecific databricksOutputPortSpecific =
                (DatabricksOutputPortSpecific) provisionRequest.component().getSpecific();

        String workspaceName = databricksOutputPortSpecific.getWorkspaceOP();

        Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> eitherDatabricksWorkspaceInfo =
                workspaceHandler.getWorkspaceInfo(workspaceName);
        if (eitherDatabricksWorkspaceInfo.isLeft()) {
            return new ProvisioningStatus(
                    ProvisioningStatus.StatusEnum.FAILED, "Update Acl failed while getting workspace info");
        }

        var databricksWorkspaceInfo = eitherDatabricksWorkspaceInfo.get().get();

        var eitherWorkspaceClient = workspaceHandler.getWorkspaceClient(databricksWorkspaceInfo);
        if (eitherWorkspaceClient.isLeft()) {
            return new ProvisioningStatus(
                    ProvisioningStatus.StatusEnum.FAILED, "Update Acl failed while getting workspace client");
        }

        WorkspaceClient workspaceClient = eitherWorkspaceClient.get();

        if (kind.equals(OUTPUTPORT_KIND)) {
            Either<FailedOperation, ProvisioningStatus> eitherUpdatedAcl = outputPortHandler.updateAcl(
                    (ProvisionRequest<DatabricksOutputPortSpecific>) provisionRequest,
                    updateAclRequest,
                    workspaceClient,
                    new UnityCatalogManager(workspaceClient, databricksWorkspaceInfo));
            if (eitherUpdatedAcl.isLeft()) {
                return new ProvisioningStatus(
                        ProvisioningStatus.StatusEnum.FAILED,
                        "Update Acl failed while assigning Databricks permissions.");
            } else {
                return eitherUpdatedAcl.get();
            }
        } else {
            String error =
                    String.format("The kind '%s' of the component is not supported by this Specific Provisioner", kind);
            logger.error(error);
            throw new SpecificProvisionerValidationException(
                    new FailedOperation(Collections.singletonList(new Problem(error))));
        }
    }
}
