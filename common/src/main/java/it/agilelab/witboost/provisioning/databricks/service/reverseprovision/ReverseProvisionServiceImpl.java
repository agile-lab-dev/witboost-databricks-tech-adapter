package it.agilelab.witboost.provisioning.databricks.service.reverseprovision;

import static it.agilelab.witboost.provisioning.databricks.service.reverseprovision.ReverseProvisionStatusHandler.handleReverseProvisioningStatusFailed;

import it.agilelab.witboost.provisioning.databricks.config.OutputPortTemplatesConfig;
import it.agilelab.witboost.provisioning.databricks.config.WorkloadTemplatesConfig;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningStatus;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ReverseProvisionServiceImpl implements ReverseProvisionService {

    private final Logger logger = LoggerFactory.getLogger(ReverseProvisionServiceImpl.class);
    // private final WorkflowReverseProvision workflowReverseProvision;
    private final OutputPortReverseProvision outputPortReverseProvision;
    private final WorkloadTemplatesConfig workloadTemplatesConfig;
    private final OutputPortTemplatesConfig outputPortTemplatesConfig;

    public ReverseProvisionServiceImpl(
            // WorkflowReverseProvision workflowReverseProvision,
            OutputPortReverseProvision outputPortReverseProvision,
            WorkloadTemplatesConfig workloadTemplatesConfig,
            OutputPortTemplatesConfig outputPortTemplatesConfig) {
        // this.workflowReverseProvision = workflowReverseProvision;
        this.outputPortReverseProvision = outputPortReverseProvision;
        this.workloadTemplatesConfig = workloadTemplatesConfig;
        this.outputPortTemplatesConfig = outputPortTemplatesConfig;
    }

    @Override
    public ReverseProvisioningStatus runReverseProvisioning(ReverseProvisioningRequest reverseProvisioningRequest) {
        return startReverseProvisioning(reverseProvisioningRequest);
    }

    private ReverseProvisioningStatus startReverseProvisioning(ReverseProvisioningRequest reverseProvisioningRequest) {

        String useCaseTemplateId = getUseCaseTemplateId(reverseProvisioningRequest.getUseCaseTemplateId());

        if (outputPortTemplatesConfig.getOutputport().contains(useCaseTemplateId))
            return outputPortReverseProvision.reverseProvision(reverseProvisioningRequest);
        // else if (workloadTemplatesConfig.getWorkflow().contains(useCaseTemplateId))
        //    return workflowReverseProvision.reverseProvision(reverseProvisioningRequest);

        return handleReverseProvisioningStatusFailed(String.format(
                "The useCaseTemplateId '%s' of the component is not supported by this Tech Adapter",
                useCaseTemplateId));
    }

    private static String getUseCaseTemplateId(String useCaseTemplateIdFull) {
        String[] parts = useCaseTemplateIdFull.split(":");
        String[] useCaseTemplateIdParts = Arrays.copyOfRange(parts, 0, parts.length - 1);
        String useCaseTemplateId = String.join(":", useCaseTemplateIdParts);
        String cleanUseCaseTemplateId = useCaseTemplateId.replace("\"", "").trim();
        return cleanUseCaseTemplateId;
    }
}
