package it.agilelab.witboost.provisioning.databricks.service.reverseprovision;

import static it.agilelab.witboost.provisioning.databricks.service.reverseprovision.ReverseProvisionStatusHandler.handleReverseProvisioningStatusCompleted;
import static it.agilelab.witboost.provisioning.databricks.service.reverseprovision.ReverseProvisionStatusHandler.handleReverseProvisioningStatusFailed;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.OutputPortTemplatesConfig;
import it.agilelab.witboost.provisioning.databricks.config.WorkloadTemplatesConfig;
import it.agilelab.witboost.provisioning.databricks.model.reverseprovisioningrequest.CatalogInfo;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningStatus;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ReverseProvisionServiceImpl implements ReverseProvisionService {

    private final Logger logger = LoggerFactory.getLogger(ReverseProvisionServiceImpl.class);
    private final WorkflowReverseProvisionHandler workflowReverseProvisionHandler;
    private final OutputPortReverseProvisionHandler outputPortReverseProvisionHandler;
    private final WorkloadTemplatesConfig workloadTemplatesConfig;
    private final OutputPortTemplatesConfig outputPortTemplatesConfig;

    public ReverseProvisionServiceImpl(
            WorkflowReverseProvisionHandler workflowReverseProvisionHandler,
            OutputPortReverseProvisionHandler outputPortReverseProvisionHandler,
            WorkloadTemplatesConfig workloadTemplatesConfig,
            OutputPortTemplatesConfig outputPortTemplatesConfig) {
        this.workflowReverseProvisionHandler = workflowReverseProvisionHandler;
        this.outputPortReverseProvisionHandler = outputPortReverseProvisionHandler;
        this.workloadTemplatesConfig = workloadTemplatesConfig;
        this.outputPortTemplatesConfig = outputPortTemplatesConfig;
    }

    @Override
    public ReverseProvisioningStatus runReverseProvisioning(ReverseProvisioningRequest reverseProvisioningRequest) {
        return startReverseProvisioning(reverseProvisioningRequest);
    }

    private ReverseProvisioningStatus startReverseProvisioning(ReverseProvisioningRequest reverseProvisioningRequest) {
        Object catalogInfoObj = reverseProvisioningRequest.getCatalogInfo();
        ObjectMapper objectMapper = new ObjectMapper();
        CatalogInfo catalogInfo = objectMapper.convertValue(catalogInfoObj, CatalogInfo.class);
        String componentName = catalogInfo.getSpec().getMesh().getName();

        String useCaseTemplateId = getUseCaseTemplateId(reverseProvisioningRequest.getUseCaseTemplateId());

        if (outputPortTemplatesConfig.getOutputport().contains(useCaseTemplateId))
            return outputPortReverseProvisionHandler.reverseProvision(reverseProvisioningRequest);
        else if (workloadTemplatesConfig.getWorkflow().contains(useCaseTemplateId)) {
            Either<FailedOperation, LinkedHashMap<Object, Object>> reverseProvisioningResult =
                    workflowReverseProvisionHandler.reverseProvision(reverseProvisioningRequest);

            if (reverseProvisioningResult.isLeft()) {
                String errorMessage = String.format(
                        "(%s) Reverse provisioning based on useCaseTemplateId %s failed. Details: %s",
                        componentName, useCaseTemplateId, buildErrorDetails(reverseProvisioningResult.getLeft()));
                logger.error(errorMessage);
                return handleReverseProvisioningStatusFailed(errorMessage);
            }

            LinkedHashMap<Object, Object> updates = reverseProvisioningResult.get();

            logger.info(String.format(
                    "(%s) Reverse provision based on useCaseTemplateId %s successfully completed.",
                    componentName, useCaseTemplateId));
            return handleReverseProvisioningStatusCompleted("Reverse provisioning successfully completed.", updates);
        }

        return handleReverseProvisioningStatusFailed(String.format(
                "The useCaseTemplateId '%s' of the component is not supported by this Tech Adapter",
                useCaseTemplateId));
    }

    private String buildErrorDetails(FailedOperation failure) {
        return failure.problems().stream().map(Problem::description).collect(Collectors.joining(", "));
    }

    private static String getUseCaseTemplateId(String useCaseTemplateIdFull) {
        String[] parts = useCaseTemplateIdFull.split(":");
        String[] useCaseTemplateIdParts = Arrays.copyOfRange(parts, 0, parts.length - 1);
        String useCaseTemplateId = String.join(":", useCaseTemplateIdParts);
        String cleanUseCaseTemplateId = useCaseTemplateId.replace("\"", "").trim();
        return cleanUseCaseTemplateId;
    }
}
