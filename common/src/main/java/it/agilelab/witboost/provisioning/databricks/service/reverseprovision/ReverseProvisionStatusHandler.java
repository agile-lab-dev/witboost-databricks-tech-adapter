package it.agilelab.witboost.provisioning.databricks.service.reverseprovision;

import it.agilelab.witboost.provisioning.databricks.openapi.model.Log;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningStatus;
import java.time.OffsetDateTime;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReverseProvisionStatusHandler {

    private static final Logger logger = LoggerFactory.getLogger(ReverseProvisionStatusHandler.class);

    public static ReverseProvisioningStatus handleReverseProvisioningStatusFailed(String message) {
        logger.error(message);
        ReverseProvisioningStatus reverseProvisioningStatus =
                new ReverseProvisioningStatus(ReverseProvisioningStatus.StatusEnum.FAILED, null);
        reverseProvisioningStatus.addLogsItem(new Log(OffsetDateTime.now(), Log.LevelEnum.ERROR, message));
        return reverseProvisioningStatus;
    }

    public static ReverseProvisioningStatus handleReverseProvisioningStatusCompleted(
            String message, HashMap<Object, Object> updates) {
        logger.info(message);
        ReverseProvisioningStatus reverseProvisioningStatus =
                new ReverseProvisioningStatus(ReverseProvisioningStatus.StatusEnum.COMPLETED, updates);
        reverseProvisioningStatus.addLogsItem(
                new Log(OffsetDateTime.now(), Log.LevelEnum.INFO, "Reverse provisioning successfully completed."));
        return reverseProvisioningStatus;
    }
}
