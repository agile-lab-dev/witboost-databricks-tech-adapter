package it.agilelab.witboost.provisioning.databricks.principalsmapping.databricks;

import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.Mapper;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.azure.AzureMapper;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksMapper implements Mapper {

    private static final Logger logger = LoggerFactory.getLogger(AzureMapper.class);
    private static final String USER_PREFIX = "user:";
    private static final String GROUP_PREFIX = "group:";

    @Override
    public Map<String, Either<Throwable, String>> map(Set<String> subjects) {
        return subjects.stream().collect(Collectors.toMap(ref -> ref, this::mapSubject));
    }

    private Either<Throwable, String> mapSubject(String ref) {
        if (ref.startsWith(USER_PREFIX)) {
            return getAndMapUser(ref.substring(USER_PREFIX.length()));
        } else if (ref.startsWith(GROUP_PREFIX)) {
            return Either.right(ref.substring(GROUP_PREFIX.length()));
        } else {
            String errorMessage = String.format("The subject %s is neither a Witboost user nor a group", ref);
            logger.error(errorMessage);
            return Either.left(new Throwable(errorMessage));
        }
    }

    protected Either<Throwable, String> getAndMapUser(String user) {
        try {
            int underscoreIndex = user.lastIndexOf('_');
            String mail = (underscoreIndex == -1)
                    ? user
                    : user.substring(0, underscoreIndex) + "@" + user.substring(underscoreIndex + 1);
            return Either.right(mail);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An unexpected error occurred while mapping the the Witboost user %s. Please try again later. If the issue still persists, contact the platform team for assistance! Details: %s",
                    user, e.getMessage());
            logger.error(errorMessage, e);
            return Either.left(new Throwable(errorMessage, e));
        }
    }
}
