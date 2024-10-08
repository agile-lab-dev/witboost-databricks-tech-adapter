package it.agilelab.witboost.provisioning.databricks.principalsmapping.azure;

import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.Mapper;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class AzureMapper implements Mapper {

    private static final Logger logger = LoggerFactory.getLogger(AzureMapper.class);
    private static final String USER_PREFIX = "user:";
    private static final String GROUP_PREFIX = "group:";
    private final AzureClient client;

    @Autowired
    public AzureMapper(AzureClient client) {
        this.client = client;
    }

    @Override
    public Map<String, Either<Throwable, String>> map(Set<String> subjects) {
        return subjects.stream().collect(Collectors.toMap(ref -> ref, this::mapSubject));
    }

    private Either<Throwable, String> mapSubject(String ref) {
        if (ref.startsWith(USER_PREFIX)) {
            return getAndMapUser(ref.substring(USER_PREFIX.length()));
        } else if (ref.startsWith(GROUP_PREFIX)) {
            return getAndMapGroup(ref.substring(GROUP_PREFIX.length()));
        } else {
            String errorMessage = String.format("The subject %s is neither a Witboost user nor a group", ref);
            logger.error(errorMessage);
            return Either.left(new Throwable(errorMessage));
        }
    }

    private Either<Throwable, String> getAndMapUser(String user) {
        int underscoreIndex = user.lastIndexOf('_');
        String mail = (underscoreIndex == -1)
                ? user
                : user.substring(0, underscoreIndex) + "@" + user.substring(underscoreIndex + 1);
        return client.getUserId(mail);
    }

    private Either<Throwable, String> getAndMapGroup(String group) {
        return client.getGroupId(group);
    }
}
