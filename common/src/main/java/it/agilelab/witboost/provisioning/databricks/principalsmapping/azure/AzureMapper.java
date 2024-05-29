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

    private final AzureClient client;

    @Autowired
    public AzureMapper(AzureClient client) {
        this.client = client;
    }

    @Override
    public Map<String, Either<Throwable, String>> map(Set<String> subjects) {
        return subjects.stream().collect(Collectors.toMap(ref -> ref, ref -> {
            if (ref.startsWith("user:")) {
                return getAndMapUser(ref.substring(5));
            } else if (ref.startsWith("group:")) {
                return getAndMapGroup(ref.substring(6));
            } else {
                String errorMessage = String.format("The subject %s is neither a Witboost user nor a group", ref);
                logger.error(errorMessage);
                return Either.left(new Throwable(errorMessage));
            }
        }));
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
