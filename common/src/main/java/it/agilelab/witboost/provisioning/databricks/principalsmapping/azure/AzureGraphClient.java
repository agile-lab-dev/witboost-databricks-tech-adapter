package it.agilelab.witboost.provisioning.databricks.principalsmapping.azure;

import com.microsoft.graph.serviceclient.GraphServiceClient;
import io.vavr.control.Either;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureGraphClient implements AzureClient {

    private static final Logger logger = LoggerFactory.getLogger(AzureGraphClient.class);

    private final GraphServiceClient graphServiceClient;

    public AzureGraphClient(GraphServiceClient graphServiceClient) {
        this.graphServiceClient = graphServiceClient;
    }

    @Override
    public Either<Throwable, String> getUserId(String mail) {
        return Try.of(
                        () -> graphServiceClient
                                .users()
                                .get(r -> r.queryParameters.filter = String.format("mail eq '%s'", mail))
                                .getValue()
                                .stream()
                                .findFirst())
                .toEither()
                .flatMap(opt -> opt.map(user -> Either.<Throwable, String>right(user.getId()))
                        .orElseGet(() -> {
                            String errorMessage =
                                    String.format("User %s not found on the configured Azure tenant", mail);
                            logger.error(errorMessage);
                            return Either.left(new Throwable(errorMessage));
                        }));
    }

    @Override
    public Either<Throwable, String> getGroupId(String group) {
        return Try.of(() -> graphServiceClient
                        .groups()
                        .get(r -> r.queryParameters.filter = String.format("displayName eq '%s'", group))
                        .getValue()
                        .stream()
                        .findFirst())
                .toEither()
                .flatMap(opt -> opt.map(grp -> Either.<Throwable, String>right(grp.getId()))
                        .orElseGet(() -> {
                            String errorMessage =
                                    String.format("Group %s not found on the configured Azure tenant", group);
                            logger.error(errorMessage);
                            return Either.left(new Throwable(errorMessage));
                        }));
    }
}
