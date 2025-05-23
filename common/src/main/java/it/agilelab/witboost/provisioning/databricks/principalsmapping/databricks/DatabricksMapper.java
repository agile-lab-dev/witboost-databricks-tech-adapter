package it.agilelab.witboost.provisioning.databricks.principalsmapping.databricks;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.AccountClient;
import com.databricks.sdk.service.iam.Group;
import com.databricks.sdk.service.iam.ListAccountGroupsRequest;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.Mapper;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.azure.AzureMapper;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksMapper implements Mapper {

    private static final Logger logger = LoggerFactory.getLogger(AzureMapper.class);
    private static final String USER_PREFIX = "user:";
    private static final String GROUP_PREFIX = "group:";

    private final AccountClient accountClient;

    public DatabricksMapper(AccountClient accountClient) {
        this.accountClient = accountClient;
    }

    public Either<Throwable, String> retrieveCaseSensitiveGroupDisplayName(String groupNameCaseInsensitive) {
        // This method is needed as in MEID group are case-insensitive (so 'group-A' is the same as 'group-a'),
        // instead Databricks interprets these as two different groups.
        // So, from the group that is in the request we have to retrieve the exact Display Name in order to interact
        // with Databricks sdk

        Iterable<Group> groupsAccountCaseInsensitive = accountClient
                .groups()
                .list(new ListAccountGroupsRequest()
                        .setFilter(String.format("displayName eq '%s'", groupNameCaseInsensitive)));

        List<Group> groupsAccountCaseInsensitiveList = StreamSupport.stream(
                        groupsAccountCaseInsensitive.spliterator(), false)
                .toList();

        if (groupsAccountCaseInsensitiveList.isEmpty()) {
            String errorMessage =
                    String.format("Group '%s' not found at Databricks account level.", groupNameCaseInsensitive);
            logger.error(errorMessage);
            return left(new Throwable(errorMessage));
        } else if (groupsAccountCaseInsensitiveList.size() > 1) {
            String errorMessage =
                    String.format("More than one group with name '%s' has been found", groupNameCaseInsensitive);
            logger.error(errorMessage);
            return left(new Throwable(errorMessage));
        } else {
            String groupNameCaseSensitive =
                    groupsAccountCaseInsensitiveList.get(0).getDisplayName();
            logger.info(String.format("Group found: correct displayName of the group is '%s'", groupNameCaseSensitive));
            return right(groupNameCaseSensitive);
        }
    }

    @Override
    public Map<String, Either<Throwable, String>> map(Set<String> subjects) {
        return subjects.stream().collect(Collectors.toMap(ref -> ref, this::mapSubject));
    }

    private Either<Throwable, String> mapSubject(String ref) {
        if (ref.startsWith(USER_PREFIX)) {
            return getAndMapUser(ref.substring(USER_PREFIX.length()));
        } else if (ref.startsWith(GROUP_PREFIX)) {
            return retrieveCaseSensitiveGroupDisplayName(ref.substring(GROUP_PREFIX.length()));
        } else {
            String errorMessage = String.format("The subject %s is neither a Witboost user nor a group", ref);
            logger.error(errorMessage);
            return left(new Throwable(errorMessage));
        }
    }

    protected Either<Throwable, String> getAndMapUser(String user) {
        try {
            int underscoreIndex = user.lastIndexOf('_');
            String mail = (underscoreIndex == -1)
                    ? user
                    : user.substring(0, underscoreIndex) + "@" + user.substring(underscoreIndex + 1);
            return right(mail);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An unexpected error occurred while mapping the the Witboost user %s. Please try again later. If the issue still persists, contact the platform team for assistance! Details: %s",
                    user, e.getMessage());
            logger.error(errorMessage, e);
            return left(new Throwable(errorMessage, e));
        }
    }
}
