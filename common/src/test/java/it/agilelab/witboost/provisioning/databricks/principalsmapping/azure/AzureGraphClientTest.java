package it.agilelab.witboost.provisioning.databricks.principalsmapping.azure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.User;
import com.microsoft.graph.serviceclient.GraphServiceClient;
import com.microsoft.kiota.ApiException;
import io.vavr.control.Either;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class AzureGraphClientTest {

    private GraphServiceClient graphServiceClient;
    private AzureGraphClient azureClient;
    private User azureUser;
    private Group azureGroup;

    @BeforeEach
    void setUp() {
        graphServiceClient = Mockito.mock(GraphServiceClient.class, Mockito.RETURNS_DEEP_STUBS);
        azureClient = new AzureGraphClient(graphServiceClient);
        azureUser = Mockito.mock(User.class);
        azureGroup = Mockito.mock(Group.class);
    }

    @Test
    void testMapMailToAzureObjectId() {
        String mail = "name.surname@email.com";
        String userId = UUID.randomUUID().toString();
        when(azureUser.getId()).thenReturn(userId);
        when(graphServiceClient.users().get(any(Consumer.class)).getValue())
                .thenReturn(Collections.singletonList(azureUser));

        Either<Throwable, String> res = azureClient.getUserId(mail);

        assertTrue(res.isRight());
        assertEquals(userId, res.get());
    }

    @Test
    void testUserNotFound() {
        String mail = "not.existing@email.com";
        when(graphServiceClient.users().get(any(Consumer.class)).getValue()).thenReturn(Collections.emptyList());
        String expectedError = String.format("User %s not found on the configured Azure tenant", mail);

        Either<Throwable, String> res = azureClient.getUserId(mail);

        assertTrue(res.isLeft());
        assertEquals(expectedError, res.getLeft().getMessage());
    }

    @Test
    void testExceptionWhileSearchingForUsers() {
        String mail = "name.surname@email.com";
        String error = "Unexpected error";
        ApiException expectedException = new ApiException(error);
        when(graphServiceClient.users().get(any(Consumer.class)).getValue()).thenThrow(expectedException);

        Either<Throwable, String> res = azureClient.getUserId(mail);

        assertTrue(res.isLeft());
        assertEquals(expectedException, res.getLeft());
    }

    @Test
    void testMapGroupToAzureObjectId() {
        String group = "dev";
        String groupId = UUID.randomUUID().toString();
        when(azureGroup.getId()).thenReturn(groupId);
        when(graphServiceClient.groups().get(any(Consumer.class)).getValue())
                .thenReturn(Collections.singletonList(azureGroup));

        Either<Throwable, String> res = azureClient.getGroupId(group);

        assertTrue(res.isRight());
        assertEquals(groupId, res.get());
    }

    @Test
    void testGroupNotFound() {
        String group = "notexisting";
        when(graphServiceClient.groups().get(any(Consumer.class)).getValue()).thenReturn(Collections.emptyList());
        String expectedError = String.format("Group %s not found on the configured Azure tenant", group);

        Either<Throwable, String> res = azureClient.getGroupId(group);

        assertTrue(res.isLeft());
        assertEquals(expectedError, res.getLeft().getMessage());
    }

    @Test
    void testExceptionWhileSearchingForGroups() {
        String group = "dev";
        String error = "Unexpected error";
        ApiException expectedException = new ApiException(error);
        when(graphServiceClient.groups().get(any(Consumer.class)).getValue()).thenThrow(expectedException);

        Either<Throwable, String> res = azureClient.getGroupId(group);

        assertTrue(res.isLeft());
        assertEquals(expectedException, res.getLeft());
    }
}
