package it.agilelab.witboost.provisioning.databricks.principalsmapping.azure;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.vavr.control.Either;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AzureMapperTest {

    private AzureClient client;
    private AzureMapper mapper;
    private final Set<String> inputUser = Set.of("user:name.surname_email.com");
    private final Set<String> inputGroup = Set.of("group:dev");
    private final Set<String> wrongIdentity = Set.of("wrong:id");

    @BeforeEach
    void setUp() {
        client = mock(AzureClient.class);
        mapper = new AzureMapper(client);
    }

    @Test
    void testMapWitboostUserIdentityToAzureObjectId() {
        String userId = UUID.randomUUID().toString();
        String mail = "name.surname@email.com";
        when(client.getUserId(mail)).thenReturn(Either.right(userId));

        Map<String, Either<Throwable, String>> res = mapper.map(inputUser);

        assertEquals(1, res.size());
        assertEquals(inputUser.iterator().next(), res.keySet().iterator().next());
        assertTrue(res.values().iterator().next().isRight());
        assertEquals(userId, res.values().iterator().next().get());
    }

    @Test
    void testMapWitboostGroupIdentityToAzureObjectId() {
        String groupId = UUID.randomUUID().toString();
        String group = "dev";
        when(client.getGroupId(group)).thenReturn(Either.right(groupId));

        Map<String, Either<Throwable, String>> res = mapper.map(inputGroup);

        assertEquals(1, res.size());
        assertEquals(inputGroup.iterator().next(), res.keySet().iterator().next());
        assertTrue(res.values().iterator().next().isRight());
        assertEquals(groupId, res.values().iterator().next().get());
    }

    @Test
    void testReturnLeftForWrongIdentity() {
        Map<String, Either<Throwable, String>> res = mapper.map(wrongIdentity);

        assertEquals(1, res.size());
        assertEquals(wrongIdentity.iterator().next(), res.keySet().iterator().next());
        assertTrue(res.values().iterator().next().isLeft());
        assertEquals(
                "The subject wrong:id is neither a Witboost user nor a group",
                res.values().iterator().next().getLeft().getMessage());
    }
}
