package it.agilelab.witboost.provisioning.databricks.principalsmapping.databricks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
class DatabricksMapperTest {

    private DatabricksMapper mapper;
    private final Set<String> inputUserNoUnderscore = Set.of("user:namesurnameemail.com");
    private final Set<String> inputUserWithUnderscore =
            Set.of("user:name.surname_email.com", "user:john.doe_email.com");
    private final Set<String> inputGroup = Set.of("group:dev");
    private final Set<String> wrongIdentity = Set.of("wrong:id");
    private final Set<String> emptySet = Set.of();

    @BeforeEach
    void setUp() {
        mapper = new DatabricksMapper();
    }

    @Test
    void testMapWitboostUserIdentityToDatabricksIdentityWithoutUnderscore() {
        Map<String, Either<Throwable, String>> res = mapper.map(inputUserNoUnderscore);

        assertEquals(1, res.size());
        for (String key : inputUserNoUnderscore) {
            assertTrue(res.containsKey(key));
            Either<Throwable, String> either = res.get(key);
            assertTrue(either.isRight());
            String email = key.substring(5); // No underscore, result is the same as the input
            assertEquals(email, either.get());
        }
    }

    @Test
    void testMapWitboostUserIdentityToDatabricksIdentityWithUnderscore() {
        Map<String, Either<Throwable, String>> res = mapper.map(inputUserWithUnderscore);

        assertEquals(2, res.size());
        for (String key : inputUserWithUnderscore) {
            assertTrue(res.containsKey(key));
            Either<Throwable, String> either = res.get(key);
            assertTrue(either.isRight());
            String email = key.substring(5).replace('_', '@');
            assertEquals(email, either.get());
        }
    }

    @Test
    void testMapWitboostGroupIdentityToDatabricksGroups() {
        Map<String, Either<Throwable, String>> res = mapper.map(inputGroup);

        assertEquals(1, res.size());
        for (String key : inputGroup) {
            assertTrue(res.containsKey(key));
            Either<Throwable, String> either = res.get(key);
            assertTrue(either.isRight());
            String group = key.substring(6);
            assertEquals(group, either.get());
        }
    }

    @Test
    void testReturnLeftForWrongIdentity() {
        Map<String, Either<Throwable, String>> res = mapper.map(wrongIdentity);

        assertEquals(1, res.size());
        for (String key : wrongIdentity) {
            assertTrue(res.containsKey(key));
            Either<Throwable, String> either = res.get(key);
            assertTrue(either.isLeft());
            assertEquals(
                    "The subject " + key + " is neither a Witboost user nor a group",
                    either.getLeft().getMessage());
        }
    }

    @Test
    void testMapWithEmptySet() {
        Map<String, Either<Throwable, String>> res = mapper.map(emptySet);

        assertEquals(0, res.size());
    }
}
