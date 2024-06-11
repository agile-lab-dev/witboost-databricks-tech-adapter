package it.agilelab.witboost.provisioning.databricks.model.databricks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import it.agilelab.witboost.provisioning.databricks.TestConfig;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class SchedulingSpecificTest {

    @Autowired
    private Validator validator;

    private SchedulingSpecific schedulingSpecific;

    @BeforeEach
    public void setUp() {
        schedulingSpecific = new SchedulingSpecific();
    }

    @Test
    public void testDefaultValues() {
        assertNotNull(schedulingSpecific);
        assertNull(schedulingSpecific.getCronExpression());
        assertNull(schedulingSpecific.getJavaTimezoneId());
    }

    @Test
    public void testGettersAndSetters() {
        // Test for cronExpression
        String cronExpression = "0 0/5 * * * ?";
        schedulingSpecific.setCronExpression(cronExpression);
        assertEquals(cronExpression, schedulingSpecific.getCronExpression());

        // Test for javaTimezoneId
        String timezoneId = "Europe/Rome";
        schedulingSpecific.setJavaTimezoneId(timezoneId);
        assertEquals(timezoneId, schedulingSpecific.getJavaTimezoneId());
    }

    @Test
    public void testEnableSchedulingNotNullWithValidValue() {
        // Test with a valid value to ensure no violations are found
        Set<ConstraintViolation<SchedulingSpecific>> violations = validator.validate(schedulingSpecific);
        assertEquals(0, violations.size());
    }
}
