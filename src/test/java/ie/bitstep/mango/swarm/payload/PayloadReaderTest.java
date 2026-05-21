package ie.bitstep.mango.swarm.payload;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PayloadReaderTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void extractsAliasesNestedPathsAndDefaults() throws Exception {
        PayloadReader reader = new PayloadReader(objectMapper.readTree("""
                {
                  "recipientEmail": "x@y.com",
                  "userId": "123",
                  "priority": 8
                }
                """));

        EmailPayload payload = new EmailPayload(
                reader.required(String.class, "customerId", "userId", "customer.id"),
                reader.required(String.class, "email", "recipientEmail", "to.address"),
                reader.optional(String.class, "templateId", "template.id", "emailTemplate").orDefault("default-template"),
                reader.optional(Integer.class, "priority").orDefault(5),
                reader.optional(Boolean.class, "trackOpens").orDefault(true));

        assertThat(payload).isEqualTo(new EmailPayload("123", "x@y.com", "default-template", 8, true));
    }

    @Test
    void failsClearlyForMissingRequiredData() throws Exception {
        PayloadReader reader = new PayloadReader(objectMapper.readTree("{}"));

        assertThatThrownBy(() -> reader.required(String.class, "customerId", "userId"))
                .isInstanceOf(PayloadExtractionException.class)
                .hasMessageContaining("Missing required payload field");
    }

    private record EmailPayload(String customerId, String email, String templateId, int priority, boolean trackOpens) {
    }
}
