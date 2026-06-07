package ie.bitstep.mango.swarm.payload;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PayloadReaderTest {
	private final ObjectMapper objectMapper = new ObjectMapper();

	@Test
	void extractsAliasesNestedPathsAndDefaults() throws Exception {
		PayloadReader reader = new PayloadReader(objectMapper.readTree(
				"""
				{
				"recipientEmail": "x@y.com",
				"userId": "123",
				"priority": 8
				}
				"""));

		EmailPayload payload = new EmailPayload(
				reader.required(String.class, "customerId", "userId", "customer.id"),
				reader.required(String.class, "email", "recipientEmail", "to.address"),
				reader.optional(String.class, "templateId", "template.id", "emailTemplate")
						.orDefault("default-template"),
				reader.optional(Integer.class, "priority").orDefault(5),
				reader.optional(Boolean.class, "trackOpens").orDefault(true));

		assertThat(payload).isEqualTo(new EmailPayload("123", "x@y.com", "default-template", 8, true));
	}

	@Test
	void staticExtractorConvenienceWrapsPayloadReader() throws Exception {
		String customerId = PayloadExtractor.extract(
				objectMapper.readTree("""
				{
				"customerId": "customer-1"
				}
				"""),
				reader -> reader.required(String.class, "customerId"));

		assertThat(customerId).isEqualTo("customer-1");
	}

	@Test
	void failsClearlyForMissingRequiredData() throws Exception {
		PayloadReader reader = new PayloadReader(objectMapper.readTree("{}"));

		assertThatThrownBy(() -> reader.required(String.class, "customerId", "userId"))
				.isInstanceOf(PayloadExtractionException.class)
				.hasMessageContaining("Missing required payload field")
				.hasMessageContaining("[customerId, userId]");
	}

	@Test
	void optionalValueCanBeInspectedValidatedAndRequired() throws Exception {
		PayloadReader reader = new PayloadReader(objectMapper.readTree("""
				{
				"priority": 8
				}
				"""));

		PayloadReader.OptionalValue<Integer> priority = reader.optional(Integer.class, "priority");
		PayloadReader.OptionalValue<String> template = reader.optional(String.class, "template");

		assertThat(priority.asOptional()).contains(8);
		assertThat(priority.validate(value -> value > 0, "must be positive")).isSameAs(priority);
		assertThat(priority.orElseThrow("priority required")).isEqualTo(8);
		assertThat(template.asOptional()).isEmpty();
		assertThat(template.orDefault("default")).isEqualTo("default");
		assertThatThrownBy(() -> template.orElseThrow("template required"))
				.isInstanceOf(PayloadExtractionException.class)
				.hasMessage("template required");
	}

	@Test
	void rejectsInvalidOptionalValueAndConversionFailures() throws Exception {
		PayloadReader invalid = new PayloadReader(
				objectMapper.readTree("""
				{
				"priority": -1,
				"count": "not-a-number"
				}
				"""));
		PayloadReader.OptionalValue<Integer> priority = invalid.optional(Integer.class, "priority");

		assertThatThrownBy(() -> priority.validate(value -> value > 0, "must be positive"))
				.isInstanceOf(PayloadExtractionException.class)
				.hasMessageContaining("Invalid payload field 'priority' as Integer: must be positive");

		assertThatThrownBy(() -> invalid.required(Integer.class, "count"))
				.isInstanceOf(PayloadExtractionException.class)
				.hasMessageContaining("Payload field 'count' cannot be converted to Integer");
	}

	private record EmailPayload(String customerId, String email, String templateId, int priority, boolean trackOpens) {}
}
