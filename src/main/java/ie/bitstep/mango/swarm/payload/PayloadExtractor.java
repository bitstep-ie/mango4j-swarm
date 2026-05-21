package ie.bitstep.mango.swarm.payload;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Converts a durable JSON task payload into the current Java payload model.
 *
 * @param <T> extracted payload type
 */
@FunctionalInterface
public interface PayloadExtractor<T> {
    /**
     * Extracts and validates payload data from the provided reader.
     *
     * @param reader payload reader with required/optional lookup helpers
     * @return extracted payload object
     * @throws PayloadExtractionException when semantic required data cannot be derived
     */
    T extract(PayloadReader reader) throws PayloadExtractionException;

    /**
     * Convenience extractor for one-off use with a raw {@link JsonNode}.
     *
     * @param payload JSON payload root
     * @param extractor extraction function
     * @param <T> extracted payload type
     * @return extracted payload object
     */
    static <T> T extract(JsonNode payload, PayloadExtractor<T> extractor) {
        return extractor.extract(new PayloadReader(payload));
    }
}
