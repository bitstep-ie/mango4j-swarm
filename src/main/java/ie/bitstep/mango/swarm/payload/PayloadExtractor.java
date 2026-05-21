package ie.bitstep.mango.swarm.payload;

import com.fasterxml.jackson.databind.JsonNode;

@FunctionalInterface
public interface PayloadExtractor<T> {
    T extract(PayloadReader reader) throws PayloadExtractionException;

    static <T> T extract(JsonNode payload, PayloadExtractor<T> extractor) {
        return extractor.extract(new PayloadReader(payload));
    }
}
