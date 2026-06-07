package ie.bitstep.mango.swarm.payload;

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
}
