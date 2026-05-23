package ie.bitstep.mango.swarm.payload;

/**
 * Signals semantic payload extraction failures from durable JSON payloads.
 */
public class PayloadExtractionException extends RuntimeException {
    public PayloadExtractionException(String message) {
        super(message);
    }

    public PayloadExtractionException(String message, Throwable cause) {
        super(message, cause);
    }
}
