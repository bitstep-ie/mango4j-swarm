package ie.bitstep.mango.swarm.payload;

public class PayloadExtractionException extends RuntimeException {
    public PayloadExtractionException(String message) {
        super(message);
    }

    public PayloadExtractionException(String message, Throwable cause) {
        super(message, cause);
    }
}
