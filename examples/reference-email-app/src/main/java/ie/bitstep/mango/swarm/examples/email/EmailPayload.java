package ie.bitstep.mango.examples.email;

public record EmailPayload(
        String customerId,
        String to,
        String subject,
        String body
) {
}
