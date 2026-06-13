package ie.bitstep.mango.swarm.examples.email;

public record EmailPayload(
        String customerId,
        String to,
        String subject,
        String body
) {
}
