package ie.bitstep.mango.swarm.payload;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

public final class PayloadReader {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final JsonNode root;

    public PayloadReader(JsonNode root) {
        this.root = Objects.requireNonNull(root, "root");
    }

    public <T> T required(Class<T> type, String primaryPath, String... aliases) {
        Optional<T> value = readFirst(type, primaryPath, aliases);
        return value.orElseThrow(() -> new PayloadExtractionException(
                "Missing required payload field. Tried paths: " + allPaths(primaryPath, aliases)));
    }

    public <T> OptionalValue<T> optional(Class<T> type, String primaryPath, String... aliases) {
        return new OptionalValue<>(type, primaryPath, readFirst(type, primaryPath, aliases));
    }

    private <T> Optional<T> readFirst(Class<T> type, String primaryPath, String... aliases) {
        for (String path : concat(primaryPath, aliases)) {
            JsonNode node = find(path);
            if (node != null && !node.isNull() && !node.isMissingNode()) {
                try {
                    return Optional.of(OBJECT_MAPPER.convertValue(node, type));
                } catch (IllegalArgumentException ex) {
                    throw new PayloadExtractionException("Payload field '" + path + "' cannot be converted to "
                            + type.getSimpleName(), ex);
                }
            }
        }
        return Optional.empty();
    }

    private JsonNode find(String path) {
        JsonNode current = root;
        for (String part : path.split("\\.")) {
            if (current == null || current.isMissingNode() || current.isNull()) {
                return null;
            }
            current = current.path(part);
        }
        return current;
    }

    private static String[] concat(String primary, String[] aliases) {
        String[] paths = new String[aliases.length + 1];
        paths[0] = primary;
        System.arraycopy(aliases, 0, paths, 1, aliases.length);
        return paths;
    }

    private static String allPaths(String primary, String[] aliases) {
        return Arrays.toString(concat(primary, aliases));
    }

    public static final class OptionalValue<T> {
        private final Class<T> type;
        private final String path;
        private final Optional<T> value;

        private OptionalValue(Class<T> type, String path, Optional<T> value) {
            this.type = type;
            this.path = path;
            this.value = value;
        }

        public T orDefault(T defaultValue) {
            return value.orElse(defaultValue);
        }

        public Optional<T> asOptional() {
            return value;
        }

        public T orElseThrow(String message) {
            return value.orElseThrow(() -> new PayloadExtractionException(message));
        }

        public OptionalValue<T> validate(Predicate<T> predicate, String message) {
            value.ifPresent(v -> {
                if (!predicate.test(v)) {
                    throw new PayloadExtractionException("Invalid payload field '" + path + "' as "
                            + type.getSimpleName() + ": " + message);
                }
            });
            return this;
        }
    }
}
