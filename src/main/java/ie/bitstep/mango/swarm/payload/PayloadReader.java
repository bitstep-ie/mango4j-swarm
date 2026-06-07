package ie.bitstep.mango.swarm.payload;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Helper for extracting evolved payload shapes from durable JSON task payloads.
 *
 * <p>Supports aliases, defaults, nested paths, validation and clear extraction errors.
 */
public final class PayloadReader {
	private static final ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper();

	private final JsonNode root;
	private final ObjectMapper objectMapper;

	public PayloadReader(JsonNode root) {
		this(root, DEFAULT_OBJECT_MAPPER);
	}

	public PayloadReader(JsonNode root, ObjectMapper objectMapper) {
		this.root = Objects.requireNonNull(root, "root");
		this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
	}

	/**
	 * Reads a required value from the first matching path.
	 *
	 * @param type target Java type
	 * @param primaryPath primary field path
	 * @param aliases legacy/alternate field paths
	 * @param <T> target type
	 * @return converted value
	 */
	public <T> T required(Class<T> type, String primaryPath, String... aliases) {
		Optional<T> value = readFirst(type, primaryPath, aliases);
		return value.orElseThrow(() -> new PayloadExtractionException(
				"Missing required payload field. Tried paths: " + allPaths(primaryPath, aliases)));
	}

	/**
	 * Reads an optional value from the first matching path.
	 *
	 * @param type target Java type
	 * @param primaryPath primary field path
	 * @param aliases legacy/alternate field paths
	 * @param <T> target type
	 * @return optional value wrapper with default/validation helpers
	 */
	public <T> OptionalValue<T> optional(Class<T> type, String primaryPath, String... aliases) {
		return new OptionalValue<>(type, primaryPath, readFirst(type, primaryPath, aliases));
	}

	private <T> Optional<T> readFirst(Class<T> type, String primaryPath, String... aliases) {
		for (String path : concat(primaryPath, aliases)) {
			JsonNode node = find(path);
			if (node != null && !node.isNull() && !node.isMissingNode()) {
				try {
					return Optional.of(objectMapper.convertValue(node, type));
				} catch (IllegalArgumentException ex) {
					throw new PayloadExtractionException(
							"Payload field '" + path + "' cannot be converted to " + type.getSimpleName(), ex);
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

	/** Optional payload value wrapper with validation and fallback helpers. */
	public static final class OptionalValue<T> {
		private final Class<T> type;
		private final String path;
		private final Optional<T> value;

		private OptionalValue(Class<T> type, String path, Optional<T> value) {
			this.type = type;
			this.path = path;
			this.value = value;
		}

		/** @return wrapped value or provided default when absent */
		public T orDefault(T defaultValue) {
			return value.orElse(defaultValue);
		}

		/** @return raw optional value */
		public Optional<T> asOptional() {
			return value;
		}

		/** @return value or throws extraction exception with the provided message */
		public T orElseThrow(String message) {
			return value.orElseThrow(() -> new PayloadExtractionException(message));
		}

		/**
		 * Validates the value when present.
		 *
		 * @param predicate validation predicate
		 * @param message validation error message
		 * @return this wrapper for fluent chaining
		 */
		public OptionalValue<T> validate(Predicate<T> predicate, String message) {
			value.ifPresent(v -> {
				if (!predicate.test(v)) {
					throw new PayloadExtractionException(
							"Invalid payload field '" + path + "' as " + type.getSimpleName() + ": " + message);
				}
			});
			return this;
		}
	}
}
