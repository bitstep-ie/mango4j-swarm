package ie.bitstep.mango.swarm.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.regex.Pattern;

/** Validates and applies the optional schema used by static swarm SQL queries. */
public final class SchemaQualifiedTables {
	private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

	private final String schema;

	public SchemaQualifiedTables(String schema) {
		if (schema != null && !schema.isBlank()) {
			String normalized = schema.trim();
			if (!IDENTIFIER.matcher(normalized).matches()) {
				throw new IllegalArgumentException("Invalid mango4j.swarm.database.schema: " + schema);
			}
			this.schema = normalized;
		} else {
			this.schema = null;
		}
	}

	public String schema() {
		return schema;
	}

	public <T> T withSearchPath(Connection connection, SqlWork<T> work) throws SQLException {
		if (schema == null) {
			return work.execute(connection);
		}
		boolean autoCommit = connection.getAutoCommit();
		if (autoCommit) {
			connection.setAutoCommit(false);
		}
		try {
			applySearchPath(connection);
			T result = work.execute(connection);
			if (autoCommit) {
				connection.commit();
			}
			return result;
		} catch (SQLException | RuntimeException ex) {
			if (autoCommit) {
				connection.rollback();
			}
			throw ex;
		} finally {
			if (autoCommit) {
				connection.setAutoCommit(true);
			}
		}
	}

	private void applySearchPath(Connection connection) throws SQLException {
		try (PreparedStatement statement = connection.prepareStatement("SELECT set_config('search_path', ?, true)")) {
			statement.setString(1, schema);
			statement.execute();
		}
	}

	@FunctionalInterface
	public interface SqlWork<T> {
		T execute(Connection connection) throws SQLException;
	}
}
