package ie.bitstep.mango.swarm.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.regex.Pattern;

/** Validates and applies the optional schema used by static swarm SQL queries. */
public record SchemaQualifiedTables(String schema) {
	private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_]\\w*");
	private static final int MAX_IDENTIFIER_LENGTH = 63;

	public SchemaQualifiedTables(String schema) {
		if (schema != null && !schema.isBlank()) {
			String normalized = schema.trim();
			if (normalized.length() > MAX_IDENTIFIER_LENGTH) {
				throw new IllegalArgumentException("Invalid mango4j.swarm.database.schema: " + schema);
			}
			if (!IDENTIFIER.matcher(normalized).matches()) {
				throw new IllegalArgumentException("Invalid mango4j.swarm.database.schema: " + schema);
			}
			this.schema = normalized;
		} else {
			this.schema = null;
		}
	}

	public <T> T withSearchPath(Connection connection, SqlWork<T> work) throws SQLException {
		boolean manageTransaction = connection.getAutoCommit();
		if (manageTransaction) {
			connection.setAutoCommit(false);
		}
		try {
			if (schema != null) {
				applySearchPath(connection);
			}
			T result = work.execute(connection);
			if (manageTransaction) {
				connection.commit();
			}
			return result;
		} catch (SQLException | RuntimeException ex) {
			if (manageTransaction) {
				connection.rollback();
			}
			throw ex;
		} finally {
			if (manageTransaction) {
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
