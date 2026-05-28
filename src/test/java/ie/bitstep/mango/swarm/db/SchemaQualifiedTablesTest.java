package ie.bitstep.mango.swarm.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SchemaQualifiedTablesTest {

	@Test
	void leavesTablesUnqualifiedWhenNoSchemaConfigured() {
		SchemaQualifiedTables tables = new SchemaQualifiedTables(null);

		assertThat(tables.schema()).isNull();
		assertThat(tables.tasks()).isEqualTo("mango_swarm_tasks");
		assertThat(tables.workers()).isEqualTo("mango_swarm_workers");
	}

	@Test
	void validatesSchemaButLeavesSqlTableNamesUnqualified() {
		SchemaQualifiedTables tables = new SchemaQualifiedTables("application_schema");

		assertThat(tables.schema()).isEqualTo("application_schema");
		assertThat(tables.tasks()).isEqualTo("mango_swarm_tasks");
		assertThat(tables.workers()).isEqualTo("mango_swarm_workers");
	}

	@Test
	void rejectsUnsafeSchemaNames() {
		assertThatThrownBy(() -> new SchemaQualifiedTables("bad.schema"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("Invalid mango4j.swarm.database.schema");
	}

	@Test
	void withSearchPathSkipsSchemaWorkWhenNoSchemaConfigured() throws Exception {
		Connection connection = mock(Connection.class);
		SchemaQualifiedTables tables = new SchemaQualifiedTables(null);

		String result = tables.withSearchPath(connection, ignored -> "done");

		assertThat(result).isEqualTo("done");
		verify(connection, never()).prepareStatement("SELECT set_config('search_path', ?, true)");
		verify(connection, never()).commit();
	}

	@Test
	void withSearchPathAppliesSchemaAndCommitsAutoCommitConnection() throws Exception {
		Connection connection = mock(Connection.class);
		PreparedStatement statement = mock(PreparedStatement.class);
		when(connection.getAutoCommit()).thenReturn(true);
		when(connection.prepareStatement("SELECT set_config('search_path', ?, true)"))
				.thenReturn(statement);
		SchemaQualifiedTables tables = new SchemaQualifiedTables("application_schema");

		String result = tables.withSearchPath(connection, ignored -> "done");

		assertThat(result).isEqualTo("done");
		var inOrder = inOrder(connection, statement);
		inOrder.verify(connection).setAutoCommit(false);
		inOrder.verify(connection).prepareStatement("SELECT set_config('search_path', ?, true)");
		inOrder.verify(statement).setString(1, "application_schema");
		inOrder.verify(statement).execute();
		inOrder.verify(connection).commit();
		inOrder.verify(connection).setAutoCommit(true);
	}

	@Test
	void withSearchPathRollsBackAutoCommitConnectionOnFailure() throws Exception {
		Connection connection = mock(Connection.class);
		PreparedStatement statement = mock(PreparedStatement.class);
		when(connection.getAutoCommit()).thenReturn(true);
		when(connection.prepareStatement("SELECT set_config('search_path', ?, true)"))
				.thenReturn(statement);
		SchemaQualifiedTables tables = new SchemaQualifiedTables("application_schema");

		assertThatThrownBy(() -> tables.withSearchPath(connection, ignored -> {
					throw new SQLException("failed");
				}))
				.isInstanceOf(SQLException.class)
				.hasMessage("failed");

		verify(connection).rollback();
		verify(connection).setAutoCommit(true);
	}

	@Test
	void withSearchPathDoesNotManageExistingTransactionBoundaries() throws Exception {
		Connection connection = mock(Connection.class);
		PreparedStatement statement = mock(PreparedStatement.class);
		when(connection.getAutoCommit()).thenReturn(false);
		when(connection.prepareStatement("SELECT set_config('search_path', ?, true)"))
				.thenReturn(statement);
		SchemaQualifiedTables tables = new SchemaQualifiedTables("application_schema");

		String result = tables.withSearchPath(connection, ignored -> "done");

		assertThat(result).isEqualTo("done");
		verify(connection, never()).setAutoCommit(false);
		verify(connection, never()).commit();
		verify(connection, never()).rollback();
		verify(statement).setString(1, "application_schema");
	}
}
