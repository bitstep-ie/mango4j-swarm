package ie.bitstep.mango.swarm.db;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaQualifiedTablesTest {

    @Test
    void leavesTablesUnqualifiedWhenNoSchemaConfigured() {
        SchemaQualifiedTables tables = new SchemaQualifiedTables(null);

        assertThat(tables.tasks()).isEqualTo("mango_swarm_tasks");
        assertThat(tables.workers()).isEqualTo("mango_swarm_workers");
    }

    @Test
    void qualifiesTablesWhenSchemaConfigured() {
        SchemaQualifiedTables tables = new SchemaQualifiedTables("application_schema");

        assertThat(tables.tasks()).isEqualTo("application_schema.mango_swarm_tasks");
        assertThat(tables.workers()).isEqualTo("application_schema.mango_swarm_workers");
    }

    @Test
    void rejectsUnsafeSchemaNames() {
        assertThatThrownBy(() -> new SchemaQualifiedTables("bad.schema"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid mango.swarm.database.schema");
    }
}
