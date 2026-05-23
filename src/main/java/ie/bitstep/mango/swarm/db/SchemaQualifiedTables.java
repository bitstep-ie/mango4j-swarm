package ie.bitstep.mango.swarm.db;

import java.util.regex.Pattern;

/**
 * Resolves and validates schema-qualified table names for swarm SQL queries.
 */
public final class SchemaQualifiedTables {
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private final String tasksTable;
    private final String workersTable;
    private final String taskPacersTable;

    public SchemaQualifiedTables(String schema) {
        String prefix = "";
        if (schema != null && !schema.isBlank()) {
            String normalized = schema.trim();
            if (!IDENTIFIER.matcher(normalized).matches()) {
                throw new IllegalArgumentException("Invalid mango.swarm.database.schema: " + schema);
            }
            prefix = normalized + ".";
        }
        this.tasksTable = prefix + "mango_swarm_tasks";
        this.workersTable = prefix + "mango_swarm_workers";
        this.taskPacersTable = prefix + "mango_swarm_task_pacers";
    }

    public String tasks() {
        return tasksTable;
    }

    public String workers() {
        return workersTable;
    }

    public String taskPacers() {
        return taskPacersTable;
    }
}
