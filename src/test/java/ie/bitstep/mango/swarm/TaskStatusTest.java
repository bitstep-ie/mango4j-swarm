package ie.bitstep.mango.swarm;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskStatusTest {
	@Test
	void mapsDatabaseValuesToStatuses() {
		assertThat(TaskStatus.fromDatabaseValue("queued")).isEqualTo(TaskStatus.QUEUED);
		assertThat(TaskStatus.fromDatabaseValue("claimed")).isEqualTo(TaskStatus.CLAIMED);
		assertThat(TaskStatus.fromDatabaseValue("in_progress")).isEqualTo(TaskStatus.IN_PROGRESS);
		assertThat(TaskStatus.fromDatabaseValue("completed")).isEqualTo(TaskStatus.COMPLETED);
		assertThat(TaskStatus.fromDatabaseValue("failed")).isEqualTo(TaskStatus.FAILED);
	}

	@Test
	void rejectsUnknownDatabaseValue() {
		assertThatThrownBy(() -> TaskStatus.fromDatabaseValue("unknown"))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Unknown task status: unknown");
	}
}
