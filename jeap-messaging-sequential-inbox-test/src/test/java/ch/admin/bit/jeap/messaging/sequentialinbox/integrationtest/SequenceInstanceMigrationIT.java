package ch.admin.bit.jeap.messaging.sequentialinbox.integrationtest;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class SequenceInstanceMigrationIT {
    @Container
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");

    @Test
    void upgradePreservesHistoricalRowsAndSupportsOldBinaryInserts() {
        var dataSource = new DriverManagerDataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Flyway.configure().dataSource(dataSource).target("6").load().migrate();
        var jdbc = new JdbcTemplate(dataSource);
        insertLegacyRow(jdbc, "before-upgrade");

        Flyway.configure().dataSource(dataSource).load().migrate();
        insertLegacyRow(jdbc, "after-upgrade");

        assertThat(jdbc.queryForList("SELECT created_in_recording_mode FROM sequence_instance", Boolean.class))
                .containsExactly(false, false);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM sequence_instance WHERE state = 'OPEN'", Long.class)).isEqualTo(2);
    }

    private void insertLegacyRow(JdbcTemplate jdbc, String contextId) {
        jdbc.update("""
                INSERT INTO sequence_instance (id, name, context_id, state, created_at, retain_until)
                VALUES (nextval('sequence_instance_sequence'), 'legacy', ?, 'OPEN', NOW(), NOW() + INTERVAL '1 day')
                """, contextId);
    }
}
