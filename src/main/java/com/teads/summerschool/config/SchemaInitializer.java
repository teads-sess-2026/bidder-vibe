package com.teads.summerschool.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

/**
 * Ensures each bidder's isolated Postgres schema exists before Hibernate runs DDL.
 *
 * <p>The schema name is {@code "bidder_" + sanitized bidder id} (e.g. {@code team-alpha}
 * → {@code bidder_team_alpha}). This runs as a {@link BeanPostProcessor} on the
 * {@code DataSource} bean, which Spring fully initializes before the
 * EntityManagerFactory — so the schema exists by the time Hibernate creates its tables.
 */
@Component
public class SchemaInitializer implements BeanPostProcessor {

    private static final Logger log = LoggerFactory.getLogger(SchemaInitializer.class);

    private final Environment env;

    public SchemaInitializer(Environment env) {
        this.env = env;
    }

    /** Derive a safe Postgres schema name from a bidder id. Only [a-z0-9_] survives. */
    public static String schemaFor(String bidderId) {
        return "bidder_" + bidderId.toLowerCase().replaceAll("[^a-z0-9]+", "_");
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        if (bean instanceof DataSource dataSource) {
            String schema = schemaFor(env.getProperty("bidder.id", "teads-bidder"));
            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement()) {
                // schema is sanitized to [a-z0-9_], so string concatenation is safe here
                stmt.execute("CREATE SCHEMA IF NOT EXISTS " + schema);
                log.info("Ensured database schema exists: {}", schema);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to create schema " + schema, e);
            }
        }
        return bean;
    }
}
