package com.teads.summerschool.config;

import org.springframework.boot.hibernate.autoconfigure.HibernatePropertiesCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * Points Hibernate at this bidder's isolated schema so every table it maps is
 * qualified with {@code bidder_<id>}. The schema itself is created by
 * {@link SchemaInitializer} before the EntityManagerFactory is built.
 */
@Configuration
public class DatabaseConfig {

    @Bean
    public HibernatePropertiesCustomizer schemaCustomizer(Environment env) {
        String schema = SchemaInitializer.schemaFor(env.getProperty("bidder.id", "teads-bidder"));
        return props -> props.put("hibernate.default_schema", schema);
    }
}
