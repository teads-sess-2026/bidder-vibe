package com.teads.summerschool;

import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

// Override only the datasource with in-memory H2 so the context loads without a
// live Postgres; all other config still comes from the main application.properties.
@SpringBootTest
@TestPropertySource(properties = {
    "spring.datasource.url=jdbc:h2:mem:bidderdb;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE",
    "spring.datasource.driver-class-name=org.h2.Driver",
    "spring.datasource.username=sa",
    "spring.datasource.password=",
    "spring.jpa.database-platform=org.hibernate.dialect.H2Dialect",
    "spring.jpa.hibernate.ddl-auto=create-drop",
})
class SummerschoolApplicationTests {

    @MockitoBean(answers = Answers.RETURNS_DEEP_STUBS)
    StringRedisTemplate stringRedisTemplate;

    @Test
    void contextLoads() {
    }

}
