package com.trackysat.kafka;

import com.trackysat.kafka.DemoKafkaJHipsterApp;
import com.trackysat.kafka.config.AsyncSyncConfiguration;
import com.trackysat.kafka.config.EmbeddedCassandra;
import com.trackysat.kafka.config.EmbeddedKafka;
import com.trackysat.kafka.config.EmbeddedRedis;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

/**
 * Base composite annotation for integration tests.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest(classes = { DemoKafkaJHipsterApp.class, AsyncSyncConfiguration.class })
@EmbeddedRedis
@EmbeddedCassandra
@EmbeddedKafka
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public @interface IntegrationTest {
}
