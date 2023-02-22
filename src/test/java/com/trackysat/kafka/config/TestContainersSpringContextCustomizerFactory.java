package com.trackysat.kafka.config;

import java.util.List;
import org.cassandraunit.CQLDataLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.DefaultSingletonBeanRegistry;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.test.context.ContextConfigurationAttributes;
import org.springframework.test.context.ContextCustomizer;
import org.springframework.test.context.ContextCustomizerFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.KafkaContainer;

public class TestContainersSpringContextCustomizerFactory implements ContextCustomizerFactory {

    private Logger log = LoggerFactory.getLogger(TestContainersSpringContextCustomizerFactory.class);

    private static RedisTestContainer redisBean;
    private static KafkaTestContainer kafkaBean;
    private static CassandraTestContainer cassandraBean;

    @Override
    public ContextCustomizer createContextCustomizer(Class<?> testClass, List<ContextConfigurationAttributes> configAttributes) {
        return (context, mergedConfig) -> {
            ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
            TestPropertyValues testValues = TestPropertyValues.empty();
            EmbeddedRedis redisAnnotation = AnnotatedElementUtils.findMergedAnnotation(testClass, EmbeddedRedis.class);
            if (null != redisAnnotation) {
                log.debug("detected the EmbeddedRedis annotation on class {}", testClass.getName());
                log.info("Warming up the redis database");
                if (null == redisBean) {
                    redisBean = beanFactory.createBean(RedisTestContainer.class);
                    beanFactory.registerSingleton(RedisTestContainer.class.getName(), redisBean);
                    // ((DefaultListableBeanFactory)beanFactory).registerDisposableBean(RedisTestContainer.class.getName(), redisBean);
                }
                testValues =
                    testValues.and(
                        "jhipster.cache.redis.server=redis://" +
                        redisBean.getRedisContainer().getContainerIpAddress() +
                        ":" +
                        redisBean.getRedisContainer().getMappedPort(6379)
                    );
            }
            EmbeddedCassandra cassandraAnnotation = AnnotatedElementUtils.findMergedAnnotation(testClass, EmbeddedCassandra.class);
            if (null != cassandraAnnotation) {
                log.debug("detected the EmbeddedCassandra annotation on class {}", testClass.getName());
                log.info("Warming up the cassandra database");
                if (null == cassandraBean) {
                    cassandraBean = beanFactory.createBean(CassandraTestContainer.class);
                    beanFactory.registerSingleton(CassandraTestContainer.class.getName(), cassandraBean);
                    // ((DefaultListableBeanFactory)beanFactory).registerDisposableBean(CassandraTestContainer.class.getName(), cassandraBean);
                }
                testValues =
                    testValues
                        .and(
                            "spring.data.cassandra.port=" + cassandraBean.getCassandraContainer().getMappedPort(CassandraContainer.CQL_PORT)
                        )
                        .and("spring.data.cassandra.contact-points=" + cassandraBean.getCassandraContainer().getHost())
                        .and("spring.data.cassandra.keyspace-name=" + CQLDataLoader.DEFAULT_KEYSPACE_NAME)
                        .and(
                            "spring.data.cassandra.local-datacenter=" +
                            cassandraBean.getCassandraContainer().getCluster().getMetadata().getAllHosts().iterator().next().getDatacenter()
                        )
                        .and(
                            "spring.data.cassandra.cluster-name=" +
                            cassandraBean.getCassandraContainer().getCluster().getMetadata().getClusterName()
                        );
            }
            EmbeddedKafka kafkaAnnotation = AnnotatedElementUtils.findMergedAnnotation(testClass, EmbeddedKafka.class);
            if (null != kafkaAnnotation) {
                log.debug("detected the EmbeddedKafka annotation on class {}", testClass.getName());
                log.info("Warming up the kafka broker");
                if (null == kafkaBean) {
                    kafkaBean = beanFactory.createBean(KafkaTestContainer.class);
                    beanFactory.registerSingleton(KafkaTestContainer.class.getName(), kafkaBean);
                    // ((DefaultListableBeanFactory)beanFactory).registerDisposableBean(KafkaTestContainer.class.getName(), kafkaBean);
                }
                testValues =
                    testValues.and(
                        "spring.cloud.stream.kafka.binder.brokers=" +
                        kafkaBean.getKafkaContainer().getHost() +
                        ':' +
                        kafkaBean.getKafkaContainer().getMappedPort(KafkaContainer.KAFKA_PORT)
                    );
            }
            testValues.applyTo(context);
        };
    }
}
