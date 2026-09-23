package org.openphc.cce.sla;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationExcludeFilter;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.TypeExcludeFilter;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * CCE Step SLA Service — the time plane: the SLA transition evaluator.
 *
 * <p>Owns everything driven by <em>time passing</em>: it picks up the
 * {@code step_sla_state_transition} rows the Matcher Service scheduled, advances
 * {@code step_instance.sla_status}, and records the resulting {@code OVERDUE} / {@code MISSED}
 * deviations.
 *
 * <p>It does not match inbound events, enrol patients, create steps or manage definitions. Deviations
 * driven by an inbound event — {@code ORDER_VIOLATION} — stay with the Matcher Service, which detects
 * them at completion. The two services never write the same column.
 *
 * <p>{@code @EnableScheduling} drives {@link org.openphc.cce.sla.service.SlaTransitionEvaluator}.
 * Component scanning is widened to {@code org.openphc.cce} so the beans cce-common-util contributes are
 * found alongside this service's own — all but its {@code intelligence} and {@code kafka} packages.
 * Evaluating and publishing intelligence on a deviation is not part of this service yet, and those beans
 * would otherwise be created regardless: {@code IntelligenceActionEvaluator} needs
 * {@code IntelligenceTriggerProducer}, which needs a {@code KafkaTemplate}. Kafka auto-configuration is
 * excluded for the same reason: spring-kafka still reaches the classpath through cce-common-util.
 * {@code ApplicationContextTest} fails if either exclusion is lost.
 *
 * <p>Written as the three annotations {@code @SpringBootApplication} stands for, so the exclusion filters
 * sit in the one component scan rather than in a second one beside it.
 */
@SpringBootConfiguration
@EnableAutoConfiguration(exclude = KafkaAutoConfiguration.class)
@ComponentScan(basePackages = "org.openphc.cce", excludeFilters = {
        // The two filters @SpringBootApplication supplies, kept so nothing else changes.
        @ComponentScan.Filter(type = FilterType.CUSTOM, classes = TypeExcludeFilter.class),
        @ComponentScan.Filter(type = FilterType.CUSTOM, classes = AutoConfigurationExcludeFilter.class),
        @ComponentScan.Filter(type = FilterType.REGEX,
                pattern = "org\\.openphc\\.cce\\.common\\.(intelligence|kafka)\\..*")
})
// @Entity and @Repository types are not picked up by component scanning, so both are pointed at
// org.openphc.cce as well: the shared entities and repositories live in cce-common-util.
@EntityScan("org.openphc.cce")
@EnableJpaRepositories("org.openphc.cce")
@EnableScheduling
public class StepSlaServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(StepSlaServiceApplication.class, args);
    }
}
