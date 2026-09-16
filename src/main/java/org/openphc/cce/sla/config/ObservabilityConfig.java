package org.openphc.cce.sla.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.openphc.cce.sla.domain.repository.SlaTransitionFetchRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Metrics for the time plane.
 *
 * <p>The counters this service moves are registered where they are incremented
 * ({@code cce.sla.transitions.*}, {@code cce.sla.evaluator.*}). What
 * belongs here is the one thing only a gauge can express: how much due work is still outstanding.
 *
 * <p>{@code cce.sla.transitions.due} is the service's only gauge and its primary health signal. It
 * covers every verdict the service reaches, {@code MET} included — an on-time step arrives as a
 * {@code MET_CONDITION_REACHED} row, so it is backlog like any other and needs no gauge of its own. In a steady state it hovers
 * near zero; a rising value means transitions are falling due faster than they are being applied, or that
 * rows are failing and backing off. It counts what the next cycle would fetch — rows whose deadline has
 * passed, plus rows of steps already completed and so already judgeable — and nothing else: counting every
 * unprocessed row would fold in the whole future schedule and track enrolment volume instead.
 */
@Configuration
public class ObservabilityConfig {

    @Bean
    public MeterBinder slaMetrics(SlaTransitionFetchRepository transitionRepository) {
        return registry -> {
            Gauge.builder("cce.sla.transitions.due",
                            transitionRepository,
                            ObservabilityConfig::readyNow)
                    .description("SLA transition rows the next cycle would fetch: unprocessed, with "
                            + "next_attempt_at already passed")
                    .register(registry);
        };
    }

    /**
     * What the next cycle would fetch, carrying {@code fetchTransitions}'s predicate exactly. A gauge
     * over every unprocessed row would fold in the whole future schedule, so it would track enrolment
     * volume rather than lateness and could never sit near zero.
     */
    private static double readyNow(SlaTransitionFetchRepository repository) {
        return repository.countDueTransitions(OffsetDateTime.now(ZoneOffset.UTC));
    }
}
