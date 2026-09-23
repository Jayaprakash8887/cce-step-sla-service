package org.openphc.cce.sla.domain.repository;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.openphc.cce.common.entity.StepSlaStateTransition;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * The evaluator's fetch path over {@code step_sla_state_transition}.
 *
 * <p>Deliberately separate from cce-common-util's {@code StepSlaStateTransitionRepository}, which is the
 * shared read side (a step's thresholds, needed by both services). Fetching rows for processing is this
 * service's alone, so the query that does it — and the pessimistic lock it takes — lives here rather than
 * somewhere the Matcher Service could reach for it.
 *
 * <p>{@code next_attempt_at} passing is what makes a row eligible; {@code process_by} is joined in as a
 * redundant, index-friendly bound (see {@link #fetchTransitions}), and {@code step_instance} is fetched
 * with each row, which also brings it into the same lock.
 */
@Repository
public interface SlaTransitionFetchRepository extends JpaRepository<StepSlaStateTransition, UUID> {

    /**
     * Fetch a batch of transitions that have come round: a deadline that has passed, or a step recorded
     * on time whose {@code MET} is waiting to be written.
     *
     * <p>{@code FOR UPDATE SKIP LOCKED} — expressed by the {@code -2} lock timeout — is what makes this
     * safe to run on every instance at once: each caller takes rows no one else holds and steps over the
     * rest instead of blocking. The row lock <em>is</em> what reserves the row, so no lease table and no
     * leader election are needed.
     *
     * <h2>Why the join to {@code step_instance}</h2>
     * A step has up to three transition rows (its two thresholds and its {@code MET_CONDITION_REACHED}),
     * and {@code FOR UPDATE SKIP LOCKED} on {@code t} alone only protects a single row at a time — two
     * replicas can each claim a different row of the <em>same</em> step and both judge it concurrently,
     * racing on {@code step_instance.sla_status}. Fetching {@code StepInstance} in this query brings it
     * under the same {@code PESSIMISTIC_WRITE} lock: Hibernate emits {@code ... join step_instance ...
     * for no key update skip locked}, with no {@code OF} list, so a row is skipped if either its own row
     * or its step is already held. A step can therefore only ever be claimed by one replica at a time,
     * for as long as that replica's transaction runs — see
     * {@link org.openphc.cce.sla.service.SlaTransitionApplier}'s {@code MAX_BATCH_SIZE} for why
     * {@code cce.sla.batch-size} is capped, which bounds how long that hold can last.
     *
     * <p>The fetch also hands the applier every step it judges, so a batch costs one query however large
     * it grows, and a step's rows in the same batch all see the one managed instance — a verdict the
     * first writes is visible to the rest. The join must keep {@code FETCH} (or otherwise select from
     * {@code s}): a bare {@code JOIN t.stepInstance s} is pruned from the SQL, since the association is
     * non-null and nothing of {@code s} is used, and the step's lock goes with it.
     *
     * <p>Selects on {@code next_attempt_at}, equal to {@code process_by} initially and pushed out by a
     * failure so a retry is deferred without rewriting {@code process_by} — which stays the immutable
     * record of when the deadline fell. The {@code process_by <= :now} bound is redundant with
     * {@code next_attempt_at <= :now} ({@code next_attempt_at} is never earlier than {@code process_by}),
     * but paired with the partial index {@code idx_sslt_due_order} (on {@code process_by}, Matcher's
     * migration) it lets Postgres walk that index in {@code ORDER BY} order and stop after one batch,
     * instead of reading and sorting the whole due backlog — the join makes that sort more expensive, not
     * less, so the two changes ship together.
     *
     * <p>One query for all three transition types. {@code MET_CONDITION_REACHED} needs no predicate of
     * its own: Matcher writes it with a {@code process_by} of the {@code completed_at} that satisfied it,
     * so it is already past and this fetch takes it on the next cycle — which is what retired the
     * separate sweep of {@code step_instance} that used to settle {@code MET}.
     *
     * <p>The only way a transition row is fetched. There is deliberately no second path reaching rows by
     * their step's state — taking a settled step's remaining row ahead of its deadline would buy nothing
     * and cost the retry contract. Nothing would change: the verdict is a function of {@code process_by}
     * and the step's own columns, none of which move while the row is pending, so an early apply produces
     * exactly the outcome the deadline produces later. And such a query would have to ask for
     * {@code next_attempt_at > :now}, which is precisely the state {@code backOff} puts a failed row
     * into — it would re-fetch on the next cycle a row the back-off had just deferred, so the exponential
     * interval would never take effect for the rows it covered.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query("""
            SELECT t FROM StepSlaStateTransition t
            JOIN FETCH t.stepInstance s
            WHERE t.processed = false
              AND t.nextAttemptAt <= :now
              AND t.processBy <= :now
            ORDER BY t.processBy ASC
            """)
    List<StepSlaStateTransition> fetchTransitions(@Param("now") OffsetDateTime now, Limit limit);

    /**
     * The {@code cce.sla.transitions.due} gauge: rows the next cycle will fetch.
     *
     * <p>Carries {@link #fetchTransitions}'s row-eligibility predicate — the gauge has to count what the
     * next cycle will fetch, or it stops being a backlog. Counting every unprocessed row would instead
     * fold in the whole future schedule, so it would track enrolment volume rather than lateness and
     * could never sit near zero. Deliberately no join to {@code StepInstance} and no lock here: this is a
     * plain count, not a claim, so it never contends with a replica mid-batch.
     */
    @Query("""
            SELECT COUNT(t) FROM StepSlaStateTransition t
            WHERE t.processed = false
              AND t.nextAttemptAt <= :now
            """)
    long countDueTransitions(@Param("now") OffsetDateTime now);
}
