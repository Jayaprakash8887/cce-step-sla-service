package org.openphc.cce.sla.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.openphc.cce.common.entity.StepInstance;
import org.openphc.cce.common.entity.StepSlaStateTransition;
import org.openphc.cce.common.enums.SlaStatus;
import org.openphc.cce.common.enums.SlaTransitionType;
import org.openphc.cce.common.enums.StepStatus;
import org.openphc.cce.common.repository.StepInstanceRepository;
import org.openphc.cce.common.support.RequiredBehavior;
import org.openphc.cce.common.deviation.DeviationRecorder;
import org.openphc.cce.common.history.StateTransitionHistoryWriter;
import org.openphc.cce.sla.domain.repository.SlaTransitionFetchRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Fetches due {@code step_sla_state_transition} rows and applies them.
 *
 * <p>The Matcher Service writes one row per threshold a step can cross and never touches it again.
 * Everything after that is owned here: deciding which rows are due, writing
 * {@code step_instance.sla_status}, raising the {@code OVERDUE} / {@code MISSED} deviation, recording
 * the transition in {@code step_instance_history}, and marking the row processed. There is no Kafka hop
 * and no HTTP call between the two services — they meet on this one table.
 *
 * <p>Fetch and apply share a transaction. The row lock taken by {@code FOR UPDATE SKIP LOCKED} is what
 * reserves the row, so concurrent instances drain disjoint sets with no lease table and no leader election, and a
 * deviation can never be recorded without the row being marked processed in the same commit.
 *
 * <h2>The fetch also locks the step</h2>
 * {@link SlaTransitionFetchRepository#fetchTransitions} joins {@code StepInstance} under the same lock,
 * so a step is held by exactly one replica for the whole of {@link #fetchAndApply} — not just for the
 * instant {@link #writeSlaStatus} runs its {@code UPDATE}. That is what makes the forward-only check in
 * {@link #writeSlaStatus} reliable: by the time it reads {@code sla_status}, no other replica can be
 * concurrently judging the same step's other rows, and Matcher cannot be mid-completion on it either.
 * {@link #MAX_BATCH_SIZE} bounds how long that hold can last, since every step tied to a row in the batch
 * is locked from the fetch, whether or not that row turns out to need a write at all.
 *
 * <p>Separate bean from {@link SlaTransitionEvaluator}, which drives the polling loop. Not cosmetic:
 * Spring's transaction proxy is bypassed by self-invocation, so a driver calling its own
 * {@code @Transactional} method would silently run it without a transaction.
 *
 * <h2>This service is the only writer of sla_status</h2>
 * Matcher records <em>that</em> a step was completed and when; it never judges whether that was timely.
 * So there is no rule here about not overwriting what Matcher decided — it decided nothing. A step's
 * {@code sla_status} is null until a threshold falls due and this service judges it.
 *
 * <p>The judgement compares {@code step_instance.completed_at} — the clinical occurrence time of the
 * completing event — against the threshold the row stands for. The wall clock never enters into it:
 * what a deadline <em>means</em> depends only on whether the work had happened by then.
 *
 * <p>Every verdict comes from a row, including {@code MET}. {@code OVERDUE} and {@code MISSED} are
 * breaches of a deadline, measured against the {@code process_by} the row carries. {@code MET} is not a
 * breach but a condition already satisfied: Matcher writes a {@code MET_CONDITION_REACHED} row the
 * moment a completing event lands before the step's due date, with that {@code completed_at} as its
 * {@code process_by}, so the row is due at once and this service records the verdict on its next cycle.
 * One fetch, one loop — there is no second sweep of {@code step_instance}, even though the fetch now
 * joins it to take the step-level lock described above.
 *
 * <p>A row is fetched for exactly one reason: its schedule has come round, and the verdict it stands
 * for must be reached ({@code fetchTransitions}). Nothing pulls a step's remaining rows forward because
 * the step completed or was judged — a settled step keeps its unspent schedule until those dates
 * arrive, and each row is consumed then, recording nothing.
 *
 * <h2>Only mandatory steps are judged</h2>
 * A deadline is the point at which work the protocol <em>required</em> has not been recorded, so only a
 * mandatory step can breach one. Matcher schedules no row for an optional step, and a protocol that
 * gives an optional action a {@code tolerance-days} is rejected at load. A row for an optional step can
 * therefore only be one written before those rules, and the {@code V4} migration deleted those — so
 * this is a guard against a row that should not exist at all. It is consumed, leaving no
 * {@code sla_status} and no deviation, whatever it stands for, {@code MET} included. Scheduling and
 * judging enforce the rule separately, so neither alone has to be trusted. The table below describes
 * mandatory steps.
 *
 * <table border="1">
 *   <caption>Behaviour by threshold and step state, for a mandatory step</caption>
 *   <tr><th>Row</th><th>Step state when applied</th><th>Action</th></tr>
 *   <tr><td>{@code DUE_DATE_REACHED}</td><td>not completed</td>
 *       <td>{@code OVERDUE} + {@code OVERDUE} deviation</td></tr>
 *   <tr><td>{@code DUE_DATE_REACHED}</td><td>{@code completed_at >= process_by}</td>
 *       <td>{@code OVERDUE} + {@code OVERDUE} deviation — recorded, but late</td></tr>
 *   <tr><td>{@code DUE_DATE_REACHED}</td><td>{@code completed_at < process_by}</td>
 *       <td>consume — no breach; the step's {@code MET_CONDITION_REACHED} row carries that verdict,
 *       and reached it when the work landed</td></tr>
 *   <tr><td>{@code MISSED_DATE_REACHED}</td><td>not completed</td>
 *       <td>{@code MISSED} + {@code MISSED} deviation</td></tr>
 *   <tr><td>{@code MISSED_DATE_REACHED}</td><td>{@code completed_at >= process_by}</td>
 *       <td>{@code MISSED} + {@code MISSED} deviation</td></tr>
 *   <tr><td>{@code MISSED_DATE_REACHED}</td><td>{@code completed_at < process_by}</td>
 *       <td>consume — this threshold was not breached, and the due-date row already had its say</td></tr>
 *   <tr><td>{@code MET_CONDITION_REACHED}</td><td>{@code completed_at < due_date}</td>
 *       <td>{@code MET}, no deviation — there is nothing deviant about work done on time</td></tr>
 *   <tr><td>{@code MET_CONDITION_REACHED}</td><td>anything else</td>
 *       <td>consume — the step no longer reads as on time; the judgement is made here, from the step,
 *       not taken on the row's word</td></tr>
 * </table>
 *
 * <p>The missed-date row of a completed step is the one worth being careful about: a step completed
 * <em>between</em> its two thresholds did not breach the missed date, but it is not {@code MET} either
 * — it is the {@code OVERDUE} the due-date row made it, and it has no {@code MET_CONDITION_REACHED} row
 * because it never beat its due date. "Did not breach this threshold" and "met its SLA" are only the
 * same thing at the due date.
 *
 * <p>{@code step_status} is never written here. Crossing a deadline says nothing about whether the event
 * arrived.
 *
 * <p>Nothing in the table turns on <em>when</em> a row is applied, which is what makes fetching a
 * completed step's rows early safe: the same columns decide the outcome whether the row is applied at
 * its scheduled time or the moment the completion is seen. Reading the due date off the step rather than
 * off the schedule strengthens that — the value the verdict turns on is one the sweep never rewrites.
 */
@Service
public class SlaTransitionApplier {

    private static final Logger log = LoggerFactory.getLogger(SlaTransitionApplier.class);

    /** Past this many attempts a row is logged as an error every cycle rather than failing quietly. */
    private static final int ATTEMPTS_BEFORE_ALERT = 5;

    /**
     * Hard ceiling on {@code cce.sla.batch-size}, enforced at startup.
     *
     * <p>The fetch locks every step tied to a row in the batch for the whole of {@link #fetchAndApply},
     * not just the moment a write happens — see {@link SlaTransitionFetchRepository#fetchTransitions}.
     * Measured end to end against this exact fetch-and-apply path (not a prototype) on a 20-million-row
     * {@code step_instance} table: 250–350 rows reliably finished under the 5-second poll interval;
     * 450–550 rows straddled it, with real run-to-run variance (450 rows ranged 4.5–9.7 seconds across
     * repeated runs, driven by whether that run's randomly-selected steps happened to be cache-resident);
     * 550 and above was over 5 seconds more often than not. The cost is dominated by the apply loop, not
     * the fetch — a single Hibernate session accumulating dirty state across the whole batch — which is
     * why it does not scale smoothly with batch size the way the fetch query's own cost does.
     *
     * <p>100 — the existing default — sits comfortably below where this starts, with margin for the
     * variance observed. Raising this value without re-measuring on production-scale hardware risks
     * holding {@code step_instance} locks, and therefore blocking Matcher's own writes to those steps,
     * for several seconds per batch.
     */
    private static final int MAX_BATCH_SIZE = 100;

    private final SlaTransitionFetchRepository transitionRepository;
    private final StepInstanceRepository stepInstanceRepository;
    private final DeviationRecorder deviationRecorder;
    private final StateTransitionHistoryWriter stateTransitionHistoryWriter;
    private final String instanceId;
    private final int batchSize;
    private final Duration maxBackoff;
    private final Counter appliedCounter;
    private final Counter skippedCounter;

    public SlaTransitionApplier(SlaTransitionFetchRepository transitionRepository,
                                StepInstanceRepository stepInstanceRepository,
                                DeviationRecorder deviationRecorder,
                                StateTransitionHistoryWriter stateTransitionHistoryWriter,
                                @Value("${cce.sla.instance-id:${HOSTNAME:local}}") String instanceId,
                                @Value("${cce.sla.batch-size:100}") int batchSize,
                                @Value("${cce.sla.max-backoff-seconds:3600}") long maxBackoffSeconds,
                                MeterRegistry meterRegistry) {
        if (batchSize > MAX_BATCH_SIZE) {
            // Fail at startup, not at the first oversized batch in production. See MAX_BATCH_SIZE's
            // Javadoc for the measurements behind this ceiling.
            throw new IllegalArgumentException(
                    "cce.sla.batch-size (%d) exceeds the maximum of %d — see SlaTransitionApplier.MAX_BATCH_SIZE"
                            .formatted(batchSize, MAX_BATCH_SIZE));
        }
        this.transitionRepository = transitionRepository;
        this.stepInstanceRepository = stepInstanceRepository;
        this.deviationRecorder = deviationRecorder;
        this.stateTransitionHistoryWriter = stateTransitionHistoryWriter;
        this.instanceId = instanceId;
        this.batchSize = batchSize;
        this.maxBackoff = Duration.ofSeconds(maxBackoffSeconds);
        this.appliedCounter = Counter.builder("cce.sla.transitions.applied")
                .description("SLA transitions that wrote a step's sla_status")
                .register(meterRegistry);
        this.skippedCounter = Counter.builder("cce.sla.transitions.skipped")
                .description("SLA transitions that reached no verdict: an optional step's stale schedule, or a step already settled")
                .register(meterRegistry);
    }

    /**
     * Fetch and apply one batch of due transitions.
     *
     * <p>One query, one gate. {@code next_attempt_at} decides eligibility and nothing else does; it then
     * plays no part in the judgement, which reads {@code transition_type} and {@code process_by} from the
     * row — both immutable — and {@code step_status}, {@code completed_at}, {@code sla_status} and
     * {@code required_behavior} from the step. So <em>when</em> a row is applied cannot change what it
     * decides.
     *
     * @param fetched populated with the id of every row fetched, so the caller can back them off if the
     *                transaction rolls back — the list is plain memory and survives the rollback
     * @return how many rows were fetched
     */
    @Transactional
    public int fetchAndApply(List<UUID> fetched) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        List<StepSlaStateTransition> dueRows = transitionRepository.fetchTransitions(now, Limit.of(batchSize));
        Map<UUID, StepInstance> stepsById = loadStepsFor(dueRows);

        for (StepSlaStateTransition row : dueRows) {
            fetched.add(row.getId());
            row.setAttempts(row.getAttempts() + 1);
            if (row.getAttempts() > ATTEMPTS_BEFORE_ALERT) {
                log.error("SLA transition {} for step {} has now been attempted {} times",
                        row.getId(), row.getStepInstanceId(), row.getAttempts());
            }
            applyRow(row, stepsById.get(row.getStepInstanceId()));
        }
        return dueRows.size();
    }

    /**
     * Every step the batch judges, in one query.
     *
     * <p>Fetching each row's step as the row is applied would be a query per row — a hundred round
     * trips for a hundred-row batch, all inside the transaction holding the row locks. One
     * {@code IN (…)} instead, before the loop, so the cost of a batch is two queries however large
     * {@code batch-size} grows.
     *
     * <p>Ids are de-duplicated because a step can have up to three rows — its two thresholds and its
     * {@code MET_CONDITION_REACHED} — and they can come round in the same batch. They then all see the
     * same managed instance, so a verdict written by the first is visible to the rest.
     */
    private Map<UUID, StepInstance> loadStepsFor(List<StepSlaStateTransition> rows) {
        if (rows.isEmpty()) {
            return Map.of();
        }
        List<UUID> stepIds = rows.stream()
                .map(StepSlaStateTransition::getStepInstanceId)
                .distinct()
                .toList();
        return stepInstanceRepository.findAllById(stepIds).stream()
                .collect(Collectors.toMap(StepInstance::getId, Function.identity()));
    }

    private void applyRow(StepSlaStateTransition row, StepInstance step) {
        if (step == null) {
            // Unreachable while the schema holds: step_instance_id is NOT NULL and carries a foreign key
            // to step_instance(id), with no cascade, so the database refuses to leave a row without its
            // step. Kept because the alternative to a guard here is a NullPointerException on every
            // cycle: a row whose step is somehow gone can never succeed, so consume it rather than
            // retry it forever.
            log.warn("SLA transition {} references step {} which no longer exists — consuming",
                    row.getId(), row.getStepInstanceId());
            markProcessed(row);
            return;
        }

        if (!RequiredBehavior.isMandatory(step.getRequiredBehavior())) {
            // Nothing was required of an optional step, so it has no deadline to breach and none to
            // have beaten — no verdict of any kind is its to reach. Matcher schedules no row for one
            // and a protocol that gives an optional action a deadline is rejected at load, so this row
            // predates those rules: it is closed rather than retried, and leaves nothing behind.
            skippedCounter.increment();
            log.warn("Step {} is optional (requiredBehavior={}) — {} transition {} is a stale schedule "
                            + "and records no status or deviation",
                    step.getId(), step.getRequiredBehavior(), row.getTransitionType(), row.getId());
            markProcessed(row);
            return;
        }

        if (row.getTransitionType() == SlaTransitionType.MET_CONDITION_REACHED) {
            applyOnTime(row, step);
        } else if (breachedThreshold(row, step)) {
            applyBreach(row, step);
        }
        // A deadline that was kept records nothing: the step's own MET row carries that verdict, and
        // beating the missed date says only that the step was not written off. Such a row falls
        // straight through to being marked processed.
        markProcessed(row);
    }

    /**
     * Was the work still unrecorded when this threshold fell?
     *
     * <p>A step not completed at all has plainly breached it. A completed one is judged on its
     * {@code completed_at}: at or after the threshold is a breach, before it is not. A completed step
     * with no {@code completed_at} is treated as a breach — the row is the better evidence than a
     * missing timestamp, and silently letting it pass would hide the gap.
     */
    private boolean breachedThreshold(StepSlaStateTransition row, StepInstance step) {
        if (step.getStepStatus() != StepStatus.COMPLETED) {
            return true;
        }
        OffsetDateTime completedAt = step.getCompletedAt();
        return completedAt == null || !completedAt.isBefore(row.getProcessBy());
    }

    /**
     * The work was recorded before the due date: settle the step as {@code MET}.
     *
     * <p>Matcher writes this row when the completing event lands, so it is due immediately and the
     * verdict is reached within a cycle rather than at a due date that may be weeks away. The judgement
     * is still made here, from the step's own columns — Matcher scheduled the question, it did not
     * answer it.
     *
     * <p>No deviation: there is nothing deviant about work done on time.
     */
    private void applyOnTime(StepSlaStateTransition row, StepInstance step) {
        if (!beatItsDueDate(step)) {
            skippedCounter.increment();
            log.warn("Step {} no longer reads as on time (stepStatus={}, completedAt={}, dueDate={}) — "
                            + "transition {} records nothing",
                    step.getId(), step.getStepStatus(), step.getCompletedAt(), step.getDueDate(),
                    row.getId());
            return;
        }

        if (!writeSlaStatus(step, SlaStatus.MET)) {
            // Already judged: a re-fetched row, or a deadline that beat this row to the step.
            skippedCounter.increment();
            return;
        }

        log.debug("Step {} was recorded at {}, before its due date of {} — MET",
                step.getId(), step.getCompletedAt(), step.getDueDate());
    }

    /** Whether the step's own columns still say the work beat its deadline. */
    private boolean beatItsDueDate(StepInstance step) {
        return step.getStepStatus() == StepStatus.COMPLETED
                && step.getCompletedAt() != null
                && step.getDueDate() != null
                && step.getCompletedAt().isBefore(step.getDueDate());
    }

    /** The deadline was not met: advance the SLA and record the deviation. */
    private void applyBreach(StepSlaStateTransition row, StepInstance step) {
        if (!writeSlaStatus(step, row.getTransitionType().breachStatus())) {
            // Already at or past this outcome: a re-fetched row, or rows applied out of order.
            skippedCounter.increment();
            return;
        }

        raiseDeviationFor(row, step);
    }

    /**
     * Write {@code sla_status}, recording the transition in history, unless the step is already at a
     * status this one must not overwrite.
     *
     * <p>Forward-only. {@code MET} and {@code MISSED} are settled outcomes, and {@code OVERDUE} must
     * never replace {@code MISSED} — which is what would happen if the two rows for a step were applied
     * out of order after a retry. {@code MET} is written only from null, so a step already found
     * {@code OVERDUE} cannot be relabelled as having been on time.
     *
     * <p>This check reads {@code step.getSlaStatus()} with no lock of its own, which would ordinarily be
     * a race between two replicas each holding a different row of the same step. It is safe here only
     * because {@link SlaTransitionFetchRepository#fetchTransitions} already holds this step under
     * {@code FOR UPDATE} for the whole batch — no other replica can be concurrently reading or writing
     * it. Do not call this method, or read a step loaded outside that fetch, without that lock in place.
     *
     * @return whether the status was written
     */
    private boolean writeSlaStatus(StepInstance step, SlaStatus newStatus) {
        SlaStatus currentStatus = step.getSlaStatus();
        if (!canAdvance(currentStatus, newStatus)) {
            log.debug("Step {} is already {} — not writing {}", step.getId(), currentStatus, newStatus);
            return false;
        }

        step.setSlaStatus(newStatus);
        stepInstanceRepository.save(step);
        stateTransitionHistoryWriter.recordStepInstanceTransition(
                step, OffsetDateTime.now(ZoneOffset.UTC));
        appliedCounter.increment();

        log.info("Step {} (actionId={}) SLA {} -> {}",
                step.getId(), step.getActionId(), currentStatus, newStatus);
        return true;
    }

    /**
     * Whether {@code newStatus} may replace what the step already has.
     *
     * <p>{@code MET} is written only from null. It says the step beat its due date, which a step some
     * deadline has already judged cannot be told retrospectively.
     *
     * <p>Every other outcome moves forward only, so {@code OVERDUE} can never replace {@code MISSED} —
     * which is exactly what two rows for one step applied out of order after a retry would otherwise do.
     */
    private static boolean canAdvance(SlaStatus currentStatus, SlaStatus newStatus) {
        if (newStatus == SlaStatus.MET) {
            return currentStatus == null;
        }
        return rank(newStatus) > rank(currentStatus);
    }

    /** Ordering for the forward-only rule. Null is "not yet judged", so it precedes every outcome. */
    private static int rank(SlaStatus status) {
        if (status == null) {
            return 0;
        }
        return switch (status) {
            case OVERDUE -> 1;
            case MISSED, MET -> 2;
        };
    }

    /**
     * The deviation a breach produces — the due date an {@code OVERDUE}, the missed date a
     * {@code MISSED} — read off the row's own type, which is where that mapping is declared.
     * {@link DeviationRecorder} de-duplicates on the step and type, so a re-fetched row cannot record
     * the same deviation twice.
     */
    private void raiseDeviationFor(StepSlaStateTransition row, StepInstance step) {
        deviationRecorder.recordDeviation(step, row.getTransitionType().breachDeviation());
    }

    private void markProcessed(StepSlaStateTransition row) {
        row.setProcessed(true);
        row.setProcessedAt(OffsetDateTime.now(ZoneOffset.UTC));
        row.setProcessedBy(instanceId);
        transitionRepository.save(row);
    }

    /**
     * Defer the given rows after a failed batch, so a broken row backs off instead of being retried on
     * every cycle. Runs in its own transaction because the batch it belongs to has just rolled back.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void backOff(List<UUID> transitionIds) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        for (StepSlaStateTransition row : transitionRepository.findAllById(transitionIds)) {
            if (row.isProcessed()) {
                continue;
            }
            // Exponential in the attempt count, capped so a permanently broken row is still retried
            // occasionally rather than hammering the database.
            long seconds = Math.min(maxBackoff.getSeconds(),
                    (long) Math.pow(2, Math.min(row.getAttempts(), 20)));
            row.setAttempts(row.getAttempts() + 1);
            row.setNextAttemptAt(now.plusSeconds(seconds));
            transitionRepository.save(row);
        }
    }
}
