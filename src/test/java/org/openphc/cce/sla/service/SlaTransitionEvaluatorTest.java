package org.openphc.cce.sla.service;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SlaTransitionEvaluatorTest {

    @Mock private SlaTransitionApplier applier;

    private SlaTransitionEvaluator evaluator(int batchSize) {
        return new SlaTransitionEvaluator(applier, batchSize, new SimpleMeterRegistry());
    }

    @Test
    void drainsUntilABatchComesBackShort() {
        // A backlog is cleared within one cycle rather than one batch per poll interval.
        when(applier.fetchAndApply(any())).thenReturn(10, 10, 3);

        assertEquals(23, evaluator(10).evaluateDue());
        verify(applier, times(3)).fetchAndApply(any());
    }

    @Test
    void emptyBacklogCostsASingleFetch() {
        when(applier.fetchAndApply(any())).thenReturn(0);

        assertEquals(0, evaluator(10).evaluateDue());
        verify(applier, times(1)).fetchAndApply(any());
    }

    @Test
    void failedBatch_backsOffTheRowsItHadFetched() {
        // The batch rolled back, so nothing was marked processed. The ids it collected before failing
        // are still in the caller's list, which is what lets them be deferred.
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        doAnswer(inv -> {
            List<UUID> fetched = inv.getArgument(0);
            fetched.add(a);
            fetched.add(b);
            throw new IllegalStateException("boom");
        }).when(applier).fetchAndApply(any());

        assertEquals(0, evaluator(10).evaluateDue());
        verify(applier).backOff(List.of(a, b));
    }

    @Test
    void failedBatch_stopsTheCycleRatherThanRetryingImmediately() {
        doThrow(new IllegalStateException("boom")).when(applier).fetchAndApply(any());

        evaluator(10).evaluateDue();

        verify(applier, times(1)).fetchAndApply(any());
    }

    @Test
    void poll_logsAndReturnsWhenWorkWasApplied() {
        when(applier.fetchAndApply(any())).thenReturn(1);

        assertDoesNotThrow(() -> evaluator(10).poll());
        verify(applier).fetchAndApply(any());
    }

    @Test
    void stopsAtTheCycleLimitRatherThanMonopolisingTheThread() {
        // A fetch that never drains must not spin forever inside one cycle.
        when(applier.fetchAndApply(any())).thenReturn(5);

        assertEquals(500, evaluator(5).evaluateDue());
        verify(applier, times(100)).fetchAndApply(any());
    }

    @Test
    void pollNeverPropagates_soTheSchedulerThreadSurvives() {
        doThrow(new IllegalStateException("boom")).when(applier).fetchAndApply(any());

        assertDoesNotThrow(() -> evaluator(10).poll());
    }
}
