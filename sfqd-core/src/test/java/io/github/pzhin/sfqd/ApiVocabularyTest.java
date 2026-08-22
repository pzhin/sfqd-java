package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

final class ApiVocabularyTest {
    @Test
    void configRejectsValuesOutsideTheSpecifiedDomain() {
        assertThrows(IllegalArgumentException.class, () -> new SchedulerConfig(0, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new SchedulerConfig(1_000_001, 1, 1_000_001));
        assertThrows(IllegalArgumentException.class, () -> new SchedulerConfig(1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new SchedulerConfig(1, -1, 1));
        assertThrows(IllegalArgumentException.class, () -> new SchedulerConfig(2, 1, 1));
        assertEquals(8, new SchedulerConfig(8, 10, 20).depth());
    }

    @Test
    void configMakesCancellationAccountingExplicit() {
        SchedulerConfig defaultConfig = new SchedulerConfig(8, 10, 20);
        SchedulerConfig explicitConfig = new SchedulerConfig(
                8, 10, 20, CancellationAccounting.CHARGE_RESERVED_COST);

        assertEquals(CancellationAccounting.CHARGE_RESERVED_COST, defaultConfig.cancellationAccounting());
        assertEquals(defaultConfig, explicitConfig);
        assertEquals(1, CancellationAccounting.values().length);
    }

    @Test
    void handlesAreOpaqueInertCapabilitiesWithOwnerScopedEquality() {
        OwnerToken firstOwner = new OwnerToken();
        OwnerToken secondOwner = new OwnerToken();
        FlowHandle first = new FlowHandle(firstOwner, 1L);
        FlowHandle equal = new FlowHandle(firstOwner, 1L);
        FlowHandle anotherSequence = new FlowHandle(firstOwner, 2L);
        FlowHandle anotherOwner = new FlowHandle(secondOwner, 1L);
        JobHandle job = new JobHandle(firstOwner, 1L);

        assertEquals(first, equal);
        assertEquals(first.hashCode(), equal.hashCode());
        assertNotEquals(first, anotherSequence);
        assertNotEquals(first, anotherOwner);
        assertNotEquals(first, job);
        assertFalse(Comparable.class.isAssignableFrom(FlowHandle.class));
        assertFalse(Serializable.class.isAssignableFrom(FlowHandle.class));
        assertFalse(Arrays.stream(FlowHandle.class.getMethods())
                .map(Method::getName)
                .anyMatch(name -> name.equals("sequence") || name.equals("ownerToken")));
        assertFalse(Arrays.stream(FlowHandle.class.getDeclaredConstructors())
                .anyMatch(constructor -> Modifier.isPublic(constructor.getModifiers())));
        assertEquals("FlowHandle[opaque]", first.toString());
        assertEquals("JobHandle[opaque]", job.toString());
        assertEquals(job, new JobHandle(firstOwner, 1L));
        assertEquals(job.hashCode(), new JobHandle(firstOwner, 1L).hashCode());
        assertNotEquals(job, new JobHandle(firstOwner, 2L));
        assertNotEquals(job, new JobHandle(secondOwner, 1L));
        assertNotEquals(job, first);
        assertThrows(IllegalArgumentException.class, () -> new FlowHandle(firstOwner, 0L));
        assertThrows(IllegalArgumentException.class, () -> new JobHandle(firstOwner, 0L));
    }

    @Test
    void dispatchUsesIdentityEqualityAndExposesExactlyItsDetachedValues() {
        OwnerToken owner = new OwnerToken();
        JobHandle handle = new JobHandle(owner, 1L);
        Object payload = new Object();
        Dispatch<String, String, Object> first = new Dispatch<>(handle, "job", "flow", payload, 7L);
        Dispatch<String, String, Object> second = new Dispatch<>(handle, "job", "flow", payload, 7L);

        assertNotSame(first, second);
        assertNotEquals(first, second);
        assertSame(handle, first.jobHandle());
        assertEquals("job", first.jobId());
        assertEquals("flow", first.flowId());
        assertSame(payload, first.payload());
        assertEquals(7L, first.cost());
        assertThrows(IllegalArgumentException.class, () -> new Dispatch<>(handle, "job", "flow", payload, 0L));
        assertFalse(Arrays.stream(Dispatch.class.getDeclaredConstructors())
                .anyMatch(constructor -> Modifier.isPublic(constructor.getModifiers())));
    }

    @Test
    void valueOutcomesAndSnapshotCarryOnlySpecifiedData() {
        OwnerToken owner = new OwnerToken();
        FlowHandle flow = new FlowHandle(owner, 1L);
        JobHandle job = new JobHandle(owner, 1L);
        assertSame(flow, new RegisterFlowResult.Registered(flow).flowHandle());
        assertSame(job, new EnqueueResult.Accepted(job).jobHandle());
        assertEquals(3, RegisterFlowResult.Rejected.values().length);
        assertEquals(5, EnqueueResult.Rejected.values().length);
        assertEquals(4, CloseFlowResult.values().length);
        assertEquals(3, CancelResult.values().length);
        assertEquals(3, CompletionResult.values().length);

        SchedulerSnapshot snapshot = new SchedulerSnapshot(8, 10, 20, 2, 3, 4, 4, 2, 1, 10L, 6L, 1L, 2L);
        assertEquals(8, snapshot.depth());
        assertEquals(10, snapshot.maxFlows());
        assertEquals(20, snapshot.maxLiveJobs());
        assertEquals(2, snapshot.registeredFlows());
        assertEquals(3, snapshot.queuedJobs());
        assertEquals(4, snapshot.runningJobs());
        assertEquals(4, snapshot.freeSlots());
        assertEquals(2, snapshot.activeFlows());
        assertEquals(1, snapshot.backloggedFlows());
        assertEquals(10L, snapshot.acceptedTotal());
        assertEquals(6L, snapshot.dispatchedTotal());
        assertEquals(1L, snapshot.cancelledTotal());
        assertEquals(2L, snapshot.completedTotal());
        assertTrue(snapshot.toString().contains("acceptedTotal=10"));
        SchedulerSnapshot copy = new SchedulerSnapshot(8, 10, 20, 2, 3, 4, 4, 2, 1, 10L, 6L, 1L, 2L);
        assertEquals(snapshot, copy);
        assertEquals(snapshot.hashCode(), copy.hashCode());
        assertFalse(Arrays.stream(SchedulerSnapshot.class.getDeclaredConstructors())
                .anyMatch(constructor -> Modifier.isPublic(constructor.getModifiers())));
        assertFalse(Arrays.stream(RegisterFlowResult.Registered.class.getDeclaredConstructors())
                .anyMatch(constructor -> Modifier.isPublic(constructor.getModifiers())));
        assertFalse(Arrays.stream(EnqueueResult.Accepted.class.getDeclaredConstructors())
                .anyMatch(constructor -> Modifier.isPublic(constructor.getModifiers())));
    }
}
