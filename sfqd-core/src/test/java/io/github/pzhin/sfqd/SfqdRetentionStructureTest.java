package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class SfqdRetentionStructureTest {
    @Test
    void lifecycleTransitionsReleaseDomainObjectsFromSchedulerReachability()
            throws ReflectiveOperationException {
        SfqdScheduler<Object, Object, Object> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(1, 1, 2));
        Object flowId = new Object();
        Object firstJobId = new Object();
        Object secondJobId = new Object();
        Object firstPayload = new Object();
        Object secondPayload = new Object();
        FlowHandle flow = registered(scheduler.registerFlow(flowId, 1L));
        JobHandle first = accepted(scheduler.enqueue(flow, firstJobId, firstPayload, 1L));
        JobHandle second = accepted(scheduler.enqueue(flow, secondJobId, secondPayload, 1L));
        Object firstQueuedNode = mapField(scheduler, "queued").get(first);
        Object secondQueuedNode = mapField(scheduler, "queued").get(second);
        Object firstStartTag = field(firstQueuedNode, "start").get(firstQueuedNode);

        Dispatch<Object, Object, Object> dispatch = scheduler.dispatchUpTo(1).get(0);

        assertSame(firstPayload, dispatch.payload());
        assertFalse(reachableFrom(scheduler, firstPayload));
        assertFalse(reachableFrom(scheduler, firstQueuedNode));
        assertTrue(reachableFrom(scheduler, firstJobId));
        assertTrue(reachableFrom(scheduler, secondPayload));
        assertTrue(reachableFrom(scheduler, secondQueuedNode));
        Object runningRecord = mapField(scheduler, "running").get(first);
        assertFalse(reachableFrom(runningRecord, firstPayload));
        assertFalse(reachableFrom(runningRecord, firstQueuedNode));
        assertFalse(reachableFrom(runningRecord, firstStartTag));
        assertFalse(reachableTypeFrom(runningRecord, ExactTag.class));

        assertEquals(CancelResult.CANCELLED, scheduler.cancel(second));
        Object flowState = mapField(scheduler, "registeredFlows").get(flow);
        assertNull(field(flowState, "head").get(flowState));
        assertNull(field(flowState, "tail").get(flowState));
        assertFalse(reachableFrom(scheduler, secondJobId));
        assertFalse(reachableFrom(scheduler, secondPayload));
        assertFalse(reachableFrom(scheduler, secondQueuedNode));
        assertFalse(reachableFrom(scheduler, second));

        assertEquals(CompletionResult.COMPLETED, scheduler.complete(first));
        assertFalse(reachableFrom(scheduler, firstJobId));
        assertFalse(reachableFrom(scheduler, firstPayload));
        assertFalse(reachableFrom(scheduler, first));
        assertTrue(mapField(scheduler, "queued").isEmpty());
        assertTrue(mapField(scheduler, "running").isEmpty());
        assertTrue(mapField(scheduler, "liveById").isEmpty());
        assertFalse(iterableField(scheduler, "backlogged").iterator().hasNext());
        assertTrue(reachableFrom(scheduler, flowId));

        assertEquals(CloseFlowResult.CLOSED, scheduler.closeFlow(flow));
        assertFalse(reachableFrom(scheduler, flowId));
        assertFalse(reachableFrom(scheduler, flow));
        assertTrue(mapField(scheduler, "registeredById").isEmpty());
        assertTrue(mapField(scheduler, "registeredFlows").isEmpty());
    }

    @Test
    void handlesAreInertAndNoTerminalMapsExist() {
        assertHandleShape(FlowHandle.class);
        assertHandleShape(JobHandle.class);
        assertEquals(0, OwnerToken.class.getDeclaredFields().length);

        Set<String> mapFields = Arrays.stream(SfqdScheduler.class.getDeclaredFields())
                .filter(field -> Map.class.isAssignableFrom(field.getType()))
                .map(Field::getName)
                .collect(Collectors.toUnmodifiableSet());
        assertEquals(Set.of("registeredById", "registeredFlows", "liveById", "queued", "running"), mapFields);
        Set<String> collectionFields = Arrays.stream(SfqdScheduler.class.getDeclaredFields())
                .filter(field -> Collection.class.isAssignableFrom(field.getType()))
                .map(Field::getName)
                .collect(Collectors.toUnmodifiableSet());
        assertEquals(Set.of("backlogged"), collectionFields);
    }

    private static void assertHandleShape(Class<?> handleType) {
        Field[] fields = Arrays.stream(handleType.getDeclaredFields())
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .toArray(Field[]::new);
        assertEquals(2, fields.length);
        assertEquals(Set.of(OwnerToken.class, long.class),
                Arrays.stream(fields).map(Field::getType).collect(Collectors.toUnmodifiableSet()));
    }

    private static boolean reachableFrom(Object root, Object target) throws IllegalAccessException {
        return traverse(root, value -> value == target);
    }

    private static boolean reachableTypeFrom(Object root, Class<?> targetType) throws IllegalAccessException {
        return traverse(root, targetType::isInstance);
    }

    private static boolean traverse(Object root, java.util.function.Predicate<Object> matches)
            throws IllegalAccessException {
        Deque<Object> pending = new ArrayDeque<>();
        IdentityHashMap<Object, Boolean> visited = new IdentityHashMap<>();
        pending.add(root);
        while (!pending.isEmpty()) {
            Object current = pending.removeFirst();
            if (matches.test(current)) {
                return true;
            }
            if (visited.put(current, Boolean.TRUE) != null) {
                continue;
            }
            if (current instanceof Map<?, ?> map) {
                map.forEach((key, value) -> {
                    addIfPresent(pending, key);
                    addIfPresent(pending, value);
                });
            } else if (current instanceof Iterable<?> iterable) {
                iterable.forEach(value -> addIfPresent(pending, value));
            } else if (current.getClass().isArray()) {
                for (int index = 0; index < Array.getLength(current); index++) {
                    addIfPresent(pending, Array.get(current, index));
                }
            } else if (isProjectObject(current)) {
                for (Field field : current.getClass().getDeclaredFields()) {
                    if (!Modifier.isStatic(field.getModifiers()) && !field.getType().isPrimitive()) {
                        field.setAccessible(true);
                        addIfPresent(pending, field.get(current));
                    }
                }
            }
        }
        return false;
    }

    private static boolean isProjectObject(Object value) {
        return value.getClass().getPackageName().equals("io.github.pzhin.sfqd");
    }

    private static void addIfPresent(Deque<Object> pending, Object value) {
        if (value != null) {
            pending.addLast(value);
        }
    }

    private static Map<?, ?> mapField(Object target, String name) throws ReflectiveOperationException {
        return (Map<?, ?>) field(target, name).get(target);
    }

    private static Iterable<?> iterableField(Object target, String name) throws ReflectiveOperationException {
        return (Iterable<?>) field(target, name).get(target);
    }

    private static Field field(Object target, String name) throws ReflectiveOperationException {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    private static FlowHandle registered(RegisterFlowResult result) {
        return ((RegisterFlowResult.Registered) result).flowHandle();
    }

    private static JobHandle accepted(EnqueueResult result) {
        return ((EnqueueResult.Accepted) result).jobHandle();
    }
}
