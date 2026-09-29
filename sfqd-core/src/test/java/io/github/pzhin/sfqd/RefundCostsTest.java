package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

final class RefundCostsTest {
    @Test
    void aggregateMatchesIndependentScanThroughRotationsDuplicatesAndDeletion() {
        RefundCosts costs = new RefundCosts();
        List<Long> remaining = new ArrayList<>();
        Random random = new Random(781923L);
        for (int index = 1; index <= 2000; index++) {
            long cost = index % 3 == 0 ? Long.MAX_VALUE : index;
            costs.add(cost);
            remaining.add(cost);
            assertAggregate(costs, remaining);
        }
        Collections.shuffle(remaining, random);
        while (!remaining.isEmpty()) {
            costs.remove(remaining.remove(remaining.size() - 1));
            assertAggregate(costs, remaining);
        }
        assertThrows(IllegalStateException.class, () -> costs.remove(1));
        assertAggregate(costs, remaining);
    }

    private static void assertAggregate(RefundCosts actual, List<Long> expected) {
        BigInteger sum = BigInteger.ZERO;
        BigInteger gcd = BigInteger.ZERO;
        for (long cost : expected) {
            sum = sum.add(BigInteger.valueOf(cost));
            gcd = gcd.gcd(BigInteger.valueOf(cost));
        }
        assertEquals(sum, actual.sum());
        assertEquals(gcd, actual.gcd());
    }
}
