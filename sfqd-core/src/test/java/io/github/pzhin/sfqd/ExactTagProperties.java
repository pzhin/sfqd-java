package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Provide;
import net.jqwik.api.Property;
import net.jqwik.api.Tuple;
import net.jqwik.api.constraints.LongRange;

final class ExactTagProperties {
    @Property(tries = 1_000)
    void smallArithmeticAgreesWithIndependentOracle(
            @ForAll @LongRange(min = 1L, max = 1_000_000L) long firstCost,
            @ForAll @LongRange(min = 1L, max = 10_000L) long firstWeight,
            @ForAll @LongRange(min = 1L, max = 1_000_000L) long secondCost,
            @ForAll @LongRange(min = 1L, max = 10_000L) long secondWeight)
            throws NumericLimitException {
        assertArithmeticMatchesOracle(firstCost, firstWeight, secondCost, secondWeight);
    }

    @Property(tries = 1_000)
    void generalLongArithmeticAgreesWithIndependentOracle(
            @ForAll("positiveLongs") long firstCost,
            @ForAll("positiveLongs") long firstWeight,
            @ForAll("positiveLongs") long secondCost,
            @ForAll("positiveLongs") long secondWeight) throws NumericLimitException {
        assertArithmeticMatchesOracle(firstCost, firstWeight, secondCost, secondWeight);
    }

    private static void assertArithmeticMatchesOracle(
            long firstCost,
            long firstWeight,
            long secondCost,
            long secondWeight) throws NumericLimitException {
        ExactTag first = ExactTag.fromCostAndWeight(firstCost, firstWeight);
        ExactTag second = ExactTag.fromCostAndWeight(secondCost, secondWeight);
        ExactRational oracleFirst = ExactRational.of(firstCost, firstWeight);
        ExactRational oracleSecond = ExactRational.of(secondCost, secondWeight);

        assertSameValue(oracleFirst.add(oracleSecond), first.add(second));
        assertEquals(Integer.signum(oracleFirst.compareTo(oracleSecond)),
                Integer.signum(first.compareExact(second)));
        assertSameValue(oracleFirst.max(oracleSecond), first.max(second));

        if (oracleFirst.compareTo(oracleSecond) >= 0) {
            assertSameValue(oracleFirst.subtract(oracleSecond), first.subtractNonNegative(second));
        } else {
            assertSameValue(oracleSecond.subtract(oracleFirst), second.subtractNonNegative(first));
        }
    }

    @Property(tries = 1_000)
    void comparatorObeysAntisymmetryTransitivityAndEquality(
            @ForAll("tags") ExactTag first,
            @ForAll("tags") ExactTag second,
            @ForAll("tags") ExactTag third) throws NumericLimitException {
        int firstSecond = Integer.signum(first.compareExact(second));
        int secondFirst = Integer.signum(second.compareExact(first));

        assertEquals(-firstSecond, secondFirst);
        assertEquals(first.equals(second), firstSecond == 0);
        if (first.compareExact(second) <= 0 && second.compareExact(third) <= 0) {
            assertTrue(first.compareExact(third) <= 0);
        }
    }

    @Property(tries = 1_000)
    void positiveIncrementAlwaysMakesFinishGreaterThanStart(
            @ForAll("tags") ExactTag start,
            @ForAll("positiveLongs") long cost,
            @ForAll("positiveLongs") long weight) throws NumericLimitException {
        ExactTag finish = start.add(ExactTag.fromCostAndWeight(cost, weight));

        assertTrue(finish.compareExact(start) > 0);
    }

    @Provide
    Arbitrary<Long> positiveLongs() {
        return Arbitraries.longs().between(1L, Long.MAX_VALUE);
    }

    @Provide
    Arbitrary<ExactTag> tags() {
        return CombinatorsSupport.combinePositiveLongs(positiveLongs())
                .map(values -> createTag(values.get1(), values.get2()));
    }

    private static ExactTag createTag(long numerator, long denominator) {
        try {
            return ExactTag.fromCostAndWeight(numerator, denominator);
        } catch (NumericLimitException impossibleForLongComponents) {
            throw new AssertionError("positive long components must fit the persistent budget",
                    impossibleForLongComponents);
        }
    }

    private static void assertSameValue(ExactRational expected, ExactTag actual) {
        assertEquals(expected.numerator(), actual.numerator());
        assertEquals(expected.denominator(), actual.denominator());
    }

    private static final class CombinatorsSupport {
        private CombinatorsSupport() {
        }

        static Arbitrary<Tuple.Tuple2<Long, Long>> combinePositiveLongs(Arbitrary<Long> values) {
            return net.jqwik.api.Combinators.combine(values, values).as(Tuple::of);
        }
    }
}
