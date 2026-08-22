package io.github.pzhin.sfqd;

import java.util.Objects;
import java.util.OptionalLong;

/**
 * Immutable registration policy for flow weights.
 *
 * <p>{@link #unrestricted()} preserves the full syntactic weight range in
 * {@code [1, Long.MAX_VALUE]}. It does not constrain the combined arithmetic
 * structure of registered weights, so an otherwise valid workload can reach
 * the scheduler's exact-tag representation budget after a bounded number of
 * admissions.
 *
 * <p>{@link #divisorsOf(long)} admits only weights that divide one fixed common
 * scale. Every reduced {@code cost / weight} denominator then divides that
 * scale, and addition, maximum, and rebase subtraction cannot introduce a new
 * denominator factor. This prevents pairwise-coprime weights from causing
 * denominator growth. It is not a promise that {@code NUMERIC_LIMIT} is
 * impossible: numerators and accumulated cancellation debt remain bounded by
 * the scheduler's documented representation budget.
 */
public final class WeightDomain {
    private static final WeightDomain UNRESTRICTED = new WeightDomain(OptionalLong.empty());

    private final OptionalLong commonScale;

    private WeightDomain(OptionalLong commonScale) {
        this.commonScale = commonScale;
    }

    /**
     * Returns the compatibility policy accepting every positive {@code long} weight.
     *
     * @return unrestricted weight domain
     */
    public static WeightDomain unrestricted() {
        return UNRESTRICTED;
    }

    /**
     * Returns a domain accepting exactly the positive divisors of {@code commonScale}.
     *
     * @param commonScale fixed positive common weight scale
     * @return divisor-constrained weight domain
     * @throws IllegalArgumentException if commonScale is not positive
     */
    public static WeightDomain divisorsOf(long commonScale) {
        if (commonScale <= 0L) {
            throw new IllegalArgumentException("commonScale must be positive");
        }
        return new WeightDomain(OptionalLong.of(commonScale));
    }

    /**
     * Returns the common scale when weights are divisor-constrained.
     *
     * @return common scale, or empty for the unrestricted domain
     */
    public OptionalLong commonScale() {
        return commonScale;
    }

    boolean permits(long weight) {
        return commonScale.isEmpty() || commonScale.getAsLong() % weight == 0L;
    }

    @Override
    public boolean equals(Object other) {
        return this == other
                || other instanceof WeightDomain domain
                && commonScale.equals(domain.commonScale);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonScale);
    }

    @Override
    public String toString() {
        return commonScale.isEmpty()
                ? "WeightDomain[unrestricted]"
                : "WeightDomain[divisorsOf=" + commonScale.getAsLong() + "]";
    }
}
