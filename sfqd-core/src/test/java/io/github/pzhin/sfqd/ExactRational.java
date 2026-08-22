package io.github.pzhin.sfqd;

import java.math.BigInteger;
import java.util.Objects;

final class ExactRational implements Comparable<ExactRational> {
    static final ExactRational ZERO = new ExactRational(BigInteger.ZERO, BigInteger.ONE);
    static final ExactRational ONE = new ExactRational(BigInteger.ONE, BigInteger.ONE);

    private final BigInteger numerator;
    private final BigInteger denominator;

    private ExactRational(BigInteger numerator, BigInteger denominator) {
        this.numerator = numerator;
        this.denominator = denominator;
    }

    static ExactRational of(long numerator, long denominator) {
        return of(BigInteger.valueOf(numerator), BigInteger.valueOf(denominator));
    }

    static ExactRational of(BigInteger numerator, BigInteger denominator) {
        Objects.requireNonNull(numerator, "numerator");
        Objects.requireNonNull(denominator, "denominator");
        if (numerator.signum() < 0) {
            throw new IllegalArgumentException("numerator must be non-negative");
        }
        if (denominator.signum() <= 0) {
            throw new IllegalArgumentException("denominator must be positive");
        }
        if (numerator.signum() == 0) {
            return ZERO;
        }
        BigInteger divisor = numerator.gcd(denominator);
        return new ExactRational(numerator.divide(divisor), denominator.divide(divisor));
    }

    BigInteger numerator() {
        return numerator;
    }

    BigInteger denominator() {
        return denominator;
    }

    ExactRational add(ExactRational other) {
        Objects.requireNonNull(other, "other");
        BigInteger sum = numerator.multiply(other.denominator)
                .add(other.numerator.multiply(denominator));
        return of(sum, denominator.multiply(other.denominator));
    }

    ExactRational subtract(ExactRational other) {
        Objects.requireNonNull(other, "other");
        BigInteger difference = numerator.multiply(other.denominator)
                .subtract(other.numerator.multiply(denominator));
        if (difference.signum() < 0) {
            throw new ArithmeticException("negative rational result");
        }
        return of(difference, denominator.multiply(other.denominator));
    }

    ExactRational max(ExactRational other) {
        return compareTo(other) >= 0 ? this : other;
    }

    @Override
    public int compareTo(ExactRational other) {
        Objects.requireNonNull(other, "other");
        return numerator.multiply(other.denominator)
                .compareTo(other.numerator.multiply(denominator));
    }

    @Override
    public boolean equals(Object other) {
        return this == other
                || other instanceof ExactRational rational
                && numerator.equals(rational.numerator)
                && denominator.equals(rational.denominator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numerator, denominator);
    }

    @Override
    public String toString() {
        return numerator + "/" + denominator;
    }
}
