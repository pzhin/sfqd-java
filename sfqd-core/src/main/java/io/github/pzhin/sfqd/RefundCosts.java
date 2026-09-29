package io.github.pzhin.sfqd;

import java.math.BigInteger;
import java.util.Objects;

/** AVL multiset of queued costs; retains only live costs, their sum, and their gcd. */
final class RefundCosts {
    private Node root;
    private BigInteger sum = BigInteger.ZERO;

    BigInteger sum() {
        return sum;
    }

    BigInteger gcd() {
        return BigInteger.valueOf(gcd(root));
    }

    void add(long cost) {
        root = add(root, cost);
        sum = sum.add(BigInteger.valueOf(cost));
    }

    void remove(long cost) {
        root = remove(root, cost, false);
        sum = sum.subtract(BigInteger.valueOf(cost));
    }

    private static Node add(Node node, long cost) {
        if (node == null) {
            return new Node(cost);
        }
        if (cost < node.cost) {
            node.left = add(node.left, cost);
        } else if (cost > node.cost) {
            node.right = add(node.right, cost);
        } else {
            node.count++;
        }
        return balance(node);
    }

    private static Node remove(Node node, long cost, boolean all) {
        if (node == null) {
            throw new IllegalStateException("queued cost missing");
        }
        if (cost < node.cost) {
            node.left = remove(node.left, cost, all);
        } else if (cost > node.cost) {
            node.right = remove(node.right, cost, all);
        } else if (!all && node.count > 1) {
            node.count--;
        } else {
            if (node.left == null) {
                return node.right;
            }
            if (node.right == null) {
                return node.left;
            }
            Node successor = node.right;
            while (successor.left != null) {
                successor = successor.left;
            }
            node.cost = successor.cost;
            node.count = successor.count;
            node.right = remove(node.right, successor.cost, true);
        }
        return balance(node);
    }

    private static Node balance(Node node) {
        update(node);
        if (height(node.left) - height(node.right) > 1) {
            Node left = Objects.requireNonNull(node.left);
            if (height(left.left) < height(left.right)) {
                node.left = rotateLeft(node.left);
            }
            return rotateRight(node);
        }
        if (height(node.right) - height(node.left) > 1) {
            Node right = Objects.requireNonNull(node.right);
            if (height(right.right) < height(right.left)) {
                node.right = rotateRight(node.right);
            }
            return rotateLeft(node);
        }
        return node;
    }

    private static Node rotateLeft(Node node) {
        Node promoted = Objects.requireNonNull(node.right);
        node.right = promoted.left;
        promoted.left = node;
        update(node);
        update(promoted);
        return promoted;
    }

    private static Node rotateRight(Node node) {
        Node promoted = Objects.requireNonNull(node.left);
        node.left = promoted.right;
        promoted.right = node;
        update(node);
        update(promoted);
        return promoted;
    }

    private static void update(Node node) {
        node.height = 1 + Math.max(height(node.left), height(node.right));
        node.gcd = gcd(node.cost, gcd(gcd(node.left), gcd(node.right)));
    }

    private static int height(Node node) {
        return node == null ? 0 : node.height;
    }

    private static long gcd(Node node) {
        return node == null ? 0L : node.gcd;
    }

    private static long gcd(long first, long second) {
        while (second != 0L) {
            long remainder = first % second;
            first = second;
            second = remainder;
        }
        return first;
    }

    private static final class Node {
        private long cost;
        private int count = 1;
        private int height = 1;
        private long gcd;
        private Node left;
        private Node right;

        private Node(long cost) {
            this.cost = cost;
            gcd = cost;
        }
    }
}
