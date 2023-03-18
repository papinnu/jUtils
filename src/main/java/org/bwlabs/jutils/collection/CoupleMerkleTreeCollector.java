package org.bwlabs.jutils.collection;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * Collects {@code byte[]} elements sequentially as a pair - current element and the next one. If there is no next element for the last one it will
 * be collected twice. For each pair, the hash is calculated and the result collected in a list and returned when finished - by the
 * finisher {@code Function}.
 *
 * @author Plamen Uzunov
 */
public class CoupleMerkleTreeCollector implements Collector<byte[], List<byte[]>, List<byte[]>> {

    private final MessageDigest md;
    private byte[] hashTwo;

    public CoupleMerkleTreeCollector(MessageDigest md) {
        this.md = md;
    }

    public static CoupleMerkleTreeCollector toCollector(MessageDigest md) {
        return new CoupleMerkleTreeCollector(md);
    }

    /**
     * Creates a new instance of {@code List<byte[]>} to collect {@code byte[]} elements.
     *
     * @return a {@code List<byte[]>} used to collect {@code byte[]} elements.
     */
    @Override
    public Supplier<List<byte[]>> supplier() {
        return ArrayList::new;
    }

    /**
     * Accumulates {@code byte[]} elements in the order to be collected together with the next  one. If there is no next element for
     * the last one it will be collected twice. For each pair, the hash is calculated and the result collected in a list and returned
     * when finished - by the finisher.
     *
     * @return {@code BiConsumer} that will be called to accumulate {@code byte[]} elements.
     */
    @Override
    public BiConsumer<List<byte[]>, byte[]> accumulator() {
        return (list, bytes) -> {
            byte[] hashOne = hashTwo;
            hashTwo = bytes;
            if (hashOne != null) {
                list.add(calcHash(hashOne, hashTwo));
                hashTwo = null;
            }
        };
    }

    /**
     * The Merkle Tree implementation does not support parallel computing.
     */
    @Override
    public BinaryOperator<List<byte[]>> combiner() {
        return (list1, list2) -> list1;
    }

    /**
     * Returns the collected elements of this Merkle Tree calculation implementation.
     *
     * @return {@code Function} that returns the collected elements of this Merkle Tree calculation implementation.
     */
    @Override
    public Function<List<byte[]>, List<byte[]>> finisher() {
        return (list) -> {
            if (hashTwo != null) {
                list.add(calcHash(hashTwo, hashTwo));
                hashTwo = null;
            }
            return Collections.unmodifiableList(list);
        };
    }

    /**
     * No characteristics are supported.
     *
     * @return supported characteristics by this implementation.
     */
    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }

    /**
     * Calculates the Merkle Tree hash for the given two elements.
     *
     * @param first  element
     * @param second element
     * @return calculated hash
     */
    private byte[] calcHash(byte[] first, byte[] second) {
        md.reset();
        md.update(first);
        md.update(second);
        return md.digest();
    }

}
