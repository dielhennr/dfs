package edu.usfca.cs.dfs;

import com.sangupta.murmur.Murmur3;
import java.util.BitSet;

/**
 * A bloom filter implementation using Murmur3 hash and the Kirsch-Mitzenmacher optimization.
 *
 * @author ryandielhenn
 */
public class BloomFilter {

  /* Where we will store hashes of entries */
  private BitSet bitSet;

  /* Number of times to hash an entry */
  private final int numHashes;

  /* Number of entries currently in the filter */
  private int items;

  /**
   * Number of bits in our filter
   *
   * <p>bitSet.size() will be multiple of 64, we only care about the portion of the set from index 0
   * to {@link #size}
   */
  private final int size;

  public int getSize() {
    return this.size;
  }

  /**
   * Returns the {@link BitSet} of this BloomFilter
   *
   * @return the bit set of the filter
   */
  public BitSet getBitSet() {
    return this.bitSet;
  }

  /**
   * Merge one filters routing table with another
   *
   * @param filter
   */
  public void mergeFilter(BloomFilter filter) {
    this.bitSet.or(filter.getBitSet());
  }

  /**
   * Constructs a bloom filter given a size and number of hashes per entry
   *
   * @param size - size of the bloom filter in bits
   * @param hashFrequency - number of times we will hash entries
   */
  public BloomFilter(int size, int numHashes) {
    bitSet = new BitSet(size);
    this.size = size;
    this.numHashes = numHashes;
    this.items = 0;
  }

  /**
   * Puts an entry into our bloom filter by hashing it @{@link #numHashes} times
   *
   * @param data - data to store in the filter
   */
  public void put(byte[] data) {
    long hash1 = Murmur3.hash_x86_32(data, data.length, 0);
    long hash2 = Murmur3.hash_x86_32(data, data.length, hash1);
    for (int i = 0; i < numHashes; i++) {
      /* Casting here is safe since the max value of this.size is INT_MAX */
      int combinedHash = (int) ((hash1 + i * hash2) % this.size);
      bitSet.set(combinedHash);
    }

    this.items++;
  }

  /**
   * Gets a value from our bloom filter
   *
   * @param data query for filter
   * @return true if maybe in filter, false if not in the filter
   */
  public boolean get(byte[] data) {
    long hash1 = Murmur3.hash_x86_32(data, data.length, 0);
    long hash2 = Murmur3.hash_x86_32(data, data.length, hash1);
    for (int i = 0; i < numHashes; i++) {
      /* Casting here is safe since the max value of this.size is INT_MAX */
      int combinedHash = (int) ((hash1 + i * hash2) % this.size);
      boolean bit = bitSet.get(combinedHash);
      if (bit == false) {
        return false;
      }
    }
    return true;
  }

  /**
   * Computes the probability that {@link #get(byte[])} returns false positive
   *
   * @return probability that {@link #get(byte[])} returns false positive
   */
  public double falsePositiveProb() {
    if (this.size == 0 || this.items == 0) {
      return 0.0;
    }
    return Math.pow(
        1 - Math.exp((-this.numHashes) / ((double) this.size / this.items)), this.numHashes);
  }
}
