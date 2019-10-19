package edu.usfca.cs.dfs;
/**
 * Demonstrates how to calculate the entropy of a file, or basically a measure of file randomness.
 * This can be useful when determining how "compressible" something is, although it is worth noting
 * that it is just an estimate. Some algorithms may not be able to reach optimal reduction in size,
 * while domain-specific compression may be able to do even better. Caveat emptor.
 */
import java.nio.file.Files;
import java.nio.file.Paths;

public class Entropy {

  public static void main(String[] args) throws Exception {
    byte[] test = Files.readAllBytes(Paths.get(args[0]));
    double entr = entropy(test);
    System.out.println("Entropy (bits per byte): " + String.format("%.2f", entr));
    System.out.println(
        "Optimal Size Reduction: " + String.format("%.0f%%", (1 - (entr / 8)) * 100));
    System.out.println("Optimal Compression Ratio: " + String.format("%.0f:1", 8 / entr));
  }

  /**
   * Calculates the entropy per character/byte of a byte array.
   *
   * @param input array to calculate entropy of
   * @return entropy bits per byte
   */
  public static double entropy(byte[] input) {
    if (input.length == 0) {
      return 0.0;
    }

    /* Total up the occurrences of each byte */
    int[] charCounts = new int[256];
    for (byte b : input) {
      charCounts[b & 0xFF]++;
    }

    double entropy = 0.0;
    for (int i = 0; i < 256; ++i) {
      if (charCounts[i] == 0.0) {
        continue;
      }

      double freq = (double) charCounts[i] / input.length;
      entropy -= freq * (Math.log(freq) / Math.log(2));
      System.out.println(entropy);
    }

    return entropy;
  }
}
