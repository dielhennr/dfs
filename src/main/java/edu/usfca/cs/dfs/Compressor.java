package edu.usfca.cs.dfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterOutputStream;

public class Compressor {

  /**
   * Compress an array of bytes using a deflator
   *
   * @param data - data
   * @return compressed data
   * @throws IOException
   */
  public static byte[] compress(byte[] data) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DeflaterOutputStream deflater = new DeflaterOutputStream(outputStream);
    deflater.write(data);
    deflater.flush();
    deflater.close();
    return outputStream.toByteArray();
  }

  /**
   * Decompress an array of bytes with inflater
   *
   * @param data - compressed data
   * @return decompressed data
   * @throws IOException
   */
  public static byte[] decompress(byte[] data) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    InflaterOutputStream infl = new InflaterOutputStream(out);
    infl.write(data);
    infl.flush();
    infl.close();
    return out.toByteArray();
  }
}
