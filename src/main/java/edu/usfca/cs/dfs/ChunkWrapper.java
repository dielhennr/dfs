package edu.usfca.cs.dfs;

import java.nio.file.Path;

public class ChunkWrapper {

  Path pathToChunk;
  String fileName;
  long chunkID;
  long totalChunks;
  String checksum;
  boolean isCompressed;

  public ChunkWrapper(
      Path pathToChunk,
      String fileName,
      long chunkID,
      long totalChunks,
      String checksum,
      boolean isCompressed) {

    this.pathToChunk = pathToChunk;
    this.fileName = fileName;
    this.chunkID = chunkID;
    this.totalChunks = totalChunks;
    this.checksum = checksum;
    this.isCompressed = isCompressed;
  }

  public Path getPath() {
    return this.pathToChunk;
  }

  public String getFileName() {
    return this.fileName;
  }

  public String getChecksum() {
    return this.checksum;
  }

  public long getChunkID() {
    return this.chunkID;
  }

  public long getTotalChunks() {
    return this.totalChunks;
  }
}
