package edu.usfca.cs.dfs;

import java.nio.file.Path;

public class ChunkWrapper {
	
	Path pathToChunk;
	long chunkID;
	long totalChunks;
    String checksum;
	
    public ChunkWrapper(Path pathToChunk, long chunkID, long totalChunks, String checksum) {
    	this.pathToChunk = pathToChunk;
    	this.chunkID = chunkID;
    	this.totalChunks = totalChunks;
        this.checksum = checksum;
    }
    
    public Path getPath() { return this.pathToChunk; }
    public long getChunkID() { return this.chunkID; }
    public long getTotalChunks() { return this.totalChunks; }
    
}
