package edu.usfca.cs.dfs;

import java.nio.file.Path;

public class ChunkWrapper {
	
	Path pathToChunk;
	long chunkID;
	long totalChunks;
	
    public ChunkWrapper(Path pathToChunk, long chunkID, long totalChunks) {
    	this.pathToChunk = pathToChunk;
    	this.chunkID = chunkID;
    	this.totalChunks = totalChunks;
    }
    
    public Path getPath() { return this.pathToChunk; }
    public long getChunkID() { return this.chunkID; }
    public long getTotalChunks() { return this.totalChunks; }
    
}
