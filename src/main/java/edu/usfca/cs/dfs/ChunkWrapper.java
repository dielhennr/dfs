package edu.usfca.cs.dfs;

import java.nio.file.Path;

public class ChunkWrapper {
	
	Path pathToChunk;
	int totalChunks;
	int chunkID;
	
	
    public ChunkWrapper(Path pathToChunk, int totalChunks, int chunkID) {
    	this.pathToChunk = pathToChunk;
    	this.totalChunks = totalChunks;
    	this.chunkID = chunkID;
    }
    
    public Path getPath() { return this.pathToChunk; }
    public int chunkID() { return this.chunkID; }
    
    
    
}
