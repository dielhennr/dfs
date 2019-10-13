package edu.usfca.cs.dfs;

import java.util.Comparator;

public class ChunkWrapperComparator implements Comparator<ChunkWrapper> {

	public int compare(ChunkWrapper chunk1, ChunkWrapper chunk2) {
		long difference = chunk1.getChunkID() - chunk2.getChunkID();
		int out = 0;
		if (difference < 0) {
			out = -1;
		} else if (difference > 0) {
			out = 1;
		}
		return out;
	}
    
}
