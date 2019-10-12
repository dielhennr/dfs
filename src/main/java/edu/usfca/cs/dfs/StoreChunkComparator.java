package edu.usfca.cs.dfs;

import java.util.Comparator;

/**
 * Compares two storage nodes based on requests processed. Used to balance load
 * between storage nodes
 *
 * @author ryandielhenn
 */
public class StoreChunkComparator implements Comparator<StorageMessages.StoreChunk> {
	public int compare(StorageMessages.StoreChunk chunk1, StorageMessages.StoreChunk chunk2) {
        long difference = chunk1.getChunkId() - chunk2.getChunkId();
        int out = 0;
        if (difference < 0) {
            out = -1;
        } else if (difference > 0) {
            out = 1;
        }
        return out;
	}
}
