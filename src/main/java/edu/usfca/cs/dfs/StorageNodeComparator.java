package edu.usfca.cs.dfs;

import java.util.Comparator;

/**
 * Compares two storage nodes based on requests processed. Used to balance load between storage
 * nodes
 *
 * @author ryandielhenn
 */
public class StorageNodeComparator implements Comparator<StorageNodeContext> {
  public int compare(StorageNodeContext node1, StorageNodeContext node2) {
    return node1.getRequests() - node2.getRequests();
  }
}
