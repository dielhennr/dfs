package edu.usfca.cs.dfs;

import io.netty.channel.ChannelHandlerContext;

/**
 * An interface for different nodes in our DFS
 *
 * <p>Allows the inbound handler of a node to process messages regardless of the Nodes job in the
 * network
 */
public interface DFSNode {
  void onMessage(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper message);
}
