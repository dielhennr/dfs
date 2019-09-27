package edu.usfca.cs.dfs;

import io.netty.channel.ChannelHandlerContext;

public interface DFSNode {
	
	 void onMessage(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper message);
}
