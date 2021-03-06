package edu.usfca.cs.dfs.net;

import edu.usfca.cs.dfs.DFSNode;
import edu.usfca.cs.dfs.StorageMessages;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class InboundHandler
    extends SimpleChannelInboundHandler<StorageMessages.StorageMessageWrapper> {

  /* Now we can interface with the nodes when different channel events occur */
  public DFSNode node;

  public InboundHandler() {}

  public InboundHandler(DFSNode node) {
    this.node = node;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    /* A connection has been established */
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    /* A channel has been disconnected */
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
    /* Writable status of the channel changed */
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper msg) {
    node.onMessage(ctx, msg);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
  }
}
