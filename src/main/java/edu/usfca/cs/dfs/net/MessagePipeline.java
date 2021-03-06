package edu.usfca.cs.dfs.net;

import edu.usfca.cs.dfs.DFSNode;
import edu.usfca.cs.dfs.StorageMessages;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;

public class MessagePipeline extends ChannelInitializer<SocketChannel> {

  private InboundHandler inboundHandler;

  public MessagePipeline() {
    inboundHandler = new InboundHandler();
  }

  public MessagePipeline(DFSNode node) {
    /* Finally, pass the node to the inbound handler */
    inboundHandler = new InboundHandler(node);
  }

  @Override
  public void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();

    /* Inbound: */
    /*
     * For the LengthFieldBasedFrameDecoder, set the maximum frame length (first
     * parameter) based on your maximum chunk size plus some extra space for
     * additional metadata in your proto messages. Assuming a chunk size of 100 MB,
     * we'll use 128 MB here. We use a 4-byte length field to give us 32 bits' worth
     * of frame length, which should be plenty for the future...
     */

    /*
     * Make this integer.MAXVAL, add logic in client to ensure protobuf chunks do
     * not exceed this size
     */
    pipeline.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
    pipeline.addLast(
        new ProtobufDecoder(StorageMessages.StorageMessageWrapper.getDefaultInstance()));

    /* Outbound: */
    pipeline.addLast(new LengthFieldPrepender(4));
    pipeline.addLast(new ProtobufEncoder());
    pipeline.addLast(inboundHandler);
  }
}
