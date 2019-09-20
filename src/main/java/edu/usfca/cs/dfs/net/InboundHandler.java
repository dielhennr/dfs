package edu.usfca.cs.dfs.net;

import java.net.InetSocketAddress;

import edu.usfca.cs.dfs.StorageMessages;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class InboundHandler
extends SimpleChannelInboundHandler<StorageMessages.StorageMessageWrapper> {

    public InboundHandler() { }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        /* A connection has been established */
        InetSocketAddress addr
            = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection established: " + addr);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        /* A channel has been disconnected */
        InetSocketAddress addr
            = (InetSocketAddress) ctx.channel().remoteAddress();
        System.out.println("Connection lost: " + addr);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
    throws Exception {
        /* Writable status of the channel changed */
    }

    @Override
    public void channelRead0(
            ChannelHandlerContext ctx,
            StorageMessages.StorageMessageWrapper msg) {

        System.out.println(msg.hasJoinRequest());
        
        if (msg.hasStoreChunkMsg()) {
			StorageMessages.StoreChunk storeChunkMsg
				= msg.getStoreChunkMsg();
			System.out.println("Storing file name: "
					+ storeChunkMsg.getFileName());
        } else if (msg.hasJoinRequest()){
			StorageMessages.JoinRequest joinRequest
				= msg.getJoinRequest();
			System.out.println("Request to join network from: "
					+ joinRequest.getNodeName());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
    }
}
