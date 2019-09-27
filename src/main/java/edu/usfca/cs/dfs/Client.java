package edu.usfca.cs.dfs;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import edu.usfca.cs.dfs.StorageMessages.StorageMessageWrapper;
import edu.usfca.cs.dfs.net.MessagePipeline;
import edu.usfca.cs.dfs.net.ServerMessageRouter;

public class Client implements DFSNode{
	private static final Logger logger = LogManager.getLogger(Client.class);
	ServerMessageRouter messageRouter;
    public Client() {
    	messageRouter = new ServerMessageRouter(this);

    }

    public static void main(String[] args)
    throws IOException {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        MessagePipeline pipeline = new MessagePipeline();

        Bootstrap bootstrap = new Bootstrap()
            .group(workerGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .handler(pipeline);

        ChannelFuture cf = bootstrap.connect("10.10.35.8", 13100);
        cf.syncUninterruptibly();

        StorageMessages.SendToNode sendToNode
            = StorageMessages.SendToNode.newBuilder()
                .setFileName("my_file.txt")
                .build();

        
        StorageMessages.StorageMessageWrapper msgWrapper =
            StorageMessages.StorageMessageWrapper.newBuilder()
                .setSendToNode(sendToNode)
                .build();
        

        Channel chan = cf.channel();
        ChannelFuture write = chan.write(msgWrapper);
        chan.flush();
        write.syncUninterruptibly();
        Client client = new Client();
        client.start();
        
        
        

        /* Don't quit until we've disconnected: */
        System.out.println("Shutting down");
        workerGroup.shutdownGracefully();
    }

	private void start()
	throws IOException {
		messageRouter = new ServerMessageRouter(this);
		messageRouter.listen(13100);
		System.out.println("Listening for connections on port 13100");
	}
	
	@Override
	public void onMessage(ChannelHandlerContext ctx, StorageMessageWrapper message) {
		logger.info("Recieved permission to put file on " + message.getJoinRequest().getNodeName());
		
	}
}
