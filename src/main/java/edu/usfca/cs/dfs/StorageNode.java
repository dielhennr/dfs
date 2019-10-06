package edu.usfca.cs.dfs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.StorageMessages.StorageMessageWrapper;
import edu.usfca.cs.dfs.net.MessagePipeline;
import edu.usfca.cs.dfs.net.ServerMessageRouter;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class StorageNode implements DFSNode {

	private static final Logger logger = LogManager.getLogger(StorageNode.class);
	
	ServerMessageRouter messageRouter;
	
	private InetAddress ip;
	
	private String hostname;
	
	String controllerHostName;
	
	ArgumentMap arguments;

    static HeartBeatRunner heartBeat;

	public StorageNode(String[] args) throws UnknownHostException {
		ip = InetAddress.getLocalHost();
		hostname = ip.getHostName();
		arguments = new ArgumentMap(args);
		if (arguments.hasFlag("-h")) {
			controllerHostName = arguments.getString("-h");
		} else {
			System.err.println("Usage: java -cp ..... -h controllerhostname");
			System.exit(1);
		}

		/* For log4j2 */
		System.setProperty("hostName", hostname);
	};

	private String getHostname() {
		return hostname;
	}

	public static void main(String[] args) throws IOException {
		StorageNode storageNode = null;
		try {
			storageNode = new StorageNode(args);
		} catch (UnknownHostException e) {
			logger.error("Could not start storage node.");
			System.exit(1);
		} 

		StorageMessages.StorageMessageWrapper msgWrapper = StorageNode.buildJoinRequest(storageNode.getHostname());

		EventLoopGroup workerGroup = new NioEventLoopGroup();
		MessagePipeline pipeline = new MessagePipeline(storageNode, 16384);


		Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
				.option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);

		ChannelFuture cf = bootstrap.connect(storageNode.controllerHostName, 13100);
		cf.syncUninterruptibly();

		Channel chan = cf.channel();

		ChannelFuture write = chan.writeAndFlush(msgWrapper);

		if (write.syncUninterruptibly().isSuccess()) {
			logger.info("Sent join request to " + storageNode.controllerHostName);
		} else {
			logger.info("Failed join request to " + storageNode.controllerHostName);
			chan.close().syncUninterruptibly();
			workerGroup.shutdownGracefully();
			System.exit(1);
		}

		chan.close().syncUninterruptibly();

		/*
		 * Have a thread start sending heartbeats to controller. Pass bootstrap to make
		 * connections
		 **/
		heartBeat = new HeartBeatRunner(storageNode.getHostname(), storageNode.controllerHostName, bootstrap);
		Thread heartThread = new Thread(heartBeat);
		heartThread.start();
        logger.info("Started heartbeat thread.");
		storageNode.start();

	}

	public void start() throws IOException {
		/* Pass a reference of the controller to our message router */
		messageRouter = new ServerMessageRouter(this);
		messageRouter.listen(13111);
		System.out.println("Listening for connections on port 13100");
	}
	
	@Override
	public void onMessage(ChannelHandlerContext ctx, StorageMessageWrapper message) {
        if (message.hasStoreRequest()) {
            logger.info("Request to store " + message.getStoreRequest().getFileName() 
                    + " size: " + message.getStoreRequest().getFileSize());
            messageRouter.changeDecoder((int)message.getStoreRequest().getFileSize());

        } else if (message.hasStoreChunk()) {
            logger.info("recieved store chunk");
            Path path = Paths.get("/bigdata/rdielhenn", message.getStoreChunk().getFileName());
            try {
                Files.write(path, message.getStoreChunk().getData().toByteArray());
            } catch (IOException ioe) {
                logger.info("Could not write file");
            }
        }
	}

	/**
	 * Build a join request protobuf with hostname/ip
	 * 
	 * @param hostname
	 * @param ip
	 * @return the protobuf
	 */
	public static StorageMessages.StorageMessageWrapper buildJoinRequest(String hostname) {
		/* Store hostname in a JoinRequest protobuf */
		StorageMessages.JoinRequest joinRequest = StorageMessages.JoinRequest
                                                                 .newBuilder()
                                                                 .setNodeName(hostname)
				                                                 .build();

		/* Wrapper */
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                .newBuilder()
				.setJoinRequest(joinRequest)
                .build();

		return msgWrapper;
	}

	/**
	 * Build a heartbeat protobuf
	 * 
	 * @param hostname
	 * @param freeSpace
	 * @param requests
	 * @return the protobuf
	 */
	public static StorageMessages.StorageMessageWrapper buildHeartBeat(String hostname, long freeSpace, int requests) {

		StorageMessages.Heartbeat heartbeat = StorageMessages.Heartbeat.newBuilder().setFreeSpace(freeSpace)
				.setHostname(hostname).setRequests(requests).setTimestamp(System.currentTimeMillis()).build();

		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setHeartbeat(heartbeat).build();

		return msgWrapper;
	}
	
}
