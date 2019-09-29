package edu.usfca.cs.dfs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StorageNode implements DFSNode {

	private static final Logger logger = LogManager.getLogger(StorageNode.class);
	ServerMessageRouter messageRouter;
	private InetAddress ip = null;
	private String hostname = null;

	public StorageNode() throws UnknownHostException {
		ip = InetAddress.getLocalHost();
		hostname = ip.getHostName();
	};

	private InetAddress getIp() {
		return ip;
	}

	private String getHostname() {
		return hostname;
	}

	public static void main(String[] args) throws IOException {

		StorageNode storageNode = null;
		try {
			storageNode = new StorageNode();
		} catch (UnknownHostException e) {
			logger.error("Could not start storage node.");
			System.exit(1);
		}

		StorageMessages.StorageMessageWrapper msgWrapper = buildJoinRequest(storageNode.getHostname());

		EventLoopGroup workerGroup = new NioEventLoopGroup();
		MessagePipeline pipeline = new MessagePipeline();

		Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
				.option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);

		ChannelFuture cf = bootstrap.connect("10.10.35.8", 13100);
		cf.syncUninterruptibly();

		Channel chan = cf.channel();

		ChannelFuture write = chan.write(msgWrapper);
		logger.info("Sent join request to 10.10.35.8");
		chan.flush();
		write.syncUninterruptibly();

		/**
		 * Shutdown the worker group and exit if join request was not successful
		 */
		if (write.isDone() && write.isSuccess()) {
			logger.info("Join request to 10.10.35.8 successful.");
		} else if (write.isDone() && (write.cause() != null)) {
			logger.warn("Join request to 10.10.35.8 failed.");
			workerGroup.shutdownGracefully();
			System.exit(1);
		} else if (write.isDone() && write.isCancelled()) {
			logger.warn("Join request to 10.10.35.8 cancelled.");
			workerGroup.shutdownGracefully();
			System.exit(1);
		}

		/* Have a thread start sending heartbeats to controller */
		HeartBeatRunner heartBeat = new HeartBeatRunner(storageNode.getHostname());
		Thread thread = new Thread(heartBeat);
		thread.run();

		storageNode.start();

		/* Don't quit until we've disconnected: */
		System.out.println("Shutting down");
		workerGroup.shutdownGracefully();

	}

	public void start() throws IOException {
		/* Pass a reference of the controller to our message router */
		messageRouter = new ServerMessageRouter(this);
		messageRouter.listen(13100);
		System.out.println("Listening for connections on port 13100");
	}

	@Override
	public void onMessage(ChannelHandlerContext ctx, StorageMessageWrapper message) {
		if (message.hasStoreChunk()) {
			StorageMessages.StoreChunk chunk = message.getStoreChunk();
			File fileStore = new File("/bigdata/" + chunk.getFileName() + "_chunk" + chunk.getChunkId());
			try {
				fileStore.createNewFile();
				BufferedWriter writer = new BufferedWriter(new FileWriter(fileStore));
				writer.write(chunk.getData().toString());
				writer.close();
			} catch (IOException e) {
				logger.error("Could not write " + chunk.getFileName() + "_chunk" + chunk.getChunkId() + " to disk.");
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
	static StorageMessages.StorageMessageWrapper buildJoinRequest(String hostname) {
		/* Store hostname in a JoinRequest protobuf */
		StorageMessages.JoinRequest joinRequest = StorageMessages.JoinRequest.newBuilder().setNodeName(hostname)
				.build();

		/* Wrapper */
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setJoinRequest(joinRequest).build();

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
	private static StorageMessages.StorageMessageWrapper buildHeartBeat(String hostname, long freeSpace, int requests) {

		StorageMessages.Heartbeat heartbeat = StorageMessages.Heartbeat.newBuilder().setFreeSpace(freeSpace)
				.setHostname(hostname).setRequests(0).build();

		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setHeartbeat(heartbeat).build();

		return msgWrapper;
	}

	/**
	 * Runnable object that sends heartbeats to the Controller every 5 seconds
	 */
	private static class HeartBeatRunner implements Runnable {

		String hostname;
		int requests;
		File f;

		public HeartBeatRunner(String hostname) {
			f = new File("/bigdata");
			this.hostname = hostname;
			this.requests = 0;
		}

		@Override
		public void run() {

			while (true) {

				long freeSpace = f.getFreeSpace();

				StorageMessages.StorageMessageWrapper msgWrapper = StorageNode.buildHeartBeat(hostname, freeSpace,
						requests);

				EventLoopGroup workerGroup = new NioEventLoopGroup();
				MessagePipeline pipeline = new MessagePipeline();

				Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
						.option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);

				ChannelFuture cf = bootstrap.connect("10.10.35.8", 13100);
				cf.syncUninterruptibly();

				Channel chan = cf.channel();

				ChannelFuture write = chan.write(msgWrapper);

				chan.flush();
				write.syncUninterruptibly();
				cf.channel().disconnect().syncUninterruptibly();

				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}

		}

	}

}
