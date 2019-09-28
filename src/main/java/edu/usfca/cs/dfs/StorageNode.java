package edu.usfca.cs.dfs;

import java.io.File;
import java.io.IOException;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import edu.usfca.cs.dfs.net.MessagePipeline;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StorageNode {

	private static final Logger logger = LogManager.getLogger(StorageNode.class);

	public StorageNode() {
	};

	public static void main(String[] args) throws IOException {

		InetAddress ip = null;
		String hostname = null;
		try {
			ip = InetAddress.getLocalHost();
			hostname = ip.getHostName();
			System.setProperty("hostName", hostname);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		StorageMessages.StorageMessageWrapper msgWrapper = buildJoinRequest(hostname);

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
		HeartBeatRunner heartBeat = new HeartBeatRunner(hostname);
		Thread thread = new Thread(heartBeat);
		thread.run();

		/* Don't quit until we've disconnected: */
		System.out.println("Shutting down");
		workerGroup.shutdownGracefully();

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

				StorageMessages.StorageMessageWrapper msgWrapper = StorageNode.buildHeartBeat(hostname, freeSpace, requests);

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
