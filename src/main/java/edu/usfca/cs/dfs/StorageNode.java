package edu.usfca.cs.dfs;

import java.io.File;
import java.io.IOException;

import com.google.protobuf.ByteString;

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
	
	/** Logger to use */
	private static final Logger logger = LogManager.getLogger(StorageNode.class);

	/* Get IP address and hostname */
	private InetAddress ip;
	private String hostname;
	/* This SN's channel for comms with Controller*/
	private Channel chan;
	
	/* Big dick 5-head constructor */
	public StorageNode() throws UnknownHostException {		
		ip = InetAddress.getLocalHost();
		hostname = ip.getHostName();
		System.setProperty("hostName", hostname);
		chan = null;
	};
	
	/**
	 * Get this SN's channel
	 * 
	 * @return this SN's channel
	 */
	public Channel getChan() {
		return chan;
	}

	/**
	 * Set this SN's communication channel
	 * 
	 */
	public void setChan(Channel chan) {
		this.chan = chan;
	}

	/**
	 * Get this SN's IP addrress
	 * 
	 * @return ip
	 */
	public InetAddress getIp() {
		return ip;
	}
	
	/**
	 * Get this SN's hostname
	 * 
	 * @return hostname
	 */
	public String getHostname() {
		return hostname;
	}
	
	/**
	 * Start up a new SN by bootstrapping netty and getting basic information like hostname and ip
	 * Sends join request and subsequent heartbeats to the Controller while listening for incoming queries
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		MessagePipeline pipeline = new MessagePipeline();

		Bootstrap bootstrap = new Bootstrap()
								.group(workerGroup)
								.channel(NioSocketChannel.class)
								.option(ChannelOption.SO_KEEPALIVE, true)
								.handler(pipeline);

		ChannelFuture cf = bootstrap.connect("10.10.35.8", 4123);
		cf.syncUninterruptibly();

		StorageNode storageNode = null;
		
		try {
			storageNode = new StorageNode();
		} catch (UnknownHostException e) {
			storageNode = null;
		}
		
		if (storageNode == null) {
			logger.error("Could not retrieve hostname.");
			workerGroup.shutdownGracefully();
			System.exit(1);
		}
		
		
		StorageMessages.StorageMessageWrapper msgWrapper = buildJoinRequest(storageNode.getHostname());

		/* Send join request */
		Channel chan = cf.channel();
		storageNode.setChan(chan);
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

		/*
		 * Here is where we should start sending heartbeats to the Controller And
		 * listening for incoming messages
		 */
		
		HeartBeatRunner heartBeat = new HeartBeatRunner(storageNode);
		
		heartBeat.run();
		

		/* Don't quit until we've disconnected: */
		System.out.println("Shutting down");
		workerGroup.shutdownGracefully();

	}

	/**
	 * Builds a join request protobuf and returns it
	 * 
	 * @param hostname
	 * @return msgWrapper
	 */
	private static StorageMessages.StorageMessageWrapper buildJoinRequest(String hostname) {



		/* Store hostname in a JoinRequest protobuf */
		StorageMessages.JoinRequest joinRequest = StorageMessages.JoinRequest.newBuilder().setNodeName(hostname)
				.build();

		/* Wrapper */
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setJoinRequest(joinRequest).build();

		return msgWrapper;
	}
	
	/**
	 * HeartBeatRunner thread
	 */
	private static class HeartBeatRunner implements Runnable {

		File f;
		int requests;
		StorageNode storageNode;
		
		
		public HeartBeatRunner(StorageNode storageNode) {
			f = new File("/bigdata");
			this.requests = 0;
		}
				
		@Override
		public void run() {
			
			while(true) {
				
				long freeSpace =  f.getFreeSpace();
				
				StorageMessages.StorageMessageWrapper msgWrapper = buildHeartBeat(storageNode.getHostname(), freeSpace, requests);
				
				ChannelFuture write = storageNode.getChan().write(msgWrapper);
				
				storageNode.getChan().flush();
				logger.info("Sent heartbeat from " + storageNode.getHostname());
				write.syncUninterruptibly();
				
				
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				 
			}
			
		}
		
		/**
		 * Builds a heartbeat protobuf and returns it
		 * 
		 * @param hostname
		 * @param freeSpace
		 * @param requests
		 * @return msgWrapper
		 */
		private StorageMessages.StorageMessageWrapper buildHeartBeat(String hostname, long freeSpace, int requests) {
			
			StorageMessages.HeartBeat heartbeat = StorageMessages.HeartBeat.newBuilder().
					setFreeSpace(freeSpace).setHostname(hostname).setRequests(0).build();
			
			
			StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.
					newBuilder().setHeartbeat(heartbeat).build();
			
				return msgWrapper;
		}
		
		
		
	}
}
