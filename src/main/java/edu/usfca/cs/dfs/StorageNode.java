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

	private static final Logger logger = LogManager.getLogger(StorageNode.class);

	public StorageNode() {
	};

	public static void main(String[] args) throws IOException {

		
		InetAddress ip;
		String hostname = null;
		try {
			ip = InetAddress.getLocalHost();
			hostname = ip.getHostName();
			System.setProperty("hostName", hostname);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		
		StorageMessages.StorageMessageWrapper msgWrapper = buildJoinRequest(hostname);
		
		Connect conn = new Connect("10.10.35.8");
		Channel chan = conn.connect();
		
		
		ChannelFuture write = chan.write(msgWrapper);
		logger.info("Sent join request to 10.10.35.8");
		chan.flush();
		write.syncUninterruptibly();

		/**
 * 		 * Shutdown the worker group and exit if join request was not successful
 * 		 		 */
		if (write.isDone() && write.isSuccess()) {
			logger.info("Join request to 10.10.35.8 successful.");
		} else if (write.isDone() && (write.cause() != null)) {
			logger.warn("Join request to 10.10.35.8 failed.");
			conn.workerGroup.shutdownGracefully();
			System.exit(1);
		} else if (write.isDone() && write.isCancelled()) {
			logger.warn("Join request to 10.10.35.8 cancelled.");
			conn.workerGroup.shutdownGracefully();
			System.exit(1);
		}

		/*
 * 		 * Here is where we should start sending heartbeats to the Controller And
 * 		 		 * listening for incoming messages
 * 		 		 		 */
		
		HeartBeatRunner heartBeat = new HeartBeatRunner(hostname);
		Thread thread = new Thread(heartBeat);
		thread.run();
		

		/* Don't quit until we've disconnected: */
		System.out.println("Shutting down");
		conn.workerGroup.shutdownGracefully();

	}
	

	private static StorageMessages.StorageMessageWrapper buildJoinRequest(String hostname) {



		/* Store hostname in a JoinRequest protobuf */
		StorageMessages.JoinRequest joinRequest = StorageMessages.JoinRequest.newBuilder().setNodeName(hostname)
				.build();

		/* Wrapper */
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setJoinRequest(joinRequest).build();

		return msgWrapper;
	}
	
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
			
			while(true) {
				
				long freeSpace =  f.getFreeSpace();
				
				
				StorageMessages.StorageMessageWrapper msgWrapper = buildHeartBeat(hostname, freeSpace, requests);
				
				
				Connect conn = new Connect("10.10.35.8");
				Channel chan = conn.connect();
						
				
				ChannelFuture write = chan.write(msgWrapper);
				logger.info("Recieved heartbeat from " + hostname);
				
				chan.flush();
				write.syncUninterruptibly();
				conn.shutdown();
				
				
				
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				 
			}
			
		}
		
		private StorageMessages.StorageMessageWrapper buildHeartBeat(String hostname, long freeSpace, int requests) {
			
			StorageMessages.HeartBeat heartbeat = StorageMessages.HeartBeat.newBuilder().
					setFreeSpace(freeSpace).setHostname(hostname).setRequests(0).build();
			
			
			StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.
					newBuilder().setHeartbeat(heartbeat).build();
			
				return msgWrapper;
		}
		
		
		
	}
}

