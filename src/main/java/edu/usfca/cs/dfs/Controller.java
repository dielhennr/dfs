package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.net.MessagePipeline;
import edu.usfca.cs.dfs.net.ServerMessageRouter;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class Controller implements DFSNode {

	private ServerMessageRouter messageRouter;
	ArrayList<String> storageNodes;
	ConcurrentHashMap<String, StorageNodeContext> nodeMap;
	private static final Logger logger = LogManager.getLogger(Controller.class);

	/**
	 * Constructor
	 */
	public Controller(Bootstrap bootstrap) {
		storageNodes = new ArrayList<String>();
		nodeMap = new ConcurrentHashMap<String, StorageNodeContext>();
	}

	public void start() throws IOException {
		/* Pass a reference of the controller to our message router */
		messageRouter = new ServerMessageRouter(this);
		messageRouter.listen(13100);
		System.out.println("Listening for connections on port 13100");
	}

	public static void main(String[] args) throws IOException {
		/* Start controller to listen for message */
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		MessagePipeline pipeline = new MessagePipeline();

		Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
				.option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);

		Controller controller = new Controller(bootstrap);
		controller.start();
		HeartBeatChecker checker = new HeartBeatChecker(controller.storageNodes, controller.nodeMap);
		Thread heartbeatThread = new Thread(checker);
		heartbeatThread.run();
	}

	public void onMessage(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper message) {
		if (message.hasJoinRequest()) {
			String storageHost = message.getJoinRequest().getNodeName();

			logger.info("Recieved join request from " + storageHost);
			nodeMap.put(storageHost, new StorageNodeContext(ctx));
			storageNodes.add(storageHost);
		} else if (message.hasHeartbeat()) {
			String hostName = message.getHeartbeat().getHostname();
			StorageMessages.Heartbeat heartbeat = message.getHeartbeat();
			if (nodeMap.containsKey(hostName)) {
				nodeMap.get(hostName).updateTimestamp(heartbeat.getTimestamp());
				nodeMap.get(hostName).setFreeSpace(heartbeat.getFreeSpace());
				logger.info("Recieved heartbeat from " + hostName);
			} /* Otherwise ignore if join request not processed yet? */
		} else if (message.hasStoreRequest()) {
			/* Remove next node from the queue */
			String storageNode = storageNodes.remove(0);
			logger.info("Recieved request to put file on " + storageNode + " from client.");
			/*
			 * Write back a join request to client with hostname of the node to send chunks
			 * to
			 */
			ChannelFuture write = ctx.writeAndFlush(StorageNode.buildJoinRequest(storageNode));
			write.syncUninterruptibly();

			/* Put that file in this nodes bloom filter */
			nodeMap.get(storageNode).put(message.getStoreRequest().getFileName().getBytes());
			storageNodes.add(storageNode);

		} else if (message.hasRetrieveFile()) {
			/*
			 * Here we could check each nodes bloom filter and then send the client the list
			 * of nodes that could have it.
			 */

		}
	}

	private static class HeartBeatChecker implements Runnable {
		ConcurrentHashMap<String, StorageNodeContext> nodeMap;

		public HeartBeatChecker(ArrayList<String> storageNodes, ConcurrentHashMap<String, StorageNodeContext> nodeMap) {
			this.nodeMap = nodeMap;
		}

		@Override
		public void run() {
			while (true) {
				long currentTime = System.currentTimeMillis();
				for (String node : nodeMap.keySet()) {
					StorageNodeContext storageNode = nodeMap.get(node);
					long nodeTime = storageNode.getTimestamp();

					if (currentTime - nodeTime > 7000) {
						logger.info("Detected failure on node: " + node);
						/* Also need to rereplicate data here. */
						/* We have to rereplicate this nodes replicas as well as its primarys */
						nodeMap.remove(node);
					}

				}
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
