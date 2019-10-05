package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.net.ServerMessageRouter;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;

public class Controller implements DFSNode {

	private ServerMessageRouter messageRouter;
	ArrayList<String> storageNodes;
	ConcurrentHashMap<String, StorageNodeContext> nodeMap;
	private static final Logger logger = LogManager.getLogger(Controller.class);

	/**
	 * Constructor
	 */
	public Controller() {
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
		/* Start controller to listen for messages */
		Controller controller = new Controller();
		controller.start();
		HeartBeatChecker checker = new HeartBeatChecker(controller.storageNodes, controller.nodeMap);
		Thread heartbeatThread = new Thread(checker);
		heartbeatThread.run();
		
	}

	public void onMessage(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper message) {
		if (message.hasJoinRequest()) {
			/* On join request need to add node to our network */
			String storageHost = message.getJoinRequest().getNodeName();

			logger.info("Recieved join request from " + storageHost);
			nodeMap.put(storageHost, new StorageNodeContext(ctx));
			synchronized (storageNodes) {
				storageNodes.add(storageHost);
				storageNodes.notifyAll();
			}
		} else if (message.hasHeartbeat()) {
			/* Update metadata */
			String hostName = message.getHeartbeat().getHostname();
			StorageMessages.Heartbeat heartbeat = message.getHeartbeat();
			if (nodeMap.containsKey(hostName)) {
				nodeMap.get(hostName).updateTimestamp(heartbeat.getTimestamp());
				nodeMap.get(hostName).setFreeSpace(heartbeat.getFreeSpace());
				logger.info("Recieved heartbeat from " + hostName);
			} /* Otherwise ignore if join request not processed yet? */
		} else if (message.hasStoreRequest()) {
			if (storageNodes.isEmpty()) {
				logger.error("No storage nodes in the network");
			} else {
				String storageNode;
				synchronized(storageNodes) {
					storageNode = storageNodes.remove(0);
					storageNodes.add(storageNode);
				}
				
				logger.info("Recieved request to put file on " + storageNode + " from client.");
				/*
				 * Write back a join request to client with hostname of the node to send chunks
				 * to
				 */
				ChannelFuture write = ctx.pipeline().writeAndFlush(Controller.buildStoreResponse(storageNode));
				write.syncUninterruptibly();

				/* Put that file in this nodes bloom filter */
				nodeMap.get(storageNode).put(message.getStoreRequest().getFileName().getBytes());
			}

		} else if (message.hasRetrieveFile()) {
			/*
			 * Here we could check each nodes bloom filter and then send the client the list
			 * of nodes that could have it.
			 */

		}
	}

	private static StorageMessages.StorageMessageWrapper buildStoreResponse(String hostname) {

		StorageMessages.StoreResponse storeRequest = StorageMessages.StoreResponse.newBuilder()
				.setHostname(hostname)
				.build();
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setStoreResponse(storeRequest).build();

		return msgWrapper;
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
