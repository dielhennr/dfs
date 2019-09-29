package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.net.ServerMessageRouter;
import io.netty.channel.ChannelHandlerContext;

public class Controller implements DFSNode {

	private ServerMessageRouter messageRouter;
	ArrayList<String> storageNodes;
	HashMap<String, StorageNodeContext> nodeMap;
	private static final Logger logger = LogManager.getLogger(Controller.class);

	/**
	 * Constructor
	 * 
	 */
	public Controller() {
		storageNodes = new ArrayList<String>();
		nodeMap = new HashMap<String, StorageNodeContext>();
	}

	public void start() throws IOException {
		/* Pass a reference of the controller to our message router */
		messageRouter = new ServerMessageRouter(this);
		messageRouter.listen(13100);
		System.out.println("Listening for connections on port 13100");
	}

	public static void main(String[] args) throws IOException {
		/* Start controller to listen for message */
		Controller controller = new Controller();
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
			logger.debug("Recieved heartbeat from " + message.getHeartbeat().getHostname());
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
		HashMap<String, StorageNodeContext> nodeMap;

		public HeartBeatChecker(ArrayList<String> storageNodes, HashMap<String, StorageNodeContext> nodeMap) {
			this.nodeMap = nodeMap;
		}

		@Override
		public void run() {
			while (true) {
				long currentTime = System.currentTimeMillis();
				for (String node : nodeMap.keySet()) {
					StorageNodeContext storageNode = null;
					synchronized(nodeMap) {
						storageNode = nodeMap.get(node);
					}
					long nodeTime = storageNode.getTimestamp();

					if (currentTime - nodeTime > 7000) {
						logger.info("Detected failure on node: " + node);
						nodeMap.remove(node);
					}

				}
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
