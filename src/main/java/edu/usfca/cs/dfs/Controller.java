package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.net.ServerMessageRouter;
import io.netty.channel.ChannelHandlerContext;

public class Controller implements DFSNode {

	ServerMessageRouter messageRouter;
	static ArrayList<StorageNodeContext> storageNodes;
	HashMap<String, StorageNodeContext> nodeMap;
	private static final Logger logger = LogManager.getLogger(Controller.class);

	public Controller() {
		storageNodes = new ArrayList<StorageNodeContext>();
		nodeMap = new HashMap<>();
	}

	public void start() throws IOException {
		/* Pass a reference of the controller to our message router */
		messageRouter = new ServerMessageRouter(this);
		messageRouter.listen(13100);
		System.out.println("Listening for connections on port 13100");
	}

	public static void main(String[] args) throws IOException {
		Controller controller = new Controller();
		controller.start();

		HeartBeatChecker checker = new HeartBeatChecker(storageNodes, controller.nodeMap);

	}

	public void onMessage(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper message) {
		if (message.hasJoinRequest()) {
			String storageHost = message.getJoinRequest().getNodeName();

			logger.info("Recieved join request from " + storageHost);
			storageNodes.add(new StorageNodeContext(ctx, storageHost));
			System.err.println("Join request host: " + storageHost);
			nodeMap.put(storageHost, new StorageNodeContext(ctx, storageHost));
		} else if (message.hasHeartbeat()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			logger.debug("Recieved heartbeat from " + message.getHeartbeat().getHostname());
			// Update timestamp
			System.err.println(nodeMap.toString());
			System.err.println("HeartBeat Host: " + message.getHeartbeat().getHostname());
	
			nodeMap.get(message.getHeartbeat().getHostname()).updateTimestamp(message.getHeartbeat().getTimestamp());
		} else if (message.hasStoreRequest()) {
			/* Remove next node from the queue */
			StorageNodeContext storageNode = storageNodes.get(0);
			storageNodes.add(storageNode);
			storageNodes.remove(0);
			logger.info("Recieved request to put file on " + storageNode.getHostname() + " from client.");
			/*
			 * Write back a join request to client with hostname of the node to send chunks
			 * to
			 */

			/* Put that file in this nodes bloom filter */
			System.err.println(nodeMap.toString());
			storageNode.put(message.getStoreRequest().getFileName().getBytes());
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

		public HeartBeatChecker(ArrayList<StorageNodeContext> storageNodes,
				HashMap<String, StorageNodeContext> nodeMap) {
			this.nodeMap = nodeMap;
		}

		@Override
		public void run() {

			long currentTime = System.currentTimeMillis();

			for (Map.Entry node : nodeMap.entrySet()) {

				StorageNodeContext storageNode = (StorageNodeContext) node.getValue();
				long nodeTime = storageNode.getTimestamp();

				if (currentTime - nodeTime > 7000) {
					logger.info("Detected failure on node: " + storageNode.getHostname());
				}

			}

		}

	}
}
