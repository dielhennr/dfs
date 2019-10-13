package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.net.ServerMessageRouter;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;

public class Controller implements DFSNode {

	private ServerMessageRouter messageRouter;
	PriorityQueue<StorageNodeContext> storageNodes;
	private static final Logger logger = LogManager.getLogger(Controller.class);

	ArgumentMap arguments;
	int port;

	/** Constructor */
	public Controller(String[] args) {
		storageNodes = new PriorityQueue<>(new StorageNodeComparator());
		this.arguments = new ArgumentMap(args);
		this.port = arguments.getInteger("-p", 13112);
	}

	public void start() throws IOException {
		/* Pass a reference of the controller to our message router */
		messageRouter = new ServerMessageRouter(this);
		messageRouter.listen(this.port);
		System.out.println("Listening for connections on port 13100");
	}

	public static void main(String[] args) throws IOException {
		/* Start controller to listen for messages */
		Controller controller = new Controller(args);
		controller.start();
		HeartBeatChecker checker = new HeartBeatChecker(controller.storageNodes);
		Thread heartbeatThread = new Thread(checker);
		heartbeatThread.run();
	}

	/* Controller inbound duties */
	public void onMessage(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper message) {
		if (message.hasJoinRequest()) {
			/* On join request need to add node to our network */
			String storageHost = message.getJoinRequest().getNodeName();
			logger.info("Recieved join request from " + storageHost);

			/* Add new node to priority queue */
			StorageNodeContext thisRequest = new StorageNodeContext(storageHost);
			synchronized (storageNodes) {
				storageNodes.add(thisRequest);
			}

		} else if (message.hasHeartbeat()) {
			/* Update metadata */
			String hostName = message.getHeartbeat().getHostname();
			StorageMessages.Heartbeat heartbeat = message.getHeartbeat();
			/* This sucks but works for now maybe have a hashmap hostname -> wrapper */
			synchronized (storageNodes) {
				for (StorageNodeContext storageNode : storageNodes) {
					if (storageNode.getHostName().equals(hostName)) {
						storageNode.updateTimestamp(heartbeat.getTimestamp());
						storageNode.setFreeSpace(heartbeat.getFreeSpace());
					} /* Otherwise ignore if join request not processed yet? */
				}
			}
		} else if (message.hasStoreRequest()) {
			if (storageNodes.size() < 3) {
				logger.error("Not enough storage nodes in the network");
			} else {

				synchronized (storageNodes) {
					/* StorageNode with least requests processed should be at the top */
					StorageNodeContext storageNodePrimary = storageNodes.poll();
					StorageNodeContext replicaAssignment1 = null;
					StorageNodeContext replicaAssignment2 = null;

					/*
					 * If the first node in the queue doesn't have assignments pull from queue,
					 * assign and then put the assignee back in
					 */
					if (storageNodePrimary.replicaAssignment1 == null) {
						replicaAssignment1 = storageNodes.poll();
						storageNodePrimary.replicaAssignment1 = replicaAssignment1;
						storageNodes.add(replicaAssignment1);
					}
					/* Same here for second assignment */
					if (storageNodePrimary.replicaAssignment2 == null) {
						replicaAssignment2 = storageNodes.poll();
						storageNodePrimary.replicaAssignment2 = replicaAssignment2;
						storageNodes.add(replicaAssignment2);
					}
					/* Bump requests of all assignments since we are about to send a file */
					storageNodePrimary.bumpRequests();
					storageNodePrimary.replicaAssignment1.bumpRequests();
					storageNodePrimary.replicaAssignment2.bumpRequests();

					/* Put that file in this nodes bloom filter */
					storageNodePrimary.put(message.getStoreRequest().getFileName().getBytes());

					/* Put primary back in the queue */
					storageNodes.add(storageNodePrimary);

					/**
					 * Write back a store response to client with hostname of the primary node to
					 * send chunks to. This node will handle replication with the ReplicaAssignments
					 * protobuf nested in this store response
					 */
					ChannelFuture write = ctx.pipeline()
							.writeAndFlush(Builders.buildStoreResponse(message.getStoreRequest().getFileName(),
									storageNodePrimary.getHostName(),
									storageNodePrimary.replicaAssignment1.getHostName(),
									storageNodePrimary.replicaAssignment2.getHostName()));
					write.syncUninterruptibly();

					/* Log controllers response */
					logger.info("Approving request to put file on " + storageNodePrimary.getHostName() + " from client."
							+ "This SN has processed " + storageNodePrimary.getRequests() + " requests.");
					logger.info("Replicating to " + storageNodePrimary.replicaAssignment1.getHostName() + " and "
							+ storageNodePrimary.replicaAssignment2.getHostName());
					ctx.channel().close().syncUninterruptibly();
				}

			}

		} else if (message.hasRetrieveFile()) {
			/*
			 * Here we could check each nodes bloom filter and then send the client the list
			 * of nodes that could have it.
			 */

			String fileName = message.getRetrieveFile().getFileName();

			ArrayList<String> possibleNodes = new ArrayList<>();

			String hosts = "";
			/* Find all storagenodes that might have the file */
			synchronized (storageNodes) {
				Iterator<StorageNodeContext> iter = storageNodes.iterator();
				while (iter.hasNext()) {
					StorageNodeContext node = iter.next();
					if (node.mightBeThere(fileName.getBytes())) {
						possibleNodes.add(node.getHostName());
						hosts += node.getHostName() + " ";
					}
				}
			}

			ChannelFuture write = ctx.pipeline().writeAndFlush(Builders.buildPossibleRetrievalHosts(hosts, fileName));
			write.syncUninterruptibly();
			logger.info("Possible hosts for file: " + fileName + " ---> " + hosts);
			ctx.channel().close().syncUninterruptibly();
		}
	}

	/**
	 * Runnable object that iterates through the queue of storage nodes every 5
	 * seconds checking timestamps
	 */
	private static class HeartBeatChecker implements Runnable {
		PriorityQueue<StorageNodeContext> storageNodes;

		public HeartBeatChecker(PriorityQueue<StorageNodeContext> storageNodes) {
			this.storageNodes = storageNodes;
		}

		@Override
		public void run() {
			while (true) {
				long currentTime = System.currentTimeMillis();
				/*
				 * This is throwing concurrent mod exception when detecting node failures at
				 * line 156 synchronized doesn't fix
				 */
				synchronized (storageNodes) {

					Iterator<StorageNodeContext> iter = storageNodes.iterator();
					while (iter.hasNext()) {
						StorageNodeContext node = iter.next();
						long nodeTime = node.getTimestamp();

						if (currentTime - nodeTime > 5500) {
							logger.info("Detected failure on node: " + node.getHostName());
							/* Also need to rereplicate data here. */
							/* We have to rereplicate this nodes replicas as well as its primarys */
							iter.remove();
						}
					}

				}
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

        public void rereplicate(StorageNodeContext node) {
            
        }
	}
}
