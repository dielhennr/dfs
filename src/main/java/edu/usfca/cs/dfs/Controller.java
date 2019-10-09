package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.net.ServerMessageRouter;
import io.netty.channel.Channel;
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
		this.port = arguments.getInteger("-p", 13100);
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
			/*
			 * Add to PriorityQueue save the context here or nah? arent we closing the
			 * channel from SN anyway?
			 */
            StorageNodeContext thisRequest = new StorageNodeContext(ctx, storageHost);
            StorageNodeContext first; 
            StorageNodeContext second;

            synchronized(storageNodes) {
                first = storageNodes.poll();
                second = storageNodes.poll();
			    storageNodes.add(thisRequest);
            }
            
            if (first == null) {
                first = thisRequest;
            } else if (second == null) {
                second = thisRequest;
            }
            /* Write replica assignments to the SN */
            Channel chan = ctx.channel();
            ChannelFuture response = chan
                .writeAndFlush(
                        Controller.buildReplicaRequest(storageHost, first.getHostName(), second.getHostName()));

            response.syncUninterruptibly();
            
		} else if (message.hasHeartbeat()) {
			/* Update metadata */
			String hostName = message.getHeartbeat().getHostname();
			StorageMessages.Heartbeat heartbeat = message.getHeartbeat();
			/* This sucks but works for now maybe have a hashmap hostname -> wrapper */
			for (StorageNodeContext storageNode : storageNodes) {
				if (storageNode.getHostName().equals(hostName)) {
					storageNode.updateTimestamp(heartbeat.getTimestamp());
					storageNode.setFreeSpace(heartbeat.getFreeSpace());
					logger.info("Recieved heartbeat from " + hostName);
				} /* Otherwise ignore if join request not processed yet? */
			}
		} else if (message.hasStoreRequest()) {
			if (storageNodes.isEmpty()) {
				logger.error("No storage nodes in the network");
			} else {
				StorageNodeContext storageNode;
				synchronized (storageNodes) {
					storageNode = storageNodes.remove();
					storageNode.bumpRequests();
					storageNodes.add(storageNode);
				}

				logger.info("Recieved request to put file on " + storageNode.getHostName() + " from client."
						+ "This SN has processed " + storageNode.getRequests() + " requests.");
				/*
				 * Write back a store response to client with hostname of the node to send
				 * chunks to
				 */
				ChannelFuture write = ctx.pipeline().writeAndFlush(Controller
						.buildStoreResponse(message.getStoreRequest().getFileName(), storageNode.getHostName()));
				write.syncUninterruptibly();

				ctx.channel().close().syncUninterruptibly();

				/* Put that file in this nodes bloom filter */
				storageNode.put(message.getStoreRequest().getFileName().getBytes());
			}

		} else if (message.hasRetrieveFile()) {
			/*
			 * Here we could check each nodes bloom filter and then send the client the list
			 * of nodes that could have it.
			 */

		}
	}

    /**
     * Builds a replica request protobuf to assign nodes who they're supposed to replicate to
     *
     * @param host
     * @param replicaHost1
     * @param replicaHost2
     * @return
     */
	private static StorageMessages.StorageMessageWrapper buildReplicaRequest(String host, String replicaHost1,
			String replicaHost2) {

		StorageMessages.ReplicaRequest replicaRequest = StorageMessages.ReplicaRequest
                                            .newBuilder()
                                            .setHostname(host)
				                            .setReplica1(replicaHost1)
                                            .setReplica2(replicaHost2)
                                            .build();
		StorageMessages.StorageMessageWrapper wrapper = StorageMessages.StorageMessageWrapper
                                                .newBuilder()
				                                .setReplicaRequest(replicaRequest)
                                                .build();

		return wrapper;

	}

	/**
	 * Builds a store response protobuf
	 * {@link edu.usfca.cs.dfs.StorageMessages.StoreResponse}
	 * {@link edu.usfca.cs.dfs.StorageMessages.StorageMessageWrapper}
	 *
	 * @param hostname
	 * @return msgWrapper
	 */
	private static StorageMessages.StorageMessageWrapper buildStoreResponse(String fileName, String hostname) {

		StorageMessages.StoreResponse storeRequest = StorageMessages.StoreResponse.newBuilder().setHostname(hostname)
				.setFileName(fileName).build();
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setStoreResponse(storeRequest).build();

		return msgWrapper;
	}

	/**
	 * Runnable object that iterates through the queue of storage nodes every 5
	 * sedons checking timestamps
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
				for (StorageNodeContext node : storageNodes) {
					long nodeTime = node.getTimestamp();

					if (currentTime - nodeTime > 5500) {
						logger.info("Detected failure on node: " + node.getHostName());
						/* Also need to rereplicate data here. */
						/* We have to rereplicate this nodes replicas as well as its primarys */
						storageNodes.remove(node);
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
