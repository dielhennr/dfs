package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.net.MessagePipeline;
import edu.usfca.cs.dfs.net.ServerMessageRouter;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

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
		System.out.println("Listening for connections on port " + this.port);
	}

	public static void main(String[] args) throws IOException {
		/* Start controller to listen for messages */
		Controller controller = new Controller(args);
		controller.start();
		HeartBeatChecker checker = new HeartBeatChecker(controller.storageNodes);
		Thread heartbeatThread = new Thread(checker);
		heartbeatThread.run();
		System.out.println("Uhhh running?");
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
						Iterator<StorageNodeContext> iter = storageNodes.iterator();
						StorageNodeContext snctx = iter.next();
						while (snctx.equals(storageNodePrimary.replicaAssignment2)) {
							snctx = iter.next();
							
						}
						replicaAssignment1 = snctx;
						iter.remove();
						storageNodePrimary.replicaAssignment1 = replicaAssignment1;

					}
					/* Same here for second assignment */
					if (storageNodePrimary.replicaAssignment2 == null) {
						Iterator<StorageNodeContext> iter = storageNodes.iterator();
						StorageNodeContext snctx = iter.next();
						while (snctx.equals(storageNodePrimary.replicaAssignment1)) {
							snctx = iter.next();
							
						}
						replicaAssignment2 = snctx;
						iter.remove();
						storageNodePrimary.replicaAssignment2 = replicaAssignment2;
					}
					
					/* Bump requests of all assignments since we are about to send a file */
					storageNodePrimary.bumpRequests();

					/* Put that file in this nodes bloom filter */
					storageNodePrimary.put(message.getStoreRequest().getFileName().getBytes());

					/* Put primary back in the queue */
					storageNodes.add(storageNodePrimary);
					if (replicaAssignment1 != null) {
					storageNodes.add(replicaAssignment1);
					}
					if (replicaAssignment2 != null) {
					storageNodes.add(replicaAssignment2);
					}
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
		} else if (message.hasPrintRequest()) {
			ArrayList<StorageNodeContext> nodeList = new ArrayList<>();
			nodeList.addAll(storageNodes);
			StorageMessages.StorageMessageWrapper msgWrapper = Builders.buildPrintRequest(nodeList);
			ChannelFuture write = ctx.pipeline().writeAndFlush(msgWrapper);
			write.syncUninterruptibly();
			
			
		}
	}

	/**
	 * Runnable object that iterates through the queue of storage nodes every 5
	 * seconds checking timestamps
	 */
	private static class HeartBeatChecker implements Runnable {
		PriorityQueue<StorageNodeContext> storageNodes;
        EventLoopGroup workerGroup;
        Bootstrap bootstrap;

		public HeartBeatChecker(PriorityQueue<StorageNodeContext> storageNodes) {
			this.storageNodes = storageNodes;
            workerGroup = new NioEventLoopGroup();
            MessagePipeline pipeline = new MessagePipeline();
    		bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
    				.option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);
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
							iter.remove();
							handleNodeFailure(node);
							logger.info("Detected failure on node: " + node.getHostName());
							/* Also need to rereplicate data here. */
							/* We have to rereplicate this nodes replicas as well as its primarys */
							
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

        /**
         *
         *
         *
         * @param downNode
         */
        public void handleNodeFailure(StorageNodeContext downNode) {
        	// send message to these nodes to rereplicate the data belonging to the DOWN NODE
            StorageNodeContext downNodeReplicaAssignment1 = downNode.replicaAssignment1;
            StorageNodeContext downNodeReplicaAssignment2 = downNode.replicaAssignment2;

            
            String targetHost = "";

            // this list contains nodes that were replicating to the down node
            ArrayList<StorageNodeContext> nodesThatNeedNewReplicaAssignments = new ArrayList<>();

            synchronized (storageNodes) {

				Iterator<StorageNodeContext> iter = storageNodes.iterator();
				while (iter.hasNext()) {
					StorageNodeContext currNode = iter.next();

					/*
					 	This if statement finds nodes that were replicating to the down node
					*/
					
					if (currNode.replicaAssignment1 == downNode || currNode.replicaAssignment2 == downNode) {
						logger.info("Node that needs new assignment: " + currNode.getHostName());
						nodesThatNeedNewReplicaAssignments.add(currNode);
					}

					/*
					 		This else if statement finds a node that the down node was not replicating to
					 		so we can use it as a target node for when we tell the nodes that the 
					 		down node was replicating to which node they can now replicate the down node's
					 		data to. This targetHost is sent in the first message to the first replica
					 		of the down node that we use as a new primary.
					 */
					else if(currNode != downNodeReplicaAssignment1 && currNode != downNodeReplicaAssignment2) {
						targetHost = currNode.getHostName();
                        
					}
				}
            }

            logger.info("Target Host for replication for nodes that the down node was replicating to: " + targetHost);
            
            /* We will pick the first replica assingments of the down node to be the new primary holder of the data */
            String hostname1 = downNodeReplicaAssignment1.getHostName();
            
            /* Merge the down node's bloom filter into the new primarys for routing purposes */
            downNodeReplicaAssignment1.getFilter().mergeFilter(downNode.getFilter());
            String downNodeReplicaAssignment2Hostname = downNodeReplicaAssignment2.getHostName();
            String downHost = downNode.getHostName();
            
            /*
             		Send message to the down nodes first replica assignment, telling it which node went down, and
             		which node it can use re-replicate the down nodes chunks to. 
             */
            StorageMessages.StorageMessageWrapper replicaRequest = Builders.buildReplicaRequest(targetHost, downHost, false, downNodeReplicaAssignment2Hostname);
            
    		ChannelFuture cf = bootstrap.connect(hostname1, 13114);
    		cf.syncUninterruptibly();
    		
    		Channel chan = cf.channel();
    		ChannelFuture write = chan.writeAndFlush(replicaRequest);
    		write.syncUninterruptibly();
    		
    		
    		/*
                Now we iterate through the list of nodes that were replicating TO the down node,
                but we need to find a new targetNode for their new replica assignment, since 
                the down node was one of their assignments.
    		 */
    		
    		
    		for (StorageNodeContext node : nodesThatNeedNewReplicaAssignments) {
    			String nodeName = node.getHostName();
    			
    			synchronized (storageNodes) {
    				
    				/*
    				 	Here we iterate through the storagenode queue to find a new node that they can use 
    				 	as a new replica assignment. The logic is such:
    				 	
    				 	Find a node that that is not an assignment of the current
    				 	node we are iterating on, and also make sure that the current node
    				 	we are looking at is not the same node as the node we are trying to
    				 	find a new replica for. Otherwise it is possible that the target
    				 	node (new node assignment) can be the same as the node itself
    				 								 	
    				 */
    				
                        
                    Iterator<StorageNodeContext> iter = storageNodes.iterator();
                    while (iter.hasNext()) {
                        StorageNodeContext currNode = iter.next();
                        
                        if (currNode != node.replicaAssignment1 && currNode != node.replicaAssignment2 && currNode != node) {
                            targetHost = currNode.getHostName();
                        }
                        
                        
                    }
                    
                    }	
                    cf = bootstrap.connect(nodeName, 13114);
                    cf.syncUninterruptibly();
                    chan = cf.channel();
                    
                    StorageMessages.StorageMessageWrapper replicaAssignmentChange = Builders.buildReplicaRequest(targetHost, downHost, true, null);
                    write = chan.writeAndFlush(replicaAssignmentChange).syncUninterruptibly();
                    
    		
    		
    		
    		}
    		
        }
	}
}
