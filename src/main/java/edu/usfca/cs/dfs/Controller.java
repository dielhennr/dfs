package edu.usfca.cs.dfs;

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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.PriorityQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
  }

  /* Controller inbound duties */
  public void onMessage(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper message) {
    if (message.hasJoinRequest()) {
      /* On join request need to add node to our network */
      String storageHost = message.getJoinRequest().getNodeName();
      logger.info("Recieved join request from " + storageHost);

      /* Add new node to priority queue */
      StorageNodeContext thisRequest = new StorageNodeContext(storageHost, ctx);
      synchronized (storageNodes) {
        if (storageNodes.size() == 2) {
          /* Build the directed graph of nodes replication assignments
           * once we have 3 of them in the network
           **/
          StorageNodeContext first = storageNodes.poll();
          StorageNodeContext second = storageNodes.poll();
          first.replicaAssignment1 = second;
          first.replicaAssignment2 = thisRequest;

          first
              .ctx
              .pipeline()
              .writeAndFlush(
                  Builders.buildReplicaAssignments(
                      first.replicaAssignment1.getHostName(),
                      first.replicaAssignment2.getHostName()));
          logger.info(
              "writing assignments "
                  + first.getHostName()
                  + " assigned to "
                  + first.replicaAssignment1.getHostName()
                  + " and "
                  + first.replicaAssignment2.getHostName());

          second.replicaAssignment1 = thisRequest;
          second.replicaAssignment2 = first;

          /* And second */
          second
              .ctx
              .pipeline()
              .writeAndFlush(
                  Builders.buildReplicaAssignments(
                      second.replicaAssignment1.getHostName(),
                      second.replicaAssignment2.getHostName()));

          thisRequest.replicaAssignment1 = first;
          thisRequest.replicaAssignment2 = second;

          /* Send thisRequest its assignments */
          thisRequest
              .ctx
              .pipeline()
              .writeAndFlush(
                  Builders.buildReplicaAssignments(
                      thisRequest.replicaAssignment1.getHostName(),
                      thisRequest.replicaAssignment2.getHostName()));

          storageNodes.add(first);
          storageNodes.add(second);
        } else if (storageNodes.size() > 2) {
          Iterator<StorageNodeContext> iter = storageNodes.iterator();
          thisRequest.replicaAssignment1 = iter.next();
          thisRequest.replicaAssignment2 = iter.next();

          /* Send thisRequest its assignments */
          thisRequest
              .ctx
              .pipeline()
              .writeAndFlush(
                  Builders.buildReplicaAssignments(
                      thisRequest.replicaAssignment1.getHostName(),
                      thisRequest.replicaAssignment2.getHostName()));
          logger.info(
              "writing assignments "
                  + thisRequest.getHostName()
                  + " assigned to "
                  + thisRequest.replicaAssignment1.getHostName()
                  + " and "
                  + thisRequest.replicaAssignment2.getHostName());
        } else {
          logger.info("Need more nodes in the network to make replica assignments");
        }
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

          /* Bump requests of all assignments since we are about to send a file */
          storageNodePrimary.bumpRequests();
          storageNodes.add(storageNodePrimary);

          storageNodes.remove(storageNodePrimary.replicaAssignment1);
          storageNodePrimary.replicaAssignment1.bumpRequests();
          storageNodes.add(storageNodePrimary.replicaAssignment1);

          storageNodes.remove(storageNodePrimary.replicaAssignment2);
          storageNodePrimary.replicaAssignment2.bumpRequests();
          storageNodes.add(storageNodePrimary.replicaAssignment2);

          /* Put that file in primary node's bloom filter */
          storageNodePrimary.put(message.getStoreRequest().getFileName().getBytes());

          /**
           * Write back a store response to client with hostname of the primary node to send chunks
           * to. This node will handle replication to its replica assignments
           */
          ChannelFuture write =
              ctx.pipeline()
                  .writeAndFlush(
                      Builders.buildStoreResponse(
                          message.getStoreRequest().getFileName(),
                          storageNodePrimary.getHostName()));

          write.syncUninterruptibly();

          /* Log controllers response */
          logger.info(
              "Approving request to put file on "
                  + storageNodePrimary.getHostName()
                  + " from client."
                  + "This SN has processed "
                  + storageNodePrimary.getRequests()
                  + " requests.");
          logger.info(
              "Replicating to "
                  + storageNodePrimary.replicaAssignment1.getHostName()
                  + " and "
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

      ChannelFuture write =
          ctx.pipeline().writeAndFlush(Builders.buildPossibleRetrievalHosts(hosts, fileName));
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
   * Runnable object that iterates through the queue of storage nodes every 5 seconds checking
   * timestamps
   */
  private static class HeartBeatChecker implements Runnable {
    PriorityQueue<StorageNodeContext> storageNodes;
    EventLoopGroup workerGroup;
    Bootstrap bootstrap;

    public HeartBeatChecker(PriorityQueue<StorageNodeContext> storageNodes) {
      this.storageNodes = storageNodes;
      workerGroup = new NioEventLoopGroup();
      MessagePipeline pipeline = new MessagePipeline();
      bootstrap =
          new Bootstrap()
              .group(workerGroup)
              .channel(NioSocketChannel.class)
              .option(ChannelOption.SO_KEEPALIVE, true)
              .handler(pipeline);
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
     * Handles the rereplication proccess in the event of a single node failure. Will not work for
     * concurrent failures.
     *
     * @param downNode {@link StorageNodeContext}
     */
    public void handleNodeFailure(StorageNodeContext downNode) {
      // send message to these nodes to rereplicate the data belonging to the DOWN NODE
      String downHost = downNode.getHostName();
      StorageNodeContext downNodeReplicaAssignment1 = downNode.replicaAssignment1;
      StorageNodeContext downNodeReplicaAssignment2 = downNode.replicaAssignment2;

      String targetHost = "";

      /* This list will contain nodes that were replicating to the down node */
      ArrayList<StorageNodeContext> nodesThatNeedNewReplicaAssignments = new ArrayList<>();

      /* Find nodes that were replicating to the down node */
      Iterator<StorageNodeContext> iter = storageNodes.iterator();
      while (iter.hasNext()) {
        StorageNodeContext currNode = iter.next();
        if (currNode.replicaAssignment1 == downNode || currNode.replicaAssignment2 == downNode) {
          logger.info("Node that needs new assignment: " + currNode.getHostName());
          nodesThatNeedNewReplicaAssignments.add(currNode);
        }
      }

      ChannelFuture cf;
      Channel chan;

      /**
       * Now we iterate through the list of nodes that were replicating TO the down node, but we
       * need to find a new targetNode for their new replica assignment, since the down node was one
       * of their assignments.
       */
      for (StorageNodeContext node : nodesThatNeedNewReplicaAssignments) {
        String nodeName = node.getHostName();
        /*
            Here we iterate through the storagenode queue to find a new node that they can use
            as a new replica assignment. The logic is such:
            Find a node that that is not an assignment of the current
            node we are iterating on, and also make sure that the current node
            we are looking at is not the same node as the node we are trying to
            find a new replica assignment for. Otherwise it is possible that the target
            node (new node assignment) can be the same as the node we send the message to
        */
        iter = storageNodes.iterator();
        while (iter.hasNext()) {
          StorageNodeContext currNode = iter.next();
          if ((currNode != node.replicaAssignment1)
              && (currNode != node.replicaAssignment2)
              && (currNode != node)) {
            targetHost = currNode.getHostName();
            break;
          }
        }

        cf = bootstrap.connect(nodeName, 13114);
        cf.syncUninterruptibly();
        chan = cf.channel();

        StorageMessages.StorageMessageWrapper replicaAssignmentChange =
            Builders.buildReplicateToNewAssignment(downHost, targetHost);
        chan.writeAndFlush(replicaAssignmentChange).syncUninterruptibly();
        chan.close().syncUninterruptibly();
      }

      /* Now we are done getting nodes that were replicating to down node's new assignments, but we are not done */

      /* We will sleep here for a second to ensure that all nodes recieve their new assignments, and then we will request a merge
       * of the down node's primary metadata into the new primary node
       * */

      /* If this node had no assignments it means we do not need to merge anything because no primaries were stored here done (should never be false regardless)*/
      if (downNodeReplicaAssignment1 != null && downNodeReplicaAssignment2 != null) {
        /* We will pick the first replica assingment of the down node to be the new primary holder of the data */
        String newPrimaryHolder = downNodeReplicaAssignment1.getHostName();

        /* Merge the down node's bloom filter into the new primarys for routing purposes */
        downNodeReplicaAssignment1.getFilter().mergeFilter(downNode.getFilter());

        /* Send message to the down node's first replica assignment, telling it which node went down and that nodes other assignment
         * This node will merge the down nodes data with it's primary data. Then it will check if the down nodes second replica assignment
         * is one of its own as well. If it is, it will ask downNodeReplicaAssignment2 to merge downNodes data with it's data and delete its
         * key for downnode, and then send down nodes data to its other replica assignment under the new primary key. If they don't have the same assignments,
         * the new primary holder sends the data of down node to both of it's assignments and then tells downNodeReplicaAssignment2 to delete it's data
         * for down node
         * */
        String downNodeReplicaAssignment2Hostname = downNodeReplicaAssignment2.getHostName();
        StorageMessages.StorageMessageWrapper replicaMergeRequest =
            Builders.buildMergeReplicasOnFailure(downHost, downNodeReplicaAssignment2Hostname);
        logger.info("New primary holder for merge of " + downNode + " -> " + newPrimaryHolder);
        cf = bootstrap.connect(newPrimaryHolder, 13114);
        cf.syncUninterruptibly();

        chan = cf.channel();
        ChannelFuture write = chan.writeAndFlush(replicaMergeRequest);
        write.syncUninterruptibly();

        chan.close().syncUninterruptibly();
      }
    }
  }
}
