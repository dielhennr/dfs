package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.net.ServerMessageRouter;
import io.netty.channel.ChannelHandlerContext;

public class Controller implements DFSNode {

    ServerMessageRouter messageRouter;
    Queue<StorageNodeContext> storageNodes; 
    private static final Logger logger = LogManager.getLogger(Controller.class);
    
    public Controller() {
    	storageNodes = new LinkedList<StorageNodeContext>();
    }

    public void start()
    throws IOException {
    	/* Pass a reference of the controller to our message router */
        messageRouter = new ServerMessageRouter(this);
        messageRouter.listen(13100);
        System.out.println("Listening for connections on port 13100");
    }

    public static void main(String[] args)
    throws IOException {
        Controller controller = new Controller();
        controller.start();
    }
    
    public void onMessage(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper message) {
    	if (message.hasJoinRequest()) {
    		String storageHost = message.getJoinRequest().getNodeName();
    		logger.info("Recieved join request from " + storageHost);
			storageNodes.add(new StorageNodeContext(ctx, storageHost));
    	} 
    	else if (message.hasHeartbeat()) {
    		logger.debug("Recieved heartbeat from " + message.getHeartbeat().getHostname());
    	}
    	else if (message.hasStoreQuery()) {
    		/* Remove next node from the queue*/
    		StorageNodeContext storageNode = storageNodes.poll();
    		logger.info("Recieved request to put file on " + storageNode.getHostname() + " from client.");
    		/* Write back a join request to client with hostname of the node to send chunks to*/ 
    		
			/* Put that file in this nodes bloom filter */
			storageNode.put(message.getStoreQuery().getFileName().getBytes());
			storageNodes.add(storageNode);
    		
    	} 
    	else if (message.hasRetrieveFile()) {
    		/* Here we could check each nodes bloom filter and then send the client the
    		 * list of nodes that could have it.
    		 * */
    		
    	}
    }
}
