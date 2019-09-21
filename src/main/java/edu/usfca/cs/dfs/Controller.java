package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.ArrayList;

import edu.usfca.cs.dfs.net.ServerMessageRouter;
import io.netty.channel.ChannelHandlerContext;

public class Controller {

    ServerMessageRouter messageRouter;
    static ArrayList<RequestContext> storageNodes = new ArrayList<RequestContext>();
    

    public void start()
    throws IOException {
        messageRouter = new ServerMessageRouter();
        messageRouter.listen(4123);
        System.out.println("Listening for connections on port 4123");
    }

    public static void main(String[] args)
    throws IOException {
        Controller controller = new Controller();
        controller.start();
    }
    
    public static void OnMessage(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper message) {
    	if (message.hasJoinRequest()) {
			storageNodes.add(new RequestContext(ctx, message));
    	}
    	for (RequestContext join : storageNodes) {
    	    System.out.println(join.getRequest().getJoinRequest().getNodeName());
        }
    	
    }
}
