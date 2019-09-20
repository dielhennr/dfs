package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.ArrayList;

import edu.usfca.cs.dfs.net.ServerMessageRouter;
import io.netty.channel.ChannelHandlerContext;

public class Controller {

    ServerMessageRouter messageRouter;
    static ArrayList<JoinContext> storageNodes = new ArrayList<JoinContext>();
    

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
			StorageMessages.JoinRequest joinRequest
				= message.getJoinRequest();
			storageNodes.add(new JoinContext(ctx, joinRequest));
    	}
    	for (JoinContext join : storageNodes) {
    	    System.out.println(join.getJoinRequest().getNodeName());
        }
    	
    }
}
