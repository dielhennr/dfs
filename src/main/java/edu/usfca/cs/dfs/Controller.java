package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.ArrayList;

import edu.usfca.cs.dfs.net.ServerMessageRouter;

public class Controller {

    ServerMessageRouter messageRouter;
    static ArrayList<String> storageNodes = new ArrayList<String>();
    

    public void start()
    throws IOException {
        messageRouter = new ServerMessageRouter();
        messageRouter.listen(4200);
        System.out.println("Listening for connections on port 7777");
    }

    public static void main(String[] args)
    throws IOException {
        Server s = new Server();
        s.start();
    }
    
    public static void OnMessage(StorageMessages.StorageMessageWrapper message) {
    	if (message.hasJoinRequest()) {
			StorageMessages.JoinRequest joinRequest
				= message.getJoinRequest();
			storageNodes.add(joinRequest.getNodeName());
			System.out.println(storageNodes.toString());
    	}
    	
    }
}
