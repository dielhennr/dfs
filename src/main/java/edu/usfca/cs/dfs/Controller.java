package edu.usfca.cs.dfs;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.net.ServerMessageRouter;
import io.netty.channel.ChannelHandlerContext;

public class Controller {

    ServerMessageRouter messageRouter;
    static ArrayList<RequestContext> storageNodes = new ArrayList<RequestContext>();
    private static final Logger logger = LogManager.getLogger(Controller.class);
    

    public void start()
    throws IOException {
        messageRouter = new ServerMessageRouter();
        messageRouter.listen(4123);
        System.out.println("Listening for connections on port 4123");
    }

    public static void main(String[] args)
    throws IOException {
    	/** 
    	 * Logging example -- logs will output to log/app.log in root project directory.
    	 * Configuration currently only recognizes INFO+ so logger.trace/debug does nothing
    	 * Use logger.info for debugging and logger.error for logging exceptions
    	 * 
    	 * The reason I took out Debug level is because netty uses it and logs a 
    	 * ton of unnecessary shit
    	 */
    	logger.error("ERROR");

        logger.trace("Entering application...");
        logger.debug("Entering application...");
        logger.info("Hello Log4j2...");
        logger.error("Something is wrong with this code", new Exception("Invalid message"));

        logger.trace("Exiting application...");
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
