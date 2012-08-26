package org.apache.cassandra.net;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.net.MessagingService.Verb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class Monitor {
    private DBCollection collection;

    private static final String DEFAULT_MONGO_HOST="192.168.0.128";
    private static final String DEFAULT_MONGO_PORT="27017";
    private static final String DEFAULT_MONGO_DB="collector_b";
    private static final String DEFAULT_MONGO_COLLECTION="responseTime";
    private static final Logger logger = LoggerFactory.getLogger(Monitor.class);
    
    private static final Monitor monitorInstance = new Monitor();
    private static final Executor executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("monitor-writer"));
    
    private Map<String, CallInfo> callbackInfo = new HashMap<String, CallInfo>();
    
    public static Monitor instance() {
    	return monitorInstance;
    }
    
    class CallInfo {
    	long time;
    	Verb verb;
    	CallInfo(Verb verb) {
    		time = System.nanoTime();
    		this.verb = verb; 
    	}
    }
    
    private Monitor() {
        try {
        	String mongoHost = System.getProperty("mongo.host", DEFAULT_MONGO_HOST);
        	int mongoPort = Integer.valueOf(System.getProperty("mongo.port", DEFAULT_MONGO_PORT));
        	String mongoDb = System.getProperty("mongo.collection", DEFAULT_MONGO_DB);
        	String mongoCollection = System.getProperty("mongo.collection", DEFAULT_MONGO_COLLECTION);
        	
        	Mongo mongo = new Mongo(mongoHost, mongoPort);
        	DB db = mongo.getDB(mongoDb);
        	collection = db.getCollection(mongoCollection);
        } catch (Exception e) {
        	logger.error("Cannot initialize monitor DB {}",e);
        	System.exit(1);
        }
	}
    
    public void addCallback(String messageId, Verb verb) {
    	callbackInfo.put(messageId, new CallInfo(verb));
    }
    
    public <T> void logMessage(final String callbackId, final InetAddress server, final Verb verb) {    	
		final long receivedTime = System.nanoTime();
		Runnable runnableLog = new Runnable() {
			public void run() {
				CallInfo info = callbackInfo.remove(callbackId);
				if (info == null) {
					return;
				}
				long responseTime = receivedTime - info.time;
				DBObject dbObject = new BasicDBObjectBuilder().add("server", server.toString())
					.add("responseTime", responseTime/1000)
					.add("verb",info.verb+"/"+verb.toString())
					.get();
				collection.insert(dbObject);				
			};
		};
		executor.execute(runnableLog);
    }
}