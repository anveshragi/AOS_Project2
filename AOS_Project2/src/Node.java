import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Hashtable;


public class Node {

	public static int node_num = 0;
	public static int counter = 0;
	public static int num_of_servers = 0;
	public static int num_of_users = 0;
	public static int num_of_total_nodes = 0;
	public static final int replicaFactor = 3;
	public static Hashtable<String,Socket> clientSocketsArray = new Hashtable<String,Socket>();
	public static Hashtable<String,Socket> userSocketsArray = new Hashtable<String,Socket>();
	public static Hashtable<String,Socket> serverSocketsArray = new Hashtable<String,Socket>();
	public static Hashtable<String,Socket> serverSocketsForUsersArray = new Hashtable<String,Socket>();
	public static Hashtable<String,ObjectOutputStream> outputStreamsOfserverSocketsForClients = new Hashtable<String,ObjectOutputStream>();
	public static Hashtable<String,ObjectOutputStream> outputStreamsOfserverSocketsForUsers = new Hashtable<String,ObjectOutputStream>();
	
	public static ReadConfig config;
	public Server server;
//	public static BufferedWriter bw;


	public void init(String configFileName) {

		try {

			// Read configuration file
			config = new ReadConfig();

			config.read(new File(configFileName)); // new File("config.txt")

			Node.num_of_total_nodes = config.nodeidentifiers.length;
			Node.num_of_servers = config.num_of_servers;
			Node.num_of_users = Node.num_of_total_nodes - Node.num_of_servers;
			System.out.println("total : " + Node.num_of_total_nodes + "servers : " + Node.num_of_servers + "users : " + Node.num_of_users);

			for(int i = 0; i < config.nodeidentifiers.length; i++){

				if(config.hostnames[i].equals(InetAddress.getLocalHost().getHostName().toString())) {

					Node.node_num = config.nodeidentifiers[i];

					if(config.nodetypes[i].equals("server")) {
						server = new Server(config.portnumbers[i]);
						server.start();

						Thread.sleep(5000);
						activateClientConnections();

					} else if(config.nodetypes[i].equals("client")) {
						activateUserConnections();

						Thread.sleep(2000);
						VectorClock vectorClock = new VectorClock(Node.node_num,Node.counter);
						Message message = new Message("WRITE","key","value",vectorClock);

						put(message);

						Node.counter++;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

	public static void writeToServersFile(Message object) {
		
		try {
			String filename = "File" + Node.node_num + ".txt";						
			BufferedWriter bw = new BufferedWriter(new FileWriter(filename,true));
			
			bw.write(object.getKey() + " " + object.getValue() + " " + object.getVectorClock().getNode() + object.getVectorClock().getCounter() + "\n");
			
			bw.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void activateUserConnections() {
		for(int j = 0; j < config.nodeidentifiers.length; j++) {
			try {	
				if(config.nodetypes[j].equals("server")) {

					User user = new User(config.hostnames[j], config.portnumbers[j]);
					user.start();					
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}	
	}

	public void activateClientConnections() {

		for(int j = 0; j < config.nodeidentifiers.length; j++) {
			try {	
				if(config.nodetypes[j].equals("server") && !(config.hostnames[j].equals(InetAddress.getLocalHost().getHostName().toString()))) {

					Client client = new Client(config.hostnames[j], config.portnumbers[j]);
					client.start();

				}
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}	
	}

	public static boolean isConnected(ObjectOutputStream oos) {
		
		Message msg = new Message("TEST","","",new VectorClock(0,0));
		try {
			oos.writeObject(msg);
			oos.writeObject(msg);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	public void put(final Message object) {

		(new Thread() {
			@Override
			public synchronized void run() {

				try {
					int hashCode = object.getKey().hashCode();
					int hashValue = hashCode%Node.num_of_servers;
					System.out.println("hashCode : " + hashCode + " hashValue : " + hashValue);

					String serverNames[] = new String[Node.replicaFactor];
					ObjectOutputStream output[] = new ObjectOutputStream[Node.replicaFactor];
					int numOfAvailableServers = 0;
					
//					Iterator<Entry<String, Socket>> iter = Node.serverSocketsForUsersArray.entrySet().iterator();
//					Entry<String, Socket> serverSocketEntry = null;
//					while(iter.hasNext()) {
//						serverSocketEntry = iter.next();
//						System.out.println(serverSocketEntry.getKey().toString()+ " " + serverSocketEntry.getValue().toString());
//					}
					
					for(int i = 0; i < Node.replicaFactor; i++) {
						
						serverNames[i] = Node.config.hostnames[(hashValue+i)%Node.num_of_servers];
						output[i] = Node.outputStreamsOfserverSocketsForUsers.get(serverNames[i]);
						
						if(isConnected(output[i])) {
							numOfAvailableServers++;
						}
					}				

					if(numOfAvailableServers>=Node.replicaFactor-1) {

						for(int i = 0; i < Node.replicaFactor;i++) {
							if(isConnected(output[i])) {
								output[i].writeObject(object);
								output[i].flush();
							}
						}				
					}			
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}).start();
	}	
}
