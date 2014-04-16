import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;


public class Node {

	public static int node_num = 0;
	public static int counter = 0;
	public static int num_of_servers = 0;
	public static int num_of_users = 0;
	public static int num_of_total_nodes = 0;
	public static Hashtable<String,Socket> clientSocketsArray = new Hashtable<String,Socket>();
	public static Hashtable<String,Socket> serverSocketsArray = new Hashtable<String,Socket>();
	public static Hashtable<String,Socket> serverSocketsForUsersArray = new Hashtable<String,Socket>();
	public static ReadConfig config;
	public Server server;
	public static BufferedWriter bw;


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

					node_num = config.nodeidentifiers[i];

					if(config.nodetypes[i].equals("server")) {
						server = new Server(config.portnumbers[i]);
						server.start();

						String filename = "File" + Node.node_num + ".txt";						
						bw = new BufferedWriter(new FileWriter(filename,true));

						Thread.sleep(5000);
						activateClientConnections();

					} else if(config.nodetypes[i].equals("client")) {
						activateUserConnections();

						StringBuffer buffer = new StringBuffer();
						buffer.append("write ");			// identifier/type of the message object
						buffer.append("key from ");			// key of the object
						buffer.append("value from ");		// value of the object
						buffer.append(Node.node_num + " ");	// node component in Vector clock
						buffer.append(Node.counter);		// counter component in Vector clock

						//						Message msg = 
						put(buffer.toString());

						Node.counter++;
					}
				}
			}
		} catch (Exception e) {
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

	public void put(String object) {

		try {

			int hashCode = object.hashCode();
			int hashValue = hashCode%Node.num_of_servers;
			System.out.println("hashCode : "+ hashCode + " hashValue : " + hashValue);

			Socket serverSocket = null;
			PrintWriter output = null;
			Iterator<Entry<String, Socket>> iter = Node.serverSocketsForUsersArray.entrySet().iterator();
			Entry<String, Socket> serverSocketEntry;
			
			while(iter.hasNext()) {
				serverSocketEntry = iter.next();
				
				int index = Integer.valueOf(serverSocketEntry.getValue().getInetAddress().getHostName().toString().substring(3,5))-1;

				if(index == hashValue) {
					serverSocket = serverSocketEntry.getValue();
					
					output = new PrintWriter(serverSocket.getOutputStream(), true);
					output.println(object);
				} 				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
