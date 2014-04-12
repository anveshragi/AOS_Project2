import java.io.File;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Hashtable;


public class Node {

	public static Hashtable<String,Socket> clientSocketsArray = new Hashtable<String,Socket>();
	public static Hashtable<String,Socket> serverSocketsArray = new Hashtable<String,Socket>();
	public static Hashtable<String,Socket> serverSocketsForUsersArray = new Hashtable<String,Socket>();
	public static ReadConfig config;
	public Client client[];
	public Server server;
	
	public void init(File file) {

		try {
			
			// Read configuration file
			config = new ReadConfig();
			
			config.read(file); // new File("config.txt")

			for(int i = 0; i < config.nodeidentifiers.length; i++){

				if(config.hostnames[i].equals(InetAddress.getLocalHost().getHostName().toString())) {
					
					if(config.nodetypes[i].equals("server")) {
						server = new Server(config.portnumbers[i]);
						server.start();
						
						activateServerConnectionsWithUsers();
						
					} else if(config.nodetypes[i].equals("client")) {
						activateClientConnections();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	public void activateServerConnectionsWithUsers() {
		for(int j = 0; j < config.nodeidentifiers.length; j++) {
			try {	
				if(config.nodetypes[j].equals("server") && !(config.hostnames[j].equals(InetAddress.getLocalHost().getHostName().toString()))) {

					User user = new User(config.hostnames[j], config.portnumbers[j]);
					user.start();					
				}
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}	
	}
	
	public void activateClientConnections() {

		for(int j = 0; j < config.nodeidentifiers.length; j++) {
			try {	
				if(config.nodetypes[j].equals("server") && !(config.hostnames[j].equals(InetAddress.getLocalHost().getHostName().toString()))) {

					this.client[j] = new Client(config.hostnames[j], config.portnumbers[j]);
					this.client[j].start();
					
				}
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}	
	}

}
