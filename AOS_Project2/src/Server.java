import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class Server extends Thread {
	
	private int port;
	private Socket clientSocket = null;
	private ServerSocket socket;
	
	public Server(int port) {
		super();
		this.port = port;
	}


	public synchronized void run() {

		try {
			socket = new ServerSocket(this.port);
			
			System.out.println("Initializing server...\nServer on " + InetAddress.getLocalHost().getHostName() + " listening on port#" + this.port + "\n");

			while(true) {
//				System.out.println("Entering Server Thread while loop... and it's thread number is ..." + Thread.currentThread().getName());
				try {
					this.clientSocket = socket.accept();
					
					System.out.println("At Server...Connection with client " + this.clientSocket.getInetAddress().getHostName() + " established\n");

					for(int i = 0; i < Node.config.nodeidentifiers.length; i++) {
						if(Node.config.hostnames[i].equals(this.clientSocket.getInetAddress().getHostName().toString())) {
							if(Node.config.nodetypes[i].equals("client")) {
								Node.userSocketsArray.put(this.clientSocket.getInetAddress().getHostName().toString(),this.clientSocket);
								
							} else if(Node.config.nodetypes[i].equals("server")) {

								Node.clientSocketsArray.put(this.clientSocket.getInetAddress().getHostName().toString(),this.clientSocket);
								
							}
							
							ReceiverThread receiverThread = new ReceiverThread(this.clientSocket);
							receiverThread.start();
						}
					}			
				} catch (IOException e) {
					System.out.println("Error connecting with client " + this.clientSocket.getInetAddress().getHostName() + " : " + e);
				} 
			}
		} catch (IOException e) {
			try {
				System.out.println("Error activating server " + InetAddress.getLocalHost().getHostName() + " with port# " + this.port + " : " + e);
			} catch (UnknownHostException e1) {
				System.out.println(" Unknown hostname exception : " + e);
			}
		} 
	}

}
