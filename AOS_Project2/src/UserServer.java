import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;


public class UserServer extends Thread {
	private int port;
	private Socket clientSocket = null;
	private ServerSocket socket;
	
	public UserServer(int port) {
		super();
		this.port = port;
	}

	@Override
	public void run() {

			try {
				socket = new ServerSocket(this.port);
				
				System.out.println("Initializing server...\nServer on " + InetAddress.getLocalHost().getHostName() + " listening on port#" + this.port + "\n");
	
				while(true) {
	//				System.out.println("Entering Server Thread while loop... and it's thread number is ..." + Thread.currentThread().getName());
					try {
						this.clientSocket = socket.accept();
						
//						System.out.println("At Server...Connection with client " + this.clientSocket.getInetAddress().getHostName() + " established\n");
						
						Thread.sleep(0);
						
						//System.out.println("after sleep ");
						
						ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream());
						
						//System.out.println("after ois ... ");
						Message msg = (Message) ois.readObject();
						//System.out.println("message received : " + msg.getMsg_identifier() + " "+ msg.getKey() + " "+ msg.getValue() + " "+ msg.getVectorClock().getNode() + " "+ msg.getVectorClock().getCounter());
						if(msg.getMsg_identifier().equals("ReadReply")) {
							System.out.println("message received : " + msg.getMsg_identifier() + " "+ msg.getKey() + " "+ msg.getValue() + " "+ msg.getVectorClock().getNode() + " "+ msg.getVectorClock().getCounter());
						} else if(msg.getMsg_identifier().equals("keyExists")) {
//							System.out.println("Key already exists in the server : " + this.clientSocket.getInetAddress().getHostName() + ", updating the existing key with given value " + msg.getValue());
						}										
						
					} catch ( ClassNotFoundException e) {
						System.out.println("Error connecting with client " + this.clientSocket.getInetAddress().getHostName() + " : " + e);
					} catch (InterruptedException e) {
						e.printStackTrace();
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
