import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;


public class User extends Thread {

	private String serverAddress;
	private int port;
	private Socket socket = null;
	private ObjectOutputStream output = null;

	public User(Socket skt) {
		super();
		this.socket = skt;
	}

	public User(String serverAddress, int port) {
		super();
		this.serverAddress = serverAddress;
		this.port = port;
	}

	public synchronized void run() {
		
//		System.out.println("Client Thread " + Thread.currentThread().getName());

		try {
			socket = new Socket(this.serverAddress, this.port);
			
			System.out.println("Connection with server " + this.serverAddress + " at port#" + this.port + "\n");
			
			Node.serverSocketsForUsersArray.put(this.socket.getInetAddress().getHostName().toString(), this.socket);
			
			output = new ObjectOutputStream(this.socket.getOutputStream());
			Node.outputStreamsOfserverSocketsForUsers.put(this.socket.getInetAddress().getHostName().toString(), this.output);
			
//			Message msg = new Message("write","key","value", new VectorClock(Node.node_num,Node.counter));
//			ObjectOutputStream output = new ObjectOutputStream(this.socket.getOutputStream());
//			output.writeObject(msg);
//			output.flush();
//			
//			System.out.println("message sent!!");
			
		} catch (UnknownHostException e) {
			System.out.println(" Unknown hostname exception : " + e);
		} catch (IOException e) {
			System.out.println("Error connecting to server " + this.serverAddress + " : " + e);
		}
	}

	public void closeConnection() {
		try {
			socket.close();
		} catch (IOException e) {
			System.out.println("Error closing the connection with server " + this.serverAddress + " : " + e);
		}
	}
}
