import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;


public class User extends Thread {

	private String serverAddress;
	private int port;
	private Socket socket = null;

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
			
			Node.serverSocketsForUsersArray.put(this.socket.getInetAddress().getHostAddress().toString(), this.socket);
			
			PrintWriter output = new PrintWriter(this.socket.getOutputStream(), true);
			
			StringBuffer buffer = new StringBuffer();
			buffer.append("write ");			// identifier/type of the message object
			buffer.append("key from ");			// key of the object
			buffer.append("value from ");		// value of the object
			buffer.append(Node.node_num + " ");	// node component in Vector clock
			buffer.append(Node.counter);		// counter component in Vector clock
			
			output.println(buffer.toString());
			
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
