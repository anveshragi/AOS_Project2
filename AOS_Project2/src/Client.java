import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class Client extends Thread{

	private String serverAddress;
	private int port;
	private Socket socket = null;

	public Client(Socket skt) {
		super();
		this.socket = skt;
	}

	public Client(String serverAddress, int port) {
		super();
		this.serverAddress = serverAddress;
		this.port = port;
	}

	public synchronized void run() {
		
//		System.out.println("Client Thread " + Thread.currentThread().getName());

		try {
			socket = new Socket(this.serverAddress, this.port);
			
			System.out.println("Connection with server " + this.serverAddress + " at port#" + this.port + "\n");

			Node.serverSocketsArray.put(this.socket.getInetAddress().getHostAddress().toString(), this.socket);			
			
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
