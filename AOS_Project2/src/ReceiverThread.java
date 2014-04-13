import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Iterator;
import java.util.Map.Entry;

public class ReceiverThread extends Thread{

	private Socket clientSocket;
	private BufferedReader input;

	public ReceiverThread(Socket clientSocket) {
		this.clientSocket = clientSocket;
		try {
			this.input = new BufferedReader(new InputStreamReader(this.clientSocket.getInputStream()));

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public synchronized void run() {

		//		System.out.println("ReceiverThread " + Thread.currentThread().getName());

		try {
			while(true) {
				try {
					String read = this.input.readLine();				
					System.out.println("\nMessage Received : " + read.toString() + "\n");

					String[] tokens = read.split(" ");

					int hashCode = read.hashCode();
					int hashValue = hashCode%7;
					System.out.println("hashCode : "+hashCode + " hashValue : " + hashValue);

					if(hashValue == Node.node_num) {
						
						Node.bw.write(read.toString() + "\n");
					} else if(hashValue == Node.node_num - 1){
						forward_object(read, hashValue);
					} else if(hashValue == Node.node_num - 2) {
						forward_object(read, hashValue);
					} else {
						System.out.println("Invalid hashValue of the object sent to this serve ... ");
					}				
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} 
	}

	public void forward_object(String object, int dest_node_num) {

		try {

			PrintWriter output = null;
			Iterator<Entry<String, Socket>> iter = Node.clientSocketsArray.entrySet().iterator();
			Entry<String, Socket> clientSocket;
			Socket serverSocketForRespectiveClient = null;
			int index = 0;

			while(iter.hasNext()) {

				clientSocket = iter.next();

				index = Integer.valueOf(clientSocket.getValue().getInetAddress().getHostName().toString().substring(3,5))-1;

				if(index == dest_node_num) {
					StringBuffer buffer = new StringBuffer();
					buffer.append(object);

					serverSocketForRespectiveClient = Node.serverSocketsArray.get(clientSocket.getKey().toString());

					output = new PrintWriter(serverSocketForRespectiveClient.getOutputStream(), true);

					output.println(buffer.toString());
					System.out.println("\nMessage Sent : " + buffer.toString() +"\n");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}


