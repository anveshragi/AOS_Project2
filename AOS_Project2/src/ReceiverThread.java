import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
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

		//				System.out.println("ReceiverThread " + Thread.currentThread().getName());
		try {
			if(this.clientSocket != null) {
				while(true) {					
					String read = this.input.readLine();		
					if(read != null){
						System.out.println("\nMessage Received : " + read.toString() + "\n");

						if(read.contains("WRITE")) {																
							receive_write(read);
						}							
					}	
				}					
			}	
		} catch (NumberFormatException | IOException e) {
			e.printStackTrace();
		} 
	}

	public void receive_write(String read) {

		try {			
			int hashCode = read.hashCode();
			int hashValue = hashCode%Node.num_of_servers;
			System.out.println("hashCode : "+hashCode + " hashValue : " + hashValue);

			String[] tokens = read.split(" ");

			if((Integer.valueOf(InetAddress.getLocalHost().getHostName().toString().substring(3,5))-1) == hashValue) {
				Node.bw.write(tokens[1] + " " + tokens[2] + "\n");
			} else if (((Integer.valueOf(InetAddress.getLocalHost().getHostName().toString().substring(3,5))-1) == hashValue + 1) || ((Integer.valueOf(InetAddress.getLocalHost().getHostName().toString().substring(3,5))-1) == hashValue + 2)) {				
				processDataVersion(hashValue, tokens);
			} else {
				System.out.println("Invalid hashValue of the object sent to this serve ... ");
			}	

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void processDataVersion(int firstReplicaHashValue, String[] tokens) {

		try {
			int localsourceNodeNum = Integer.valueOf(tokens[3]);
			int localObjectDataVersion = Integer.valueOf(tokens[4]);
			int version = checkDataVersionAtFirstReplica(firstReplicaHashValue, localsourceNodeNum, localObjectDataVersion);

			if(version == 0 || version == 1) {
				Node.bw.write(tokens[1] + " " + tokens[2] + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// Return 0 if object versions are same; 
	// Return -1 if local object version is less than that at first replica
	// Return 1 if local object version is greater than that at first replica
	public int checkDataVersionAtFirstReplica(int firstReplicaHashValue, int localsourceNodeNum, int localObjectDataVersion) {

		Socket firstReplicasocket = null;		
		int remoteSourceNodeNum	= 0;
		int remoteObjectDataVersion = 0;
		
		// Logic to extract data from firstreplica server's file
//		String firstReplicaHostName = "net0"+firstReplicaHashValue+".utdallas.edu";

		if(localObjectDataVersion < remoteObjectDataVersion) {
			return -1;
		} else if(localObjectDataVersion > remoteObjectDataVersion) {
			return 1;
		} else if(localObjectDataVersion == remoteObjectDataVersion) {
			return 0;
		}
		
		return -1;
	}












	public void forward_object(String[] tokens, int dest_node_num) {

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
					buffer.append(tokens[0]);

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



