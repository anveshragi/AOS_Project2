

import java.io.File;
import java.util.Scanner;
import java.util.StringTokenizer;


public class ReadConfig {
	int[] nodeidentifiers;
	String[] hostnames;
	int[] portnumbers;
	String[] nodetypes;
	int num_of_servers;
	
	public void read(File file) throws Exception
	{
		//		String dir = System.getProperty("user.dir");
		//		System.out.println("user.dir = " + dir);
		//		BufferedReader reader = new BufferedReader(new FileReader(dir + "//config1.txt"));
		
//		File file = new File("config.txt");
		Scanner sc = new Scanner(file);
		String line = null;
		
		if(sc.hasNextLine()) {
			
			line = sc.nextLine();
			//			System.out.println("skipped line : " + line.toString());
			line = sc.nextLine();
			//			System.out.println("first line : " + line.toString());
			
			StringTokenizer firstLine = new StringTokenizer(line);
			
			String noOfNodesString = firstLine.nextToken();			
			int noOfNodes = Integer.parseInt(noOfNodesString);
			
			nodeidentifiers = new int[noOfNodes];
			hostnames = new String[noOfNodes];
			portnumbers = new int[noOfNodes];
			nodetypes = new String[noOfNodes];
			
			line = sc.nextLine();
			//			System.out.println("second line : " + line.toString());
			StringTokenizer secondLine = new StringTokenizer(line);
			
			String noOfServers = secondLine.nextToken();
			num_of_servers = Integer.parseInt(noOfServers);
			
			line = sc.nextLine();
			
			while (sc.hasNextLine()) {
				
				line = sc.nextLine();
				//				System.out.println("line : " + line.toString());
				
				StringTokenizer st = new StringTokenizer(line);
				
				String element = st.nextToken();
				int node = Integer.parseInt(element);
				nodeidentifiers[node] = node;
				
				String hostname = st.nextToken();
				hostnames[node] = hostname;
				
				String portnumberString = st.nextToken();
				int portnumber = Integer.parseInt(portnumberString);
				portnumbers[node] = portnumber;
				
				String nodetype = st.nextToken();
				nodetypes[node] = nodetype;
			}
		}
		sc.close();
	}
}

