import java.math.*;
import java.rmi.*;
import java.rmi.server.*;
import java.security.*;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.Scanner;

//
// SuperNode 	 
//
// A RMI service that return the Client requests for Lookup() and Insert() 
//
public class SuperNode extends UnicastRemoteObject implements SuperNodeDef
{
    private static int numNodes;
    private static int busy;
    //int m = 5;
    //int numDHT = (int)Math.pow(2,m);
    //Node[] nodeList = new Node[numDHT];
    private static int replicaFactor;
    private static int m;
    private static int numDHT;
    private static Node[] nodeList;
    private List<Integer> nodeIDList = new ArrayList<Integer>();

    public SuperNode () throws RemoteException
    {
        //super();
    }

    public String getNodeInfo(String nodeIP, String nodePort) throws RemoteException{
        if (busy == 0) {

            synchronized (this) {
                busy = 1;
            }

            int nodeID = 0;
            String initInfo = "";
            numNodes++;
            System.out.println("*** Node Initation Call: Connection from " + nodeIP);
            try{ 
                MessageDigest md = MessageDigest.getInstance("SHA1");
                md.reset();
                String hashString = nodeIP+ nodePort;
                md.update(hashString.getBytes());
                byte[] hashBytes = md.digest();
                BigInteger hashNum = new BigInteger(1,hashBytes);

                nodeID = Math.abs(hashNum.intValue()) % numDHT;  

                System.out.println("Generated ID: " + nodeID + " for requesting node");

                while(nodeList[nodeID] != null) { //ID Collision
                    md.reset();
                    md.update(hashBytes);
                    hashBytes = md.digest();
                    hashNum = new BigInteger(1,hashBytes);
                    nodeID = Math.abs(hashNum.intValue()) % numDHT;  
                    System.out.println("ID Collision, new ID: " + nodeID);
                }


                if (nodeList[nodeID] == null) {
                    nodeList[nodeID] = new Node(nodeID,nodeIP,nodePort);
                    nodeIDList.add(nodeID);
                    System.out.println("New node added ... ");
                }


                Collections.sort(nodeIDList,Collections.reverseOrder());

                int predID = nodeID;
                Iterator<Integer> iterator = nodeIDList.iterator();
                while (iterator.hasNext()) {
                    int next = iterator.next();
                    if (next < predID) {
                        predID = next;
                        break;	
                    }
                }
                if (predID == nodeID) 
                    predID = Collections.max(nodeIDList);

                initInfo = nodeID + "/" + predID + "/" + nodeList[predID].getIP() + "/" + nodeList[predID].getPort() + "/" + Integer.toString(replicaFactor)+ "/" +numNodes;

            } catch (NoSuchAlgorithmException nsae){}


            return initInfo;

        } else {
            return "NACK";
        }
    } 

    public String getRandomNode() throws RemoteException {
        Random rand = new Random();
        int randID = rand.nextInt(nodeIDList.size());
        int index = nodeIDList.get(randID);
        String result = nodeList[index].getIP() + "," + nodeList[index].getPort();
        return result;
    }

    public String findNodeInfo(String id) throws RemoteException{
        String initInfo="";
        if(Integer.parseInt(id)>numDHT)
            return initInfo;
        if(nodeList[Integer.parseInt(id)]!=null)
            initInfo=nodeList[Integer.parseInt(id)].getIP()+"/"+nodeList[Integer.parseInt(id)].getPort();
        return initInfo;
    }

     public String getNumNodes() throws RemoteException {
        String result = ""+numNodes;
        return result;
    }

    public String deleteNode(String id, String ip, String port) throws RemoteException {
        //Node node_to_delete = new Node(id,ip,port);
 
            String result = "";
            
            System.out.println("Delete Node with id " + id+" : Supernode speaking...");
            System.out.println("------------"+Integer.parseInt(id));
            //nodeIDList.remove(Integer.parseInt(id));
            Iterator<Integer> iterator = nodeIDList.iterator();
            int cnt=0;
                while (iterator.hasNext()) {
                    int next = iterator.next();
                    if (next == Integer.parseInt(id)) {
                        //System.out.println("*=Found the node to delete in nodeIDList*");
                        nodeIDList.remove(cnt);
                        System.out.println("*Just removed it!*");
                        break;
                    }
                    cnt++;
                }

            //System.out.println(nodeList[Integer.parseInt(id)].getID()+"= Deleted!!");

            nodeList[Integer.parseInt(id)]=null;
            numNodes--;

            
            System.out.println("*** Now total number of nodes= " + numNodes);
            result="delete,"+id+","+ip+","+port;
            return result;
    } 

    public String getNodeList() throws RemoteException {
        String result = "";
        Collections.sort(nodeIDList);
        result = result + nodeIDList.size() + "/";
        Iterator<Integer> iterator = nodeIDList.iterator();
        while (iterator.hasNext()) {
            int next = iterator.next();
            result = result + nodeList[next].getID() + "," + nodeList[next].getIP() + "," + nodeList[next].getPort() + "/";
        }

        return result;
    }

    public void finishJoining(int id) throws RemoteException {
        System.out.println("*** Post Initiation Call: Node " +id + " is in the DHT.");
        System.out.println("Current number of nodes = " + numNodes + "\n");
        synchronized (this) {
            busy = 0;
        }
    }

    public static void main ( String args[] ) throws Exception
    {
        if (args.length != 1)
        {
            System.out.println
                ("Syntax - SuperNode [numNodes]");
            System.exit(1);
        }

        int maxNumNodes = Integer.parseInt(args[0]);
        m = (int) Math.ceil(Math.log(maxNumNodes) / Math.log(2));
        numDHT = (int)Math.pow(2,m);

        nodeList = new Node[numDHT];
        // Assign a security manager, in the event that dynamic
        // classes are loaded
        if (System.getSecurityManager() == null)
            System.setSecurityManager ( new RMISecurityManager() );

        busy = 0;

        // Create an instance of our power service server ...
        try {
            SuperNode svr = new SuperNode();

            // ... and bind it with the RMI Registry
            Naming.rebind ("SuperNodeDef", svr);
            System.out.println ("SuperNode started, service bound and waiting for connections ....");
            numNodes = 0;
            System.out.println ("Current number of nodes = " + numNodes + "\n");
		
		Scanner sc=new Scanner(System.in);
		
	System.out.println ("Give replica factor k (i dont check validation) : ");
            		
	for (;;) {
        	if (!sc.hasNextInt()) {
	    	    	System.out.println ("enter only integers!");
            		sc.next(); // discard
            		continue;
       	 	}
       		int  choose = sc.nextInt();
        	if (choose >= 0) {
			replicaFactor = choose;
            		System.out.println("no problem with input");

        	} else {
         	   	System.out.println("invalid inputs");
	
        	}
    		break;
  	}

		
        } catch (Exception e) {
            System.out.println ("Supernode Failed to start: " + e);
        }

    }
}

