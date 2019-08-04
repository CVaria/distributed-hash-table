import java.rmi.*;
import java.math.*;
import java.security.*;
import java.rmi.Naming;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.rmi.server.*;
import java.io.*;
import java.net.*;
import java.net.UnknownHostException;
import java.util.*;

//
//
// This is the Code for the Node that is part of the DHT. 
//
//

public class NodeDHT implements Runnable //extends UnicastRemoteObject implements NodeDHTInterface
{

    private int ID;
    private static SuperNodeDef service;
    //Comparator<Node> comparator = new MyComparator();

    private Socket connection;
    private static ServerSocket serverSocket = null; 
    private static int k, curr_nodes;
    private static Node me, pred;
    //static int m = 5;
    //static FingerTable[] finger = new FingerTable[m+1];
    //static int numDHT = (int)Math.pow(2,m);
    private static int m;
    private static FingerTable[] finger;
    private static int numDHT;
    private static List<Word> wordList = new ArrayList<Word>();
    private static List<Node> replList = new ArrayList<Node>();
    private static int IDcounter=0;

    public NodeDHT(Socket s, int i) {
        this.connection = s;
        this.ID = i;
    }
    
    

    public static void main(String args[]) throws Exception
    {
        System.out.println(" ***************************************************************************************************");
        // Check for hostname argument
        if (args.length < 3)
        {
            System.out.println("Syntax - NodeDHT [LocalPortnumber] [SuperNode-HostName] [numNodes]");
            System.out.println("         *** [LocaPortNumber] = is the port number which the Node will be listening waiting for connections.");
            System.out.println("         *** [SuperNode-HostName] = is the hostName of the SuperNode.");
            System.exit(1);
        }	

        int maxNumNodes = Integer.parseInt(args[2]);
        m = (int) Math.ceil(Math.log(maxNumNodes) / Math.log(2));
        finger = new FingerTable[m+1];
        numDHT = (int)Math.pow(2,m);

        System.out.println("The Node starts by connecting at the SuperNode.");
        System.out.println("Establishing connection to the SuperNode...");
        // Assign security manager
        if (System.getSecurityManager() == null)
        {
            System.setSecurityManager(new RMISecurityManager());
        }

        InetAddress myIP = InetAddress.getLocalHost();
        System.out.println("My IP: " + myIP.getHostAddress() + "\n");

        // Call registry for PowerService
        service = (SuperNodeDef) Naming.lookup("rmi://" + args[1] + "/SuperNodeDef");

        String initInfo = service.getNodeInfo(myIP.getHostAddress(),args[0]);
        if (initInfo.equals("NACK")) {
            System.out.println("NACK! SuperNode is busy. Try again in a few seconds...");
            System.exit(0);
        } else {
            System.out.println("Connection to the SuperNode established succefully");
            System.out.println("Now Joining the DHT network and receiving the Node ID.");	
        }

        String[] tokens = initInfo.split("/");
        me = new Node(Integer.parseInt(tokens[0]),myIP.getHostAddress(),args[0]);
        pred = new Node(Integer.parseInt(tokens[1]),tokens[2],tokens[3]);
	k = Integer.parseInt(tokens[4]);
        curr_nodes = Integer.parseInt(tokens[5]);
        System.out.println("My given Node ID is: "+me.getID() + ". Predecessor ID: " +pred.getID() +" replica factor: "+ Integer.toString(k)+" current nodes in chord: "+ curr_nodes);

        Socket temp = null;
        Runnable runnable = new NodeDHT(temp,0);
        Thread thread = new Thread(runnable);
        thread.start();

        int count = 1;
        System.out.println("Listening for connection from Client or other Nodes...");
        int port = Integer.parseInt(args[0]);

        try {
            serverSocket = new ServerSocket( port );
        } catch (IOException e) {
            System.out.println("Could not listen on port " + port);
            System.exit(-1);
        }

        while (true) {
         //   System.out.println( "*** Listening socket at:"+ port + " ***" );
            Socket newCon = serverSocket.accept();
            Runnable runnable2 = new NodeDHT(newCon,count++);
            Thread t = new Thread(runnable2);
            t.start();
        }
        //Start the Client for NodeDHT 	
    }

    public static String makeConnection(String ip, String port, String message) throws Exception {
        //System.out.println("Making connection to " + ip + " at " +port + " to " + message);
        if (me.getIP().equals(ip) && me.getPort().equals(port)){
            String response = considerInput(message);
            //System.out.println("local result " + message + " answer: "  + response);
            return response;
        } else {

            Socket sendingSocket = new Socket(ip,Integer.parseInt(port));
            DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
            BufferedReader inFromServer = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

            //System.out.println("Sending request: " + message + " to " + ip + " at " + port);
            out.writeBytes(message + "\n");

            String result = inFromServer.readLine();
            //System.out.println("From Server: " + result);
            out.close();
            inFromServer.close();
            sendingSocket.close(); 
            return result;
        }
    }


    public void run() {

        if (this.ID == 0) { //in case that this is our first thread running we construct the finger table

            System.out.println("Building Finger table ... ");
            for (int i = 1; i <= m; i++) {
                finger[i] = new FingerTable();
                finger[i].setStart((me.getID() + (int)Math.pow(2,i-1)) % numDHT);
            }
            for (int i = 1; i < m; i++) {
                finger[i].setInterval(finger[i].getStart(),finger[i+1].getStart()); 
            }
            finger[m].setInterval(finger[m].getStart(),finger[1].getStart()-1); 


            if (pred.getID() == me.getID()) { //if predecessor is same as my ID -> only node in DHT
                for (int i = 1; i <= m; i++) {
                    finger[i].setSuccessor(me);
                }
                System.out.println("Done, all finger tablet set as me (only node in DHT)");
            }
            else {
                for (int i = 1; i <= m; i++) {
                    finger[i].setSuccessor(me);
                }
                try{

			init_finger_table(pred);
            		update_others();
             
			//Update ArrayList

			String request = "findSuc/" + finger[1].getStart();
        		String result = makeConnection(pred.getIP(),pred.getPort(),request);
			
        		String[] tokens = result.split("/");
			//System.out.println("My successor for updating is "+tokens[0]);
        		Node tempSucc = new Node(Integer.parseInt(tokens[0]),tokens[1],tokens[2]);
			
			String getAll ="yes";
			if(k<curr_nodes){
				getAll="no";
			}

			String request1 = "updateValues/" + Integer.toString(me.getID()) + "/" + Integer.toString(tempSucc.getID())+ "/" + getAll;
			String result1 = makeConnection(tempSucc.getIP(), tempSucc.getPort(), request1);
		
			if(result1.length()!=0){
				String[] pairs = result1.split("/");
				//System.out.println("pairs are : " + pairs.length);
				int i;
				//create new node's  arrayList
				for(i=0; i < pairs.length; i++){
					String[] values = pairs[i].split("_");
			        	wordList.add(new Word(Integer.parseInt(values[0]),values[1] ,values[2]));
  
				}			
        			//printValues();

			}

            		//Initialize my replica_list
			String	r ="getNodeList/"+ Integer.toString(me.getID())+"/"+Integer.toString(k-1) ; 
			
			String res = makeConnection(finger[1].getSuccessor().getIP(), finger[1].getSuccessor().getPort(), r);
		
			if(res.length()!=0){
				String[] pairs = res.split("@");
				for(int i=0; i<pairs.length; i++){
					String[] nodeInfo = pairs[i].split(",");
					replList.add(new Node(Integer.parseInt(nodeInfo[0]), nodeInfo[1], nodeInfo[2]));
				}
			}

	
            		//Update others replica_lists, add myself and remove the last node of their lists 
			request = "getPred";
			result = makeConnection(me.getIP(), me.getPort(), request);
			String[] tokens1 = result.split("/");
			request = "addMe/"+Integer.toString(me.getID()) +"/" + me.getIP() + "/" + me.getPort() + "/" + Integer.toString(0) + "/" + Integer.toString(curr_nodes);
			result = makeConnection(pred.getIP(), pred.getPort(), request);
			

			// remove my keys if k< #kombon from last node
			if (k < curr_nodes){
				request ="remKeys/"+Integer.toString(k)+"/";
				
				//getMyKeys returns only my key values, these are the pairs we want to remove from kth node
				String myWords = getMyKeys();
					
				if(myWords.length()!=0){
					request = request + myWords;
					result = makeConnection(finger[1].getSuccessor().getIP(), finger[1].getSuccessor().getPort(), request);
				}
			}


               } catch (Exception e) {}
            }
            try { 
                service.finishJoining(me.getID());
            } catch (Exception e) {}
        }
        else {
            try {
              //  System.out.println( "*** A Client came; Service it *** " + this.ID );

                BufferedReader inFromClient =
                    new BufferedReader(new InputStreamReader(connection.getInputStream()));
                DataOutputStream outToClient = new DataOutputStream(connection.getOutputStream());
                String received = inFromClient.readLine();
                String response = considerInput(received);
              
                outToClient.writeBytes(response + "\n");
                String[] tokens = response.split("/");
                if(tokens[0].equals("delete")&&tokens[1].equals("successful"))
                    System.exit(1);	
            } catch (Exception e) {
                System.out.println("Thread cannot serve connection");
            }

        }
    }


    public static String considerInput(String received) throws Exception {
        String[] tokens = received.split("/");
        String outResponse = "";

        if (tokens[0].equals("setPred")) {
            Node newNode = new Node(Integer.parseInt(tokens[1]),tokens[2],tokens[3]);
            setPredecessor(newNode);
            outResponse = "set it successfully";	
        }
        else if (tokens[0].equals("getPred")) {
            Node newNode = getPredecessor();
            outResponse = newNode.getID() + "/" + newNode.getIP() + "/" + newNode.getPort() ;
        }
        else if (tokens[0].equals("tryDelete")) {
            if(leave(tokens[1],tokens[2],tokens[3],tokens[4])==1)
                outResponse = "delete/successful";
            else
                outResponse = "delete/unsuccessful";
        }
        else if (tokens[0].equals("findSuc")) {
            Node newNode = find_successor(Integer.parseInt(tokens[1]));
            outResponse = newNode.getID() + "/" + newNode.getIP() + "/" + newNode.getPort() ;
        }
        else if (tokens[0].equals("getSuc")) {
            Node newNode = getSuccessor();
            outResponse = newNode.getID() + "/" + newNode.getIP() + "/" + newNode.getPort() ;
        }
        else if (tokens[0].equals("closetPred")) {
            Node newNode = closet_preceding_finger(Integer.parseInt(tokens[1]));
            outResponse = newNode.getID() + "/" + newNode.getIP() + "/" + newNode.getPort() ;
        }
        else if (tokens[0].equals("updateFing")) {
            Node newNode = new Node(Integer.parseInt(tokens[1]),tokens[2],tokens[3]);
            update_finger_table(newNode,Integer.parseInt(tokens[4]));
            outResponse = "update finger " + Integer.parseInt(tokens[4]) + " successfully";	
        }
        else if (tokens[0].equals("remNod")) {
            Node source = new Node(Integer.parseInt(tokens[1]),tokens[2],tokens[3]);
            Node dest = new Node(Integer.parseInt(tokens[5]),tokens[6],tokens[7]);
            remove_node(source,Integer.parseInt(tokens[4]),dest);
            outResponse = "remove node " + Integer.parseInt(tokens[1]) + " successfully"; 
        }
        else if (tokens[0].equals("check")) {
            
            check_fingers(Integer.parseInt(tokens[1]),Integer.parseInt(tokens[2]),Integer.parseInt(tokens[3]),tokens[4],tokens[5]);
            outResponse = "checked fingers of " + me.getID() + " successfully"; 
        }
	else if(tokens[0].equals("updateValues")){
		outResponse = updateValues(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]), tokens[3]);
	    }

	else if (tokens[0].equals("printWordlist1")) {
	    	IDcounter++;
            	String request= "printWordlist/" +me.getIP()+"/"+me.getPort()+"/"+tokens[1]+"/"+" "+"/"+Integer.toString(IDcounter);
            	outResponse = makeConnection(me.getIP(),me.getPort(),request);
        }
        else if (tokens[0].equals("printWordlist")) {
           
            	outResponse = print_list(tokens[1],tokens[2],Integer.parseInt(tokens[3]),tokens[4], tokens[5]);
        }
        else if (tokens[0].equals("print")) {
            	outResponse = returnAllFingers();
        }
       // else if (tokens[0].equals("print_all")) {
       //     	outResponse = printWordList();
       // }
        else if (tokens[0].equals("tryInsert")){
		IDcounter++;
            	tryInsert(Integer.parseInt(tokens[1]),tokens[2],tokens[3], me.getIP(), me.getPort(), Integer.toString(IDcounter));
            	outResponse = "Inserted pair " + tokens[2] + "," + tokens[3] + " into DHT";
        }
        else if (tokens[0].equals("tryDeleteKey")){
		IDcounter++;
            	tryDeleteKey(Integer.parseInt(tokens[1]),tokens[2], me.getIP(), me.getPort(), Integer.toString(IDcounter));
            	outResponse = "Deleted word " + tokens[2]+ " from DHT";
        }
        else if(tokens[0].equals("addMe")){
		Node node = new Node (Integer.parseInt(tokens[1]), tokens[2], tokens[3]);
		fixReplicas(node, Integer.parseInt(tokens[4]), Integer.parseInt(tokens[5]));
		outResponse = "fixed";
	} 
	else if(tokens[0].equals("remKeys")){
		findLast(tokens[2], Integer.parseInt(tokens[1]));
		outResponse = "removed";	
	}
	else if(tokens[0].equals("getNodeList")){
		outResponse  = getNodeList(Integer.parseInt(tokens[1]),Integer.parseInt(tokens[2]));
	}
        else if (tokens[0].equals("insertKey")) {
            	insertKey(Integer.parseInt(tokens[1]),tokens[2],tokens[3], tokens[4], tokens[5], tokens[6]);
		outResponse = "inserted";
        }
        else if (tokens[0].equals("deleteKey")) {
           	deleteKey(Integer.parseInt(tokens[1]),tokens[2], tokens[3], tokens[4], tokens[5]);
	   	outResponse = "deleted";
        }
        else if (tokens[0].equals("lookupKey")){
		IDcounter++;
            	outResponse = lookupKey(Integer.parseInt(tokens[1]),tokens[2], me.getIP(), me.getPort(), Integer.toString(IDcounter));
        }
        else if (tokens[0].equals("getWord")) {
            	outResponse = getWord(tokens[1], tokens[2], tokens[3], tokens[4], tokens[5]);
        }
	else if (tokens[0].equals("addReplicas")){
    	    	addReplicas(Integer.parseInt(tokens[1]), tokens[2], tokens[3], Integer.parseInt(tokens[4]), Integer.parseInt(tokens[5]), tokens[6], tokens[7], tokens[8]);
    	    	outResponse = "Replicas done!";
	}
	else if (tokens[0].equals("updateReplicas")){
    	    	updateReplicas(Integer.parseInt(tokens[1]), tokens[2], tokens[3], Integer.parseInt(tokens[4]), Integer.parseInt(tokens[5]), tokens[6], tokens[7], tokens[8]);
    	    	outResponse = "Update Replicas done!";
	}
    	else if(tokens[0].equals("deleteReplicas")){
    		deleteReplicas(Integer.parseInt(tokens[1]), tokens[2], Integer.parseInt(tokens[3]), Integer.parseInt(tokens[4]), tokens[5], tokens[6], tokens[7]);
    		outResponse = "Replicas Deletion Done!";
    	}
	else if(tokens[0].equals("departUpdateReplicas")){
		departUpdateReplicas(Integer.parseInt(tokens[1]), tokens[2], tokens[3], Integer.parseInt(tokens[4]), Integer.parseInt(tokens[5]), Integer.parseInt(tokens[6]));
		outResponse = "Replicas got updated after depart";
	}
	else if(tokens[0].equals("departUpdateList")){
		departUpdateList(Integer.parseInt(tokens[1]), tokens[2], tokens[3], Integer.parseInt(tokens[4]), Integer.parseInt(tokens[5]));
		outResponse = "ReplList got updated after depart";
	}
	else if(tokens[0].equals("reply")){
		outResponse = "Answer for request with ID "+ tokens[1];
		System.out.println(outResponse);
	}
        //System.out.println("outResponse for " + tokens[0] + ": " + outResponse);
        return outResponse;
    }



	public static String print_list(String origIP, String origPort, int num_nodes,String response, String count) throws Exception{
          
            if(num_nodes<=0){
                    response = response +"\n";
                    Runnable runnable2 = new ThreadWorker(origIP, origPort, "reply/"+count+": (Query *) "+response);
                    Thread t = new Thread(runnable2);
                    t.start();
                    return response;
            }            
            Iterator<Word> iterator = wordList.iterator();
            while (iterator.hasNext()) {
                    Word wordScan = iterator.next();
                    response = response +"@"+ Integer.toString(wordScan.getKey()) + "," + wordScan.getWord() + "," + wordScan.getMeaning();
            }
            response=response+"@***NodeID="+ me.getID() + ", NodeIP=" + me.getIP() + ", NodePort=" + me.getPort()+"***";
            int nodes=num_nodes-1;
            String request= "printWordlist/"+origIP+"/"+origPort+"/"+nodes+"/"+response + "/" +count;
            Node successor = getSuccessor();
            return makeConnection(successor.getIP(),successor.getPort(),request);
    	}



	public static String getNodeList(int firstID, int m) throws Exception{
		String response;
		if((firstID==me.getID() )|| (m ==0)){
            		return "";
        	}            
      		else{
            		response = Integer.toString(me.getID()) + "," + me.getIP() + "," + me.getPort();
        		String request= "getNodeList/" + Integer.toString(firstID)+"/"+ Integer.toString(m-1);
        		return response + "@" + makeConnection(finger[1].getSuccessor().getIP(), finger[1].getSuccessor().getPort(),request);
  		}
	}




	public static void fixReplicas(Node node, int x, int current) throws Exception{
	
		if((node.getID()== me.getID()) || (x==k-1))
			return; 
		
		if(replList.size()!=0){
			Node last_Node = replList.get(replList.size()-1);	
			
			if(current > k){	
				String list = getMyKeys();
				//if i have keys to remove
				if(list.length()!=0){
					findLast(list, replList.size()+2);
				}
				replList.remove(replList.size()-1);

			}
		}

		replList.add(x, new Node (node.getID(), node.getIP(), node.getPort()));
				 	
		//i prosthiki omws theloume na ginei se olous tous katallilous kombous anexartitos an k> curr_nodes 
		String request1 = "addMe/"+ Integer.toString(node.getID())+"/"+ node.getIP()+"/"+node.getPort()+"/"+ Integer.toString(x+1)+"/"+ Integer.toString(current);
		makeConnection(pred.getIP(), pred.getPort(), request1); 
	}


	
	public static void findLast(String list, int m) throws Exception{
		//if i am the last one i remove the keys they gave me
		if(m==1){
			String[] pairs = list.split("@");
			Iterator<Word> iterator = wordList.iterator();
			while(iterator.hasNext()){
				Word w = iterator.next();
				for(int i=0; i<pairs.length; i++){
					if(pairs[i].equals(w.getWord())){
						iterator.remove();
					}
				}
			}	
		}
		else{
			//if i am not the last one i forward the message
			String request = "remKeys/"+Integer.toString(m-1)+"/"+list;
			makeConnection(finger[1].getSuccessor().getIP(), finger[1].getSuccessor().getPort(), request);
		}	
	}


	//it returns only my keys, not my replicas
	public static String getMyKeys() throws Exception{

		int lower = pred.getID();
		int upper = me.getID();
                String response = "";
                Iterator<Word> iterator = wordList.iterator();
                while (iterator.hasNext()) {
                        Word wordScan = iterator.next();
                        int wordKey = wordScan.getKey();
                        if (((wordKey > lower) && (wordKey <= upper))||((lower < wordKey) && (wordKey > upper) && (lower > upper) ) || ((wordKey < lower) && (upper < lower) && (wordKey <= upper)) ) {
					
                                        response = response + wordScan.getWord() +"@";
                                      
                                 }
                }
                return response;
	}



	// node will return  every value  that is not in (lower,upper] if k< current, else it returns everything
	public static String updateValues(int lower, int upper, String all){
		String response = "";
	        Iterator<Word> iterator = wordList.iterator();
		if(all.equals("yes")){
	    		while (iterator.hasNext()) {
            			Word wordScan = iterator.next();
            			int wordKey = wordScan.getKey();
        			response = response + wordScan.getKey() + "_" + wordScan.getWord() + "_" + wordScan.getMeaning()+"/";
        		}
	
		}
		else{
        		while (iterator.hasNext()) {
            			Word wordScan = iterator.next();
            			int wordKey = wordScan.getKey();
            			if (((wordKey > lower) && (wordKey <= upper))||((lower < wordKey) && (wordKey > upper) && (lower > upper) ) || ((wordKey < lower) && (upper < lower) && (wordKey <= upper)) ) {
				}	
				else{
        				response = response + wordScan.getKey() + "_" + wordScan.getWord() + "_" + wordScan.getMeaning()+"/";
				}
        		}
		}
		
		//printValues();
 		return response;	
	}


    

	public static String printWordList(){
        	String response = ""+wordList.size()+"/";
        	Iterator<Word> iterator = wordList.iterator();
        	while (iterator.hasNext()) {
            		Word wordScan = iterator.next();
            		response = response +Integer.toString(wordScan.getKey())+","+wordScan.getWord() + "," + wordScan.getMeaning()+"/";
        	}
        	return response;    
    	}


	public static void printValues(){
		System.out.println("I have ID : "+ me.getID()+" and my List has:\n");
		for(Word w : wordList){
			System.out.println("key: " + w.getKey() + " word: " + w.getWord() + " meaning: " + w.getMeaning() + "\n");
		}
	}



    	public static String getWord(String word, String origIP, String  origPort, String note, String count){
		boolean found = false;
		String request="";
		String response="";
        	Iterator<Word> iterator = wordList.iterator();
        	while (iterator.hasNext()) {
            		Word wordScan = iterator.next();
            		String wordMatch = wordScan.getWord();
            		if (word.equals(wordMatch)) { 
				found = true;
                		response = me.getID() + "/" + wordScan.getMeaning(); 
            		}
        	}

		if(found){
			String[] chunks;
			chunks = response.split("/");
			request = "reply/" + count + ": " + word +" with value  " + chunks[1] + " found in node  " + chunks[0];
		}
		else{
			if(note.equals("second")){
        			response = count + ": "+ word +" was not found!";
				request = "reply/" + response;
			}
			response = "No Word Found!";
		}
		if(found || note.equals("second")){
			Runnable runnable2 = new ThreadWorker(origIP, origPort, request);
           		Thread t = new Thread(runnable2);
            		t.start();
       
		}
        	return response;
    	}


    	public static String lookupKey(int key, String word, String origIP, String origPort, String count) throws Exception {
            //Don't need necessarily to find successor due to eventual consistency
            //start looking to my replicas to check if i already have the word
            String request = "getWord/" +  word +"/" +origIP +"/" +origPort + "/"+ "first" +"/" + count;
            String response = "";
            response = makeConnection(me.getIP(),me.getPort(),request);
            if (response.equals("No Word Found!")) {
                
            	Node destNode = find_successor(key);
            	request = "getWord/" +  word  +"/" +origIP +"/" +origPort +"/"+ "second" + "/" + count;
            	response = "";
            	response = makeConnection(destNode.getIP(),destNode.getPort(),request);
            }
            else{
                //System.out.println("I already have the word you are looking for!");
	     }
        	return response;
    	}


    	public static void tryInsert(int key, String word, String meaning, String origIP, String origPort, String count) throws Exception {
        	Node destNode = find_successor(key);
        	String request = "insertKey/" + key + "/" +  word + "/" + meaning+ "/" + origIP + "/" + origPort + "/" + count;
        	makeConnection(destNode.getIP(),destNode.getPort(),request);
    	}


    	public static void tryDeleteKey(int key, String word, String origIP, String origPort, String count) throws Exception {
        	Node destNode = find_successor(key);
        	String request = "deleteKey/" + key + "/" +  word +"/" +origIP +"/" +origPort +"/" + count;
        	makeConnection(destNode.getIP(),destNode.getPort(),request);
    	}


    	public static void insertKey(int key, String word, String meaning, String origIP, String origPort, String count) throws Exception { 
		String req="";
		String request="";
		boolean notExists = true;
		Runnable runnable2, run1;
		Thread t;
		for(Word w : wordList){
			if(w.getWord().equals(word)){
				notExists = false;
				w.setMeaning(meaning);
               			 
                //*****changes for eventual consistency****
				request = "updateReplicas/" + key + "/" + word + "/" + meaning + "/" + Integer.toString(k-1) + "/" + Integer.toString(me.getID()) + "/" + origIP + "/" + origPort +"/" + count;
		
		//eventually update		
				req ="reply/" + count + " : " + word +" was updated!"; 
				runnable2 = new ThreadWorker(finger[1].getSuccessor().getIP(), finger[1].getSuccessor().getPort(), request);
                		t = new Thread(runnable2);
                		t.start();
                		//makeConnection(finger[1].getSuccessor().getIP(), finger[1].getSuccessor().getPort(), request);
				break;
			}
		}
		if(notExists){
        		wordList.add(new Word(key,word,meaning));
            	
			req ="reply/" + count +" :" + word +" was added!"; 
            		//*****change for eventual consistency****
            		request = "addReplicas/" + key + "/" + word + "/" + meaning + "/" + Integer.toString(k-1) + "/" + Integer.toString(me.getID()) + "/" + origIP + "/" + origPort +"/" +count;
		
			//eventually added
            		runnable2 = new ThreadWorker(finger[1].getSuccessor().getIP(), finger[1].getSuccessor().getPort(), request);
            		t = new Thread(runnable2);
            		t.start();
			//makeConnection(finger[1].getSuccessor().getIP(), finger[1].getSuccessor().getPort(), request);		
    		}
		
		run1 = new ThreadWorker(origIP, origPort, req);
        	t = new Thread(run1);
            	t.start();
		
    	}


    	public static void addReplicas(int key, String word, String meaning, int k, int coordID, String origIP, String origPort, String count) throws Exception{
		String request="";
		//check if k > numDHT or if we are done
		if((me.getID()==coordID) || k == 0 ){
			return;	
		}else{
			wordList.add(new Word(key, word, meaning));
			request = "addReplicas/" + Integer.toString(key) + "/" + word + "/" + meaning + "/" + Integer.toString(k-1) + "/" + Integer.toString(coordID) + "/" + origIP +"/" +origPort +"/" + count;
			makeConnection(finger[1].getSuccessor().getIP(), finger[1].getSuccessor().getPort(), request);
		}
	}


	public static void updateReplicas(int key, String word, String meaning, int k, int coordID, String origIP, String origPort, String count) throws Exception{
		String request="";
		if(me.getID()==coordID || k==0){
			return;
		} else {
			for(Word x : wordList){
				if(x.getWord().equals(word)){
					x.setMeaning(meaning);
					request = "updateReplicas/" + Integer.toString(key) + "/" + word + "/" + meaning + "/" + Integer.toString(k-1) + "/" + Integer.toString(coordID) + "/" + origIP + "/" + origPort + "/" + count;
					makeConnection(finger[1].getSuccessor().getIP(), finger[1].getSuccessor().getPort(), request);
				}
			}	
		}
	} 


    	public static void deleteKey(int key, String word, String origIP, String origPort, String count) throws Exception { 
        	String request="";
        	boolean found=false;
        	Iterator<Word> iterator = wordList.iterator();
        	while (iterator.hasNext()) {
            		Word wordScan = iterator.next();
            		String wordMatch = wordScan.getWord();
            		if (word.equals(wordMatch)) { 
        			found = true;
		    		iterator.remove();
	    			request = "deleteReplicas/" + Integer.toString(key) + "/" + word + "/" + Integer.toString(k-1) + "/" + Integer.toString(me.getID()) + "/" + origIP + "/" + origPort +"/" + count; 
	    			makeConnection(finger[1].getSuccessor().getIP(), finger[1].getSuccessor().getPort(), request);
	    			break;
              		}
		}
		if(!found){
			request ="reply/" + count + ": "+ word +" for deletion was not found!";
			Runnable runnable2 = new ThreadWorker(origIP, origPort, request);
        	   	Thread t = new Thread(runnable2);
            		t.start();
		
		}
    	}


    	public static void deleteReplicas(int key, String word, int k, int coordID, String origIP, String origPort, String count) throws Exception{
		//check if k > numDHT or we are done
		//System.out.println("myID = "+ Integer.toString(me.getID())+" coordID = "+Integer.toString(coordID));
		String request="";
		if((me.getID()==coordID) || k == 0 ){
			request ="reply/" + count + ": "+ word +" was deleted!";
			Runnable runnable2 = new ThreadWorker(origIP, origPort, request);
        	   	Thread t = new Thread(runnable2);
            		t.start();
			return;	
		}else{
			Iterator<Word> iterator = wordList.iterator();
			while(iterator.hasNext()){
				Word wordScan = iterator.next();
				String wordMatch = wordScan.getWord();
				if(word.equals(wordMatch)){
					iterator.remove();
							
					request = "deleteReplicas/" + Integer.toString(key) + "/" + word + "/" + Integer.toString(k-1) + "/" + Integer.toString(coordID) +"/" + origIP +"/" +origPort +"/" +count;
					makeConnection(finger[1].getSuccessor().getIP(), finger[1].getSuccessor().getPort(), request);
					break;
				}
			}
		}
	}



	public static void departUpdateReplicas(int key, String word, String meaning, int k, int coordID, int firstID) throws Exception{
		//check if k > numDHT or we are done
		//System.out.println("myID = "+ Integer.toString(me.getID())+" coordID = "+Integer.toString(coordID) +" and k=" + k);
		//check if the node has the given word, add it if not
		boolean NotFound=true;
		for(Word w: wordList){
			if(w.getWord().equals(word)){
				NotFound = false;
				break;
			}
		}
		if(NotFound){
			//System.out.println(" I didnt find it, i will add it");
			wordList.add(new Word(key, word, meaning));
			return;
		}
		//check if we finished
		if( k == 0 || (me.getID()==coordID)){
			return;
		}else{
			String request = "departUpdateReplicas/" + Integer.toString(key) + "/" + word + "/" + meaning + "/" + Integer.toString(k-1) + "/" + Integer.toString(coordID) + "/" + Integer.toString(firstID);
			makeConnection(finger[1].getSuccessor().getIP(), finger[1].getSuccessor().getPort(), request);
		}		
	}


	public static void departUpdateList(int nodeID, String nodeIP, String nodePort,int position, int m) throws Exception{
		//System.out.println(" My id is " + Integer.toString(me.getID()) +" k= "+ Integer.toString(m));
		
		if(m==0){
			//System.out.println("I will remove node in position i ="  + Integer.toString(position));
			int size = replList.size();
			replList.remove(position);

			//in case k>=current we just remove leaving node, otherwise we add a new one 
			if(me.getID()!= nodeID){
				replList.add(new Node(nodeID, nodeIP, nodePort));
			}
		
			return;
		}
		else{
			String request = "departUpdateList/" + Integer.toString(nodeID) +"/" + nodeIP + "/" + nodePort  + "/"+ Integer.toString(position) +"/" + Integer.toString(m-1); 
			makeConnection(pred.getIP(), pred.getPort(), request);
		}

	}



    	public static String returnAllFingers(){
        	String response = "";
        	response = response + pred.getID() + "/" + pred.getIP() + "," + pred.getPort() + "/";
        	response = response + wordList.size() + "/";
        	for (int i = 1; i <= m; i++) {
            		response = response + finger[i].getStart() + "/" + finger[i].getSuccessor().getID() + "/" 
                		+ finger[i].getSuccessor().getIP() + "," + finger[i].getSuccessor().getPort() + "/";
        	}
        	return response;
    	}



    public static void init_finger_table(Node n) throws Exception {
        int myID, nextID;

        String request = "findSuc/" + finger[1].getStart();
        String result = makeConnection(n.getIP(),n.getPort(),request);
        //System.out.println("Asking node " + n.getID() + " at " + n.getIP());

        String[] tokens = result.split("/");
        finger[1].setSuccessor(new Node(Integer.parseInt(tokens[0]),tokens[1],tokens[2]));
        //printAllFingers();

        String request2 = "getPred";
        String result2 = makeConnection(finger[1].getSuccessor().getIP(),finger[1].getSuccessor().getPort(),request2);
        String[] tokens2 = result2.split("/");
        pred = new Node(Integer.parseInt(tokens2[0]),tokens2[1],tokens2[2]);

        String request3 = "setPred/" + me.getID() + "/" + me.getIP() + "/" + me.getPort();
        makeConnection(finger[1].getSuccessor().getIP(),finger[1].getSuccessor().getPort(),request3);

        int normalInterval = 1;
        for (int i = 1; i <= m-1; i++) {

            myID = me.getID();
            nextID = finger[i].getSuccessor().getID(); 

            if (myID >= nextID)
                normalInterval = 0;
            else normalInterval = 1;

            if ( (normalInterval==1 && (finger[i+1].getStart() >= myID && finger[i+1].getStart() <= nextID))
                    || (normalInterval==0 && (finger[i+1].getStart() >= myID || finger[i+1].getStart() <= nextID))) {

                finger[i+1].setSuccessor(finger[i].getSuccessor());
            } else {

                String request4 = "findSuc/" + finger[i+1].getStart();
                String result4 = makeConnection(n.getIP(),n.getPort(),request4);
                String[] tokens4 = result4.split("/");

                int fiStart = finger[i+1].getStart();
                int succ = Integer.parseInt(tokens4[0]); 
                int fiSucc = finger[i+1].getSuccessor().getID();
                if (fiStart > succ) 
                    succ = succ + numDHT;
                if (fiStart > fiSucc)
                    fiSucc = fiSucc + numDHT;

                if ( fiStart <= succ && succ <= fiSucc ) {
                    finger[i+1].setSuccessor(new Node(Integer.parseInt(tokens4[0]),tokens4[1],tokens4[2]));
                }
            }
        }
    }

    
	
    public static int leave(String id, String ip, String port, String numNodes)throws Exception {
        //System.out.println("Leave: Numnode are "+numNodes);
        //*******Copy Word list to successor*********** 

        Node successor=getSuccessor();
        Node cp_node;
        int suc_id=successor.getID();
        if(suc_id==me.getID()){
            cp_node=new Node(pred.getID(),pred.getIP(),pred.getPort());
        }
        else
            cp_node=new Node(successor.getID(),successor.getIP(),successor.getPort());

        //************Start updating finger tables
        if (me.getID() == suc_id) {
      
            return 1; // it is the last node in the cloud
         }
       
        String request = "setPred/" +  pred.getID()+"/"+pred.getIP()+"/"+pred.getPort();
        String response = "";
        response = makeConnection(successor.getIP(),successor.getPort(),request);

         for (int i=1; i<=m; i++) {
            Node p = find_predecessor(Math.abs(me.getID()-(int)Math.pow(2,i-1)+1) % numDHT);
            String request1 = "remNod/" +me.getID()+"/"+me.getIP()+"/"+me.getPort()+"/"+i+"/"+successor.getID()+"/"+successor.getIP()+"/"+successor.getPort();
            String response1 = "";
            response1 = makeConnection(p.getIP(),p.getPort(),request1);
            
         }

         //***********Extra check for fingers!!******//
        
        int newNodes=Integer.parseInt(numNodes)+1;
        String request2 = "check/"+me.getID()+"/"+newNodes+"/"+getSuccessor().getID()+"/"+getSuccessor().getIP()+"/"+getSuccessor().getPort();
        String response2 = "";
        response2 = makeConnection(getSuccessor().getIP(),getSuccessor().getPort(),request2);
    

	//********************Replicas Update**************************
	for(Word w: wordList){
	 	String request3 = "departUpdateReplicas/" + Integer.toString(w.getKey()) + "/" + w.getWord() + "/" + w.getMeaning() + "/" + Integer.toString(k) + "/" + Integer.toString(pred.getID()) + "/" + finger[1].getSuccessor().getID();
		makeConnection(finger[1].getSuccessor().getIP(), finger[1].getSuccessor().getPort(), request3);		

	}

	//update replList
	int limit = replList.size();
	for(int i=0; i < limit; i++){ 
		
		String req = "departUpdateList/" + Integer.toString(replList.get(i).getID()) +"/" + replList.get(i).getIP() + "/" + replList.get(i).getPort() + "/" +Integer.toString(limit-1-i) + "/" +Integer.toString(limit-1-i); 
		//System.out.println("I send request " + req);
		makeConnection(pred.getIP(), pred.getPort(), req);
	}

        return 1;

    }




    public static void remove_node(Node n, int i, Node repl)throws Exception {
         
         if (finger[i].getSuccessor().getID() == n.getID()) {
             finger[i].setSuccessor(repl);
             String request = "remNod/" +n.getID()+"/"+n.getIP()+"/"+n.getPort()+"/"+i+"/"+repl.getID()+"/"+repl.getIP()+"/"+repl.getPort();
             String response = "";
             Node p = find_predecessor(me.getID());
             response = makeConnection(p.getIP(),p.getPort(),request);
         }
         
     }


    public static void update_others() throws Exception{
        Node p;
        for (int i = 1; i <= m; i++) {
            int id = me.getID() - (int)Math.pow(2,i-1) + 1;
            if (id < 0)
                id = id + numDHT; 

            p = find_predecessor(id);

            String request = "updateFing/" + me.getID() + "/" + me.getIP() + "/" + me.getPort() + "/" + i;  
            makeConnection(p.getIP(),p.getPort(),request);

        }
    }


    public static void check_fingers(int node_id,int num_nodes,int repl_id,String repl_ip, String repl_port)throws Exception {
         Node repl_nd=new Node(repl_id,repl_ip,repl_port);
         if(num_nodes<=0){
            return;
         }
            
         if (me.getID() != node_id) {
         for(int i=1;i<=m;i++){
            if(finger[i].getSuccessor().getID()==node_id)
                finger[i].setSuccessor(repl_nd);
         }
         int new_nodes=num_nodes-1;
        String request2 = "check/"+node_id+"/"+new_nodes+"/" +repl_id+"/"+repl_ip+"/"+repl_port;
        String response2 = "";
        //System.out.println("I am node "+me.getID()+" and my succ is "+getSuccessor().getID());
        response2 = makeConnection(getSuccessor().getIP(),getSuccessor().getPort(),request2);
    }
         
  }


    public static void update_finger_table(Node s, int i) throws Exception {

               Node p;
               int normalInterval = 1;
               int myID = me.getID();
               int nextID = finger[i].getSuccessor().getID();
               if (myID >= nextID) 
                   normalInterval = 0;
               else normalInterval = 1;

               //System.out.println("here!" + s.getID() + " between " + myID + " and " + nextID);

               if ( ((normalInterval==1 && (s.getID() >= myID && s.getID() < nextID)) ||
                           (normalInterval==0 && (s.getID() >= myID || s.getID() < nextID)))
                       && (me.getID() != s.getID() ) ) {

                   finger[i].setSuccessor(s);
                   p = pred;

                   String request = "updateFing/" + s.getID() + "/" + s.getIP() + "/" + s.getPort() + "/" + i;  
                   makeConnection(p.getIP(),p.getPort(),request);
               }
               //printAllFingers();
    }

    public static void setPredecessor(Node n) 
    {
        pred = n;
    }

    public static Node getPredecessor() 
    {
        return pred;
    }

    public static Node find_successor(int id) throws Exception 
           {
          //     System.out.println("Visiting here at Node <" + me.getID()+"> to find successor of key ("+ id +")"); 

               Node n;
               n = find_predecessor(id);

               String request = "getSuc/" ;
               String result = makeConnection(n.getIP(),n.getPort(),request);
               String[] tokens = result.split("/");
               Node tempNode = new Node(Integer.parseInt(tokens[0]),tokens[1],tokens[2]);
               return tempNode;
    }

    public static Node find_predecessor(int id)  throws Exception
    {
        Node n = me;
        int myID = n.getID();
        int succID = finger[1].getSuccessor().getID();
        int normalInterval = 1;
        if (myID >= succID)
            normalInterval = 0;

        while ((normalInterval==1 && (id <= myID || id > succID)) ||
                (normalInterval==0 && (id <= myID && id > succID))) {


            String request = "closetPred/" + id ;
            String result = makeConnection(n.getIP(),n.getPort(),request);
            String[] tokens = result.split("/");

            n = new Node(Integer.parseInt(tokens[0]),tokens[1],tokens[2]);

            myID = n.getID();

            String request2 = "getSuc/" ;
            String result2 = makeConnection(n.getIP(),n.getPort(),request2);
            String[] tokens2 = result2.split("/");

            succID = Integer.parseInt(tokens2[0]);

            if (myID >= succID) 
                normalInterval = 0;
            else normalInterval = 1;
                }

        return n;
    }

    public static Node getSuccessor() 
    {
        return finger[1].getSuccessor();
    }

    public static Node closet_preceding_finger(int id) 
    {
        int normalInterval = 1;
        int myID = me.getID();
        if (myID >= id) {
            normalInterval = 0;
        }

        for (int i = m; i >= 1; i--) {
            int nodeID = finger[i].getSuccessor().getID();
            if (normalInterval == 1) {
                if (nodeID > myID && nodeID < id) 
                    return finger[i].getSuccessor();
            } else {
                if (nodeID > myID || nodeID < id) 
                    return finger[i].getSuccessor();
            }
        }
        return me;
    }

}

