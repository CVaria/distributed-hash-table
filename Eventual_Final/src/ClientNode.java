import java.security.*;
import java.math.*;
import java.rmi.*;
import java.rmi.Naming;
import java.io.*;
import java.util.*;
import java.net.*;

public class ClientNode 
{
    private static SuperNodeDef service; 
    private static int m;
    private static int numDHT;

    public static void insert_from_file(){
        File fileSample = null;
        BufferedReader buff = null;
        String line = "";
        String[] stringSplit = null;
        //SortedMap<String, String>       sortedMapWords  = null;

        try{
            try{
                System.out.print ("Read from file: ");
                DataInputStream din = new DataInputStream (System.in);
                String file = din.readLine();
                fileSample = new File (file);
                buff = new BufferedReader( new FileReader (fileSample));
                //sortedMapWords  = new TreeMap<String, String>();

                int count = 0;
                while(( line = buff.readLine()) != null){
                    stringSplit = line.split(",");
                    //sortedMapWords.put(stringSplit[0], stringSplit[1]);

                    MessageDigest md = MessageDigest.getInstance("SHA1");
                    md.reset();
                    md.update(stringSplit[0].getBytes());
                    byte[] hashBytes = md.digest();
                    BigInteger hashNum = new BigInteger(1,hashBytes);
                    int key = Math.abs(hashNum.intValue()) % numDHT;  
                    //System.out.println("String 1=> "+stringSplit[0] + " || String 2=> "+ key);

                    String response = service.getRandomNode();
                    String[] token = response.split(",");
                    String message = key + "/" + stringSplit[0] + "/" + stringSplit[1];
                    insertKeyDHT(token[0],token[1],message);
                }
            }
            finally{
                buff.close();
            }
        } catch (Exception ae){
            System.err.println("Error reading the file");
            ae.printStackTrace();
        }
    }

    public static void query_from_file(){
        File fileSample = null;
        BufferedReader buff = null;
        String line = "";
        String[] stringSplit = null;
        //SortedMap<String, String>       sortedMapWords  = null;

        try{
            try{
                System.out.print ("Read from file: ");
                DataInputStream din = new DataInputStream (System.in);
                String file = din.readLine();
                fileSample = new File (file);
                buff = new BufferedReader( new FileReader (fileSample));
                //sortedMapWords  = new TreeMap<String, String>();
                while(( line = buff.readLine()) != null){
                    System.out.println ("Lookup for this word: "+line);
                    String wordLookup = line;


                    MessageDigest md2 = MessageDigest.getInstance("SHA1");
                    md2.reset();
                    md2.update(wordLookup.getBytes());
                    byte[] hashBytes2 = md2.digest();
                    BigInteger hashNum2 = new BigInteger(1,hashBytes2);
                    int key2 = Math.abs(hashNum2.intValue()) % numDHT;  

                    System.out.println("Hashed key: " + key2);

                    String response2 = service.getRandomNode();
                    String[] token2 = response2.split(",");
                    String message2 = key2 + "/" + wordLookup;
                    lookupKeyDHT(token2[0],token2[1],message2);
                    System.out.println("");
                }
            }
            finally{
                buff.close();
            }
        } catch (Exception ae){
            System.err.println("Error reading the file");
            ae.printStackTrace();
        }
    }
    
    public static void read_requests_from_file(){
    File fileSample = null;
    BufferedReader buff = null;
    String line = "";
    String[] stringSplit = null;
    //SortedMap<String, String>       sortedMapWords  = null;

    try{
        try{
            System.out.print ("Read from file: ");
            DataInputStream din = new DataInputStream (System.in);
            String file = din.readLine();
            fileSample = new File (file);
            buff = new BufferedReader( new FileReader (fileSample));
            //sortedMapWords  = new TreeMap<String, String>();
            while(( line = buff.readLine()) != null){
                stringSplit = line.split(",");
                
                if (stringSplit[0].equals("insert")){
                    //sortedMapWords.put(stringSplit[0], stringSplit[1]);

                    MessageDigest md = MessageDigest.getInstance("SHA1");
                    md.reset();
                    md.update(stringSplit[1].getBytes());
                    byte[] hashBytes = md.digest();
                    BigInteger hashNum = new BigInteger(1,hashBytes);
                    int key = Math.abs(hashNum.intValue()) % numDHT;  
                    //System.out.println("String 1=> "+stringSplit[0] + " || String 2=> "+ key);

                    String response = service.getRandomNode();
                    String[] token = response.split(",");
                    String message = key + "/" + stringSplit[1] + "/" + stringSplit[2];
                    insertKeyDHT(token[0],token[1],message);
                }
                else if(stringSplit[0].equals("query")){
                    System.out.println ("Lookup for this word: "+stringSplit[1]);
                    String wordLookup = stringSplit[1];

                    MessageDigest md2 = MessageDigest.getInstance("SHA1");
                    md2.reset();
                    md2.update(wordLookup.getBytes());
                    byte[] hashBytes2 = md2.digest();
                    BigInteger hashNum2 = new BigInteger(1,hashBytes2);
                    int key2 = Math.abs(hashNum2.intValue()) % numDHT;  

                    System.out.println("Hashed key: " + key2);

                    String response2 = service.getRandomNode();
                    String[] token2 = response2.split(",");
                    String message2 = key2 + "/" + wordLookup;
                    lookupKeyDHT(token2[0],token2[1],message2);
                    System.out.println("");
                }
                else {
                    System.out.println("Invalid request");
                    System.out.println("");
                }
            }
        }
        finally{
            buff.close();
        }
    } catch (Exception ae){
        System.err.println("Error reading the file");
        ae.printStackTrace();
    }
    }

    public static void lookupKeyDHT(String ip, String port, String message) throws Exception{
        Socket sendingSocket = new Socket(ip,Integer.parseInt(port));
        DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
        BufferedReader inFromServer = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

        out.writeBytes("lookupKey/" + message + "\n");

        String result = inFromServer.readLine();
        String[] token = result.split("/");
        if (!result.equals("No Word Found!"))
            System.out.println("Lookup result: the meaning is <" + token[1] + "> found in Node " + token[0]);
        else System.out.println(result);
        out.close();
        inFromServer.close();
        sendingSocket.close(); 
    }

    public static void insertKeyDHT(String ip, String port, String message) throws Exception{
        Socket sendingSocket = new Socket(ip,Integer.parseInt(port));
        DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
        BufferedReader inFromServer = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

        out.writeBytes("tryInsert/" + message + "\n");

        String result = inFromServer.readLine();
        System.out.println(result);
        out.close();
        inFromServer.close();
        sendingSocket.close(); 
    }

    public static void deleteKeyDHT(String ip, String port, String message) throws Exception{
        Socket sendingSocket = new Socket(ip,Integer.parseInt(port));
        DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
        BufferedReader inFromServer = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

        out.writeBytes("tryDeleteKey/" + message + "\n");

        String result = inFromServer.readLine();
        System.out.println(result);
        out.close();
        inFromServer.close();
        sendingSocket.close(); 
    }

    public static void getEachNode(String id, String ip, String port) throws Exception{
        Socket sendingSocket = new Socket(ip,Integer.parseInt(port));
        DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
        BufferedReader inFromServer = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

        out.writeBytes("print" + "\n");

        String result = inFromServer.readLine();
        String[] token = result.split("/");

        System.out.println("NODE:<" + id + "> at " + ip + ":" + port + " **************");
        System.out.println("\tPredecessor: " + token[0] + " at " + token[1]); 
        System.out.println("\tContains: " + token[2] + " word:meaning pairs at this node");
        for (int i = 1,j=3; i <= m; i++,j+=3) {
            System.out.println("\tFinger[" + i + "] starts at " + token[j] + "\thas Successor Node ("
                    + token[j+1] + ")\tat " + token[j+2]);
        }
        System.out.println("");
        out.close();
        inFromServer.close();
        sendingSocket.close(); 
    }

    public static void printAllNodeInfo() throws Exception{
        String nodeList = service.getNodeList();
        String[] tokens = nodeList.split("/");
        for (int i = 1; i <= Integer.parseInt(tokens[0]); i++) {
            String[] nodeTok = tokens[i].split(",");
            getEachNode(nodeTok[0], nodeTok[1], nodeTok[2]);
        }
    }

    public static void print_all() throws Exception{
        String response = service.getRandomNode();
        String[] token = response.split(",");

        String numNodes = service.getNumNodes();
        System.out.println("print_all: Just took random node from Server -> "+token[0]+" and "+token[1]);
        
        Socket sendingSocket = new Socket(token[0],Integer.parseInt(token[1]));
        DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
        BufferedReader inFromServer = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));
        
        System.out.println("we are sending numNodes= "+numNodes);
        
        out.writeBytes("printWordlist1/"+numNodes+"\n");
        System.out.println("It looks like we are done!");
        String result;
        result = inFromServer.readLine();
        String[] token2 = result.split("@");
        int i=token2.length-1;
        while(i>0){
            System.out.println(token2[i]);
            i--;
        }
        out.close();
        inFromServer.close();
        sendingSocket.close(); 
    }

    public static void delete_and_update(String id, String ip, String port,String numNodes) throws Exception{
        //String new_port=""+port;
        //String new_ip=""+ip;
        System.out.println("numNodes="+numNodes);
        Socket sendingSocket = new Socket(ip,Integer.parseInt(port));
        DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
        BufferedReader inFromServer = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

        out.writeBytes("tryDelete/" + id + "/"+ip+"/"+port+"/"+numNodes+"\n");

        String result = inFromServer.readLine();
        System.out.println(result);
        out.close();
        inFromServer.close();
        sendingSocket.close(); 
    }


    public static void main(String args[]) throws Exception
    {
        // Check for hostname argument
        if (args.length != 2)
        {
            System.out.println
                ("Syntax - ClientNode [Supernode's IP] [maxNumNodes]");
            System.exit(1);
        }

        int maxNumNodes = Integer.parseInt(args[1]);
        m = (int) Math.ceil(Math.log(maxNumNodes) / Math.log(2));
        numDHT = (int)Math.pow(2,m);

        // Assign security manager
        if (System.getSecurityManager() == null)
        {
            System.setSecurityManager
                (new RMISecurityManager());
        }

        // Call registry for PowerService
        service = (SuperNodeDef) Naming.lookup("rmi://" + args[0] + "/SuperNodeDef");

        DataInputStream din = new DataInputStream (System.in);

        for (;;)
        {
            System.out.println(" 1 - Look up for a Word in DHT");            
            System.out.println(" 2 - Insert a Word into DHT");
            System.out.println(" 3 - Delete a key from DHT");
            System.out.println(" 4 - Insert Words into DHT from File");
            System.out.println(" 5 - Lookup for Words in DHT from File");
            System.out.println(" 6 - Read requests from File");
            System.out.println(" 7 - Insert NodeID to depart");
            System.out.println(" 8 - Print DHT Structure - All Nodes Info");
            System.out.println(" 9 - Exit"); 
            System.out.println();

            System.out.print ("Choice : ");

            String line = din.readLine();
            if (line.equals("1")) {
                System.out.print ("Lookup for this word: ");
                String wordLookup = din.readLine();
                if(wordLookup.equals("*"))
                    print_all();
                else{
                    MessageDigest md2 = MessageDigest.getInstance("SHA1");
                    md2.reset();
                    md2.update(wordLookup.getBytes());
                    byte[] hashBytes2 = md2.digest();
                    BigInteger hashNum2 = new BigInteger(1,hashBytes2);
                    int key2 = Math.abs(hashNum2.intValue()) % numDHT;  

                    System.out.println("Hashed key: " + key2);

                    String response2 = service.getRandomNode();
                    String[] token2 = response2.split(",");
                    String message2 = key2 + "/" + wordLookup;
                    lookupKeyDHT(token2[0],token2[1],message2);
                }
                System.out.println("");
            }
            else if (line.equals("2")) {
                System.out.print ("Tell me the word you want to insert: ");
                String wordInput = din.readLine();					

                MessageDigest md3 = MessageDigest.getInstance("SHA1");
                md3.reset();
                md3.update(wordInput.getBytes());
                byte[] hashBytes3 = md3.digest();
                BigInteger hashNum3 = new BigInteger(1,hashBytes3);
                int key3 = Math.abs(hashNum3.intValue()) % numDHT;  

                System.out.println("Hashed key: " + key3);
                System.out.print ("Tell me the meaning of this word: ");
                String meaningInput = din.readLine();

                // Call remote method
                String response3 = service.getRandomNode();
                String[] token3 = response3.split(",");
                String message3 = key3 + "/" + wordInput + "/" + meaningInput;
                insertKeyDHT(token3[0],token3[1],message3);
                System.out.println("");
            }
            else if (line.equals("3")) {
                System.out.print ("Tell me the key-word you want to delete: ");
                String wordInput = din.readLine();

                MessageDigest md3 = MessageDigest.getInstance("SHA1");
                md3.reset();
                md3.update(wordInput.getBytes());
                byte[] hashBytes3 = md3.digest();
                BigInteger hashNum3 = new BigInteger(1,hashBytes3);
                int key3 = Math.abs(hashNum3.intValue()) % numDHT; 

                // Call remote method
                String response3 = service.getRandomNode();
                String[] token3 = response3.split(",");
                String message3 = key3 + "/" + wordInput;
                deleteKeyDHT(token3[0],token3[1],message3); 
                System.out.println("");
            }
            else if (line.equals("4")) {
                insert_from_file();
                System.out.println("");
            }
            else if (line.equals("5")) {
                query_from_file();
                System.out.println("");
            }
            else if (line.equals("6")) {
                read_requests_from_file();
                System.out.println("");
            }
            else if (line.equals("7")) {
                System.out.print ("Insert the NodeID of the node you want to depart: ");
                String wordInput = din.readLine();                  
                //String[] token4 = wordInput.split(",");
                String response = service.findNodeInfo(wordInput);
                if(response.equals("")){
                    System.out.println("Not valid NodeID!");
                }
                else{
                    String[] token = response.split("/");
                    // Call remote method
                    String response4 = service.deleteNode(wordInput,token[0],token[1]);
                    String[] token5 = response4.split(",");
                    String response5 = service.getNumNodes();
                    System.out.println("getNumNodes returned:"+response5);
                    String[] token6 = response5.split(",");
                    delete_and_update(token5[1],token5[2],token5[3],token6[0]);
                }
                System.out.println("");
            }
            else if (line.equals("8")) {
                printAllNodeInfo();
            }

            else if (line.equals("9")) {
                System.exit(0);
            }
            else {
                System.out.println("Invalid option");
                System.out.println("");
            }
        }
    }
}

