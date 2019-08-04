import java.net.*;
import java.net.UnknownHostException;
import java.util.*;
import java.io.*;

public class ThreadWorker implements Runnable
{
	private String IP;
	private String Port;
	private String request;

	public ThreadWorker(String I, String P, String R){
		IP = I;
		Port = P;
		request = R;
	}

	public void run(){
		try{
			//System.out.println("Inside of run of ThreadWorker. I received this request:"+request);
			Socket sendingSocket = new Socket(IP,Integer.parseInt(Port));
        		DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
        		BufferedReader inFromServer = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));

        		out.writeBytes(request + "\n");
        		//System.out.println("Just sent request by threadworker!");
       			String result = inFromServer.readLine();
       			//System.out.println("ThreadWorker: Received this result:"+result);
        		inFromServer.close();
        		sendingSocket.close();
		} catch (Exception e){
			e.printStackTrace();
		}
	}


}
