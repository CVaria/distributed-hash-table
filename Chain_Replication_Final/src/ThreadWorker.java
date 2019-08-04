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
			Socket sendingSocket = new Socket(IP,Integer.parseInt(Port));
        	DataOutputStream out = new DataOutputStream(sendingSocket.getOutputStream());
        	BufferedReader inFromServer = new BufferedReader(new InputStreamReader(sendingSocket.getInputStream()));
        	String new_request= request+"\n";
        	out.writeBytes(new_request);

       		String result = inFromServer.readLine();

        	inFromServer.close();
        	sendingSocket.close();
		} catch (Exception e){
			e.printStackTrace();
		}
	}


}
