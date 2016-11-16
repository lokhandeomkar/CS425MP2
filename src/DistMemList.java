/*
Receiver:
Classes Used: DatagramPacket and DatagramSocket
Opens a DatagramSocket on listeningport, keeps receiving DatagramPackets
DatagramPacket has the destination IP and port at the sender.
Ignores packets deliberately to simulate lossy network with a specified 
loss rate and adds received packets to the queue.
*/
import java.io.*;
import java.net.*;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class DistMemList implements Runnable{
	volatile Map<String,String> ownmemList = new HashMap<String,String>();
	volatile Queue<String> incomingMessageBuffer = new LinkedList<String>();
	volatile long heartBeatSeq = 1;

	String machineID;
	String selfUpdateTime;
	String currSysStatus;
	int listeningport;
	int TfailinMS;
	int TcleaninMS;
	int gossipTimeinMS;
	int msg_sent;
	int msg_rec;
	int byte_sent;
	int byte_rec;
	double drop_rate;
	String introducerName;
	boolean displayFlag;
	static Logger logger = Logger.getLogger(DistMemList.class.getName());

	public DistMemList(String setIntroducerName,int setlisteningport,int setTfailinMS,int setTcleaninMS,int setgossipTimeinMS,double setdroprate){
		this.listeningport = setlisteningport;
		this.TfailinMS = setTfailinMS;
		this.TcleaninMS = setTcleaninMS;
		this.gossipTimeinMS = setgossipTimeinMS;
		this.introducerName = setIntroducerName;
		this.currSysStatus = "W";
		this.drop_rate = setdroprate;
		displayFlag = false;
		PropertyConfigurator.configure("./log4j.properties");		
		logger.info("[I] Starting Daemon");
	}


	public void sender() throws IOException{
		ownmemList = new HashMap<String,String>();
		heartBeatSeq = 0;

		String introIP = introducerName;
		InetAddress memberAddress;

		int RunningTime = 3000;
		int Kgossip = 1;
		int jGossipMachines;

		Date currDate = new Date();
		Timestamp currTime = new Timestamp(currDate.getTime());
		SimpleDateFormat mlformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String mltimeStamp = mlformat.format(currTime);

		machineID = "";
		String selfAddress = null;
		// Add self entry to the membership list
		try{
			InetAddress localAddress = InetAddress.getLocalHost();
			selfAddress = localAddress.getHostAddress();
			machineID = selfAddress+" "+mltimeStamp;
		}
		catch (UnknownHostException uhe)
		{
			System.out.println("ERROR: unable to lookup self ip address");
			System.exit(0);
		}

		//create variables to send data
		DatagramSocket outgoingSocket = new DatagramSocket();
		DatagramPacket sendPacket;
		InetAddress IntroducerAddress = InetAddress.getByName(introIP);
		byte[] sendData = new byte[1024];
		String sendListLine = null;

		//keep track of the time to regulate gossiping
		Date date = new Date();
		Timestamp prevtime = new Timestamp(date.getTime());
		Timestamp currtime = prevtime;
		String currtimestring = mlformat.format(currtime);
		selfUpdateTime = currtimestring;

		Set<String> keys = ownmemList.keySet();
		List<String> allKeys = new ArrayList<String>();
		String message;

		//if the current machine is the introducer, simply wait for incoming messages, else keep pinging introducer	
		if (selfAddress.equals(IntroducerAddress.getHostAddress()))
		{
			do{
				if ((message = incomingMessageBuffer.poll()) != null)
				{
					list_merge(message);
				}
				keys = ownmemList.keySet();
			}
			while (keys.size() < 1);			
		}
		else
		{
			do{
				date = new Date();
				currtime = new Timestamp(date.getTime());
				currtimestring = mlformat.format(currtime);
				if ((currtime.getTime()-prevtime.getTime()) >= gossipTimeinMS)
				{
					prevtime = currtime;
					sendListLine = "";
					sendListLine = machineID +"\t"+Long.toString(heartBeatSeq)+"\t"+currtimestring+"\tW\n";
					sendData = sendListLine.getBytes();
					sendPacket = new DatagramPacket(sendData,sendData.length,IntroducerAddress,listeningport);
					try {
						outgoingSocket.send(sendPacket);
						msg_sent++; // added by Omkar
						byte_sent +=  sendPacket.getLength();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				if ((message = incomingMessageBuffer.poll()) != null)
				{
					list_merge(message);
				}
				keys = ownmemList.keySet();
			}
			while (keys.size() < 1);
		}

		//keep sending the selfmember list to introducer till one member of the group gossips back


		//new member(s) has joined
		keys = ownmemList.keySet();

		// there are atleast 1 additional entry in the membership list, which means we have joined the group		
		// now we can start normal gossiping, by selecting k members randomly to send the message to
		date = new Date();
		currtime = new Timestamp(date.getTime());
		prevtime = currtime;
		Timestamp prevDisplaytime = currtime;

		Timestamp t0 = currtime;

		Boolean introducerAlive = true;


		while((currtime.getTime()-t0.getTime())*1E-3 <= RunningTime)
		{
			allKeys.addAll(keys);

			// find out current time and check if Tgossip milliseconds have passed			
			date = new Date();
			currtime = new Timestamp(date.getTime());
			selfUpdateTime = mlformat.format(currtime);

			check_timeout();
			if ((message = incomingMessageBuffer.poll()) != null)
			{
				list_merge(message);
			}

			if ((currtime.getTime()-prevtime.getTime()) >= gossipTimeinMS)
			{
				introducerAlive = false;
				for (String key: allKeys)
				{
					String[] checkIntroducer = key.split(" ");
					if (checkIntroducer[0].equals(introIP)){
						introducerAlive = true;
						break;
					}
				}

				prevtime = currtime;

				jGossipMachines=0;
				Collections.shuffle(allKeys);		

				sendListLine = get_list(selfUpdateTime);
				sendData = sendListLine.getBytes();

				if (!introducerAlive && !selfAddress.equals(IntroducerAddress.getHostAddress())){
					sendPacket = new DatagramPacket(sendData,sendData.length,IntroducerAddress,listeningport);
					outgoingSocket.send(sendPacket);
					msg_sent++;
					byte_sent +=  sendPacket.getLength();
				}

				while ((jGossipMachines < Kgossip) && (allKeys.size()>=1))
				{
					String memberID = allKeys.get(jGossipMachines);

					// using membership id get the ip address of the K targets
					String[] memberInfo = memberID.split(" ");
					String memberIP = memberInfo[0];
					memberAddress = InetAddress.getByName(memberIP);
					sendPacket = new DatagramPacket(sendData,sendData.length,memberAddress,listeningport);
					outgoingSocket.send(sendPacket);
					msg_sent++;
					byte_sent +=  sendPacket.getLength();

					jGossipMachines++;
				}				
				heartBeatSeq++;
			}
			if (displayFlag && ((currtime.getTime()-prevDisplaytime.getTime()) >= gossipTimeinMS)) // gossipTimeinMS
			{
				prevDisplaytime = currtime;
				System.out.println("Messages Sent: "+msg_sent+", Received: "+msg_rec);
				System.out.println("Bytes Sent: "+byte_sent+", Received: "+byte_rec);
				System.out.println(sendListLine);
			}

			allKeys.clear();
			keys = ownmemList.keySet();			
		}
		outgoingSocket.close();
	}

	/*
	this function puts self and the machines in self memlist in a string so that it can be gossipped.
	*/
	
	public String get_list(String currtimestring)
	{
		String entireList = "";
		String sendListLine;
		//		entireList = "";
		sendListLine = machineID +"\t"+Long.toString(heartBeatSeq)+"\t"+currtimestring+"\t"+currSysStatus+"\n";
		entireList = sendListLine;

		Set<String> keys = ownmemList.keySet();
		if (keys.size() > 0)
		{
			for (String key: keys)
			{
				sendListLine = "";
				sendListLine = key+"\t"+ownmemList.get(key)+"\n";
				entireList = entireList+sendListLine;	
				//			entireList = "a";
			}
		}
		return entireList;
	}
	
	public void list_merge(String inlist)
	{
		// This function merges the lists

		// Process every entry from that list 
		// If self ID, goto a line, at that line, update the hb to the current global hb and update the time
		// If a new ID is there, copy that entry but with your current timestamp 
		// If the incoming heartbeat count is newer, update that entry with your current time and the newer heartbeat count
		// else Change status to 'F' if the heartbeat is not new and the process has timed out, update the timestamp
		// If the status is 'V', mark the process as 'F', if the process was marked failed, mark it as alive, in case of new hb

		// find out the local machine time
		Date currDate = new Date();
		Timestamp currTime = new Timestamp(currDate.getTime());
		SimpleDateFormat mlformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String currtimestring = mlformat.format(currTime);

		String[] listLines = inlist.split("\n"); // members are separated by \n

		for (String member: listLines) // processing every member
		{	
			String[] memberinfo = new String[4];			

			memberinfo = member.split("\t"); // tab separation

			String memberID = memberinfo[0];
			String memberHeartBeatString = memberinfo[1];
			Long memberHeartBeatSeq = Long.parseLong(memberHeartBeatString);
			String memberstatus = memberinfo[3];
			String newval = ""; 

			// check if this entry exists in own membership list - if not then add it with current time	 	  
			if ((!ownmemList.containsKey(memberID)) && !memberID.equals(machineID))
			{
				if(memberstatus.equals("W"))
				{
					newval = Long.toString(memberHeartBeatSeq)+"\t"+currtimestring+"\t"+memberstatus;
					ownmemList.put(memberID,newval);					
					logger.info("[New] "+memberID+"\t"+newval);
				}
			}
			// if the entry exists then, check the incoming heart beat sequence, and if it is greater than ownlist seq update it with current time
			else //this is for those entries which are already present in the membership list and not equal to the localmachine
			{
				if (!memberID.equals(machineID))
				{
					String currValue = ownmemList.get(memberID);
					String[] ownmemberinfo = currValue.split("\t");
					Long ownmemberHeartBeatSeq = Long.parseLong(ownmemberinfo[0]);
					String ownmemberstatus = ownmemberinfo[2];

					if (memberHeartBeatSeq > ownmemberHeartBeatSeq){
						ownmemberHeartBeatSeq = memberHeartBeatSeq;
						if (memberstatus.equals("V")) 
						{
							ownmemberstatus = "V"; // if a node wants to leave, it will mark itself as V in its membership list and gossip it, and other nodes immediately mark it as V
						}
						else{
							ownmemberstatus = "W"; // if the node was marked as failed, then if we get a gossip with a bigger sequence,it might still be working			 			 
						}
						newval =  Long.toString(ownmemberHeartBeatSeq)+"\t"+currtimestring+"\t"+ownmemberstatus;
						ownmemList.put(memberID, newval);
						
						if (memberstatus.equals("V")) 
						{
							logger.info("[Leave] "+memberID+"\t"+newval);
						}

					}					
				}
			}
		}
	}
	
	public void update_selfentry()
	{
		Date currDate = new Date();
		SimpleDateFormat mlformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String currtimestring = mlformat.format(currDate.getTime());

		// update the local machine's heartbeat sequence in memlist
		String localmachinenewValues =  Long.toString(heartBeatSeq)+"\t"+currtimestring+"\t"+"W";
		ownmemList.put(machineID, localmachinenewValues);
	}

	public void check_timeout()
	{
		Date currDate = new Date();
		SimpleDateFormat mlformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Timestamp currTime = new Timestamp(currDate.getTime());
		String currtimestring = mlformat.format(currTime);

		Date timeStampDate = new Date();

		Set<String> keys = ownmemList.keySet();
		List<String> failedMembers = new ArrayList<String>();

		for (String memberID: keys){
			String currValue = ownmemList.get(memberID);
			String[] ownmemberinfo = currValue.split("\t");
			Long ownmemberHeartBeatSeq = Long.parseLong(ownmemberinfo[0]);
			try {
				timeStampDate = mlformat.parse(ownmemberinfo[1]);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			Timestamp memberLastUpdate = new Timestamp(timeStampDate.getTime());
			String ownmemberstatus = ownmemberinfo[2];

			String newval = null;

			if (((currTime.getTime() - memberLastUpdate.getTime()) >= TcleaninMS) && (ownmemberstatus.equals("F") || ownmemberstatus.equals("V"))){
				failedMembers.add(memberID);
				newval = Long.toString(ownmemberHeartBeatSeq)+"\t"+currtimestring+"\t"+"D";
				logger.info("[Delete] "+memberID+"\t"+newval);
			}

			if (((currTime.getTime() - memberLastUpdate.getTime()) >= TfailinMS) && ownmemberstatus.equals("W"))
			{
				newval = Long.toString(ownmemberHeartBeatSeq)+"\t"+currtimestring+"\t"+"F";
				ownmemList.put(memberID, newval);
				logger.info("[Fail] "+memberID+"\t"+newval);
			}
		}
		for (String memberID: failedMembers)
		{
			ownmemList.remove(memberID);	

		}
		failedMembers.clear();
	}

	
	
	public void receiver(){
		DatagramPacket receivePacket;
		DatagramSocket incomingsocket = null;
		byte[] receiveData = new byte[1024];
		receivePacket = new DatagramPacket(receiveData,receiveData.length);

		try{
			incomingsocket = new DatagramSocket(listeningport);	
			while(true){
				incomingsocket.receive(receivePacket);
				Random randomGenerator = new Random();
				double rand_num = randomGenerator.nextDouble();

				if (rand_num>=drop_rate)
				{	
					//System.out.println("drop_rate"+drop_rate);
					msg_rec++;					
					byte_rec += receivePacket.getLength();
					String incomingEntry = new String(receivePacket.getData(),receivePacket.getOffset(),receivePacket.getLength());
					incomingMessageBuffer.add(incomingEntry);
					
//					String selfAddress = null;
//					// Add self entry to the membership list
//					String introIP = introducerName;
//					InetAddress localAddress = InetAddress.getLocalHost();
//					selfAddress = localAddress.getHostAddress();
//					InetAddress IntroducerAddress = InetAddress.getByName(introIP);
//					if(selfAddress.equals(IntroducerAddress.getHostAddress()))
//					{
//						System.out.println(incomingEntry);
//					}
				}
			} 
		}
		catch (IOException e) {
			e.printStackTrace();
		}	
	}

	@Override
	public void run() {
		
		Thread receivingThread = new Thread(
				new Runnable() {
					public void run()
					{
						receiver();			
					}
				}
				);
		receivingThread.start();

		Thread sendingThread = new Thread(
				new Runnable() {
					public void run()
					{
						try {
							sender();
						} catch (IOException e) {
							e.printStackTrace();
						}			
					}
				}
				);
		sendingThread.start();

		while(true){
			try{
				BufferedReader inputStream = new BufferedReader(new InputStreamReader(System.in));
				String userCmd;

				System.out.print("\nCMD: X (leave), D (onetime display), C/S (start/stop continuous display): ");
				while((userCmd=inputStream.readLine())!=null){
					if(userCmd.equals("X"))
					{
						currSysStatus = "V";
						System.out.print("System will exit in "+Double.toString(gossipTimeinMS/1000.0)+" s.\n");
						Thread.sleep(gossipTimeinMS);
						System.exit(0);
					}
					else if (userCmd.equals("D")){
						System.out.println(get_list(selfUpdateTime));
						System.out.print("\nCMD: X (leave), D (onetime display), C/S (start/stop continuous display): ");
					}
					else if (userCmd.equals("C")){
						displayFlag = true;
					}
					else if (userCmd.equals("S")){
						displayFlag = false;
						System.out.print("\nCMD: X (leave), D (onetime display), C/S (start/stop continuous display): ");
					}
					else
					{
						System.out.println("Please enter a valid cmd.");
						System.out.print("\nCMD: X (leave), D (onetime display), C/S (start/stop continuous display): ");
					}
				}

			}
			catch(IOException io){
				io.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}	
		}
	}
}
