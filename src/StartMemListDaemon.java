/*
Marks this thread as either a daemon thread or a user thread. 
The Java Virtual Machine exits when the only threads running are all daemon threads.
*/

public class StartMemListDaemon{
	public static void main(String[] args)
	{
		//the user enters the introducer machine name of the introducer
		if (args.length != 1)
		{
			System.out.println("Usage: StartMemListDaemon <IntroducerName>");
			System.exit(0);
		}
		String introducerHostName = args[0];
		
		int listeningport = 30000;
		int TfailinMS = 1500;
		int TcleaninMS = 1500;
		int gossipTimeinMS = 500;
		int runTimeinMS = 3600000;
		double dropRate = 0.0;
		
		// create a memlist object
		DistMemList memListDaemon= new DistMemList(introducerHostName,listeningport,TfailinMS,TcleaninMS,gossipTimeinMS,dropRate);
		
		//start the memListDaemon on a separate thread
		Thread daemonThread = new Thread(memListDaemon);
		daemonThread.setDaemon(true);
		daemonThread.start();
		
//		Allow the daemon to run for 1 hour and then terminate.
		try {
			Thread.sleep(runTimeinMS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
