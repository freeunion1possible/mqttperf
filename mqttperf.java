package edu.mqttdev.concurrency;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class mqtperf {
 
    public static void main(String[] args) {

		int REPETITIONS = 10;
		int SENDINGFRECUENCY = 10000; 	//10 seconds	
		int MESSAGESIZE = 128; 			//128 bytes
		int HEARTBEAT = 0;				//580 segundos default
		int TESTLIFE = 5;
		String BROKERMQ = null;
		boolean CONFIGURED;
		CONFIGURED = false;

		///
    	/// --- menu
		///
		/*
		 *	Parsing of command-line options takes place...
		 */
		/////////////////////////////////////////////////////////////////////	getOPT
		int c;
		String arg;
		LongOpt[] longopts = new LongOpt[3];
		// 
		StringBuffer sb = new StringBuffer();
		longopts[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
		longopts[1] = new LongOpt("outputdir", LongOpt.REQUIRED_ARGUMENT, sb, 'o'); 
		longopts[2] = new LongOpt("maximum", LongOpt.OPTIONAL_ARGUMENT, null, 2);
		
		Getopt g = new Getopt("MQTTperf", args, "p::q::b:t:f:r:h:d:", longopts);
		g.setOpterr(false); // We'll do our own error handling
		
		
		while ((c = g.getopt()) != -1)
			switch (c)
			{
			case 0:
				arg = g.getOptarg();
				System.out.println("Got long option with value '" +
				(char)(new Integer(sb.toString())).intValue()
				+ "' with argument " +
				((arg != null) ? arg : "null"));
				break;
				//
			case 1:
				System.out.println("I see you have return in order set and that " +
						"a non-option argv element was just found " +
						"with the value '" + g.getOptarg() + "'");
				break;
				//
			case 2:
				arg = g.getOptarg();
				System.out.println("I know this, but pretend I didn't");
				System.out.println("We picked option " +
						longopts[g.getLongind()].getName() +
						" with value " + 
						((arg != null) ? arg : "null"));
				break;
				//
			case 'p':
				System.out.println("You have selected PRODUCER MODE with these OPTIONS: ");
				CONFIGURED=true;
				break;
				//
			case 'b':
				BROKERMQ = g.getOptarg();
				System.out.println("'-" + (char)c + "' Broker TCP/IP address: " +
						((BROKERMQ != null) ? BROKERMQ : "null"));			
				
				CONFIGURED=true;
				break;
				//
			case 't':	//messageSize
				MESSAGESIZE = Integer.parseInt(g.getOptarg()); 
				System.out.println("'-" + (char)c + "' Message Size of " +
						((MESSAGESIZE != 0) ? MESSAGESIZE : "null") +
						" Bytes ");
				CONFIGURED=true;
				break;
				//
			case 'f':	//sendingFrecuency
				SENDINGFRECUENCY = Integer.parseInt(g.getOptarg());
				if (SENDINGFRECUENCY >= 1000 ) {
					System.out.println("'-" + (char)c + "' Period of " +
							((SENDINGFRECUENCY != 0) ? SENDINGFRECUENCY/1000 : "null") +
							" seconds ");
				} else {
					System.out.println("'-" + (char)c + "' Period of " +
							((SENDINGFRECUENCY != 0) ? SENDINGFRECUENCY : "null") +
							" ms ");
				}
				CONFIGURED=true;
				break;
				//
			case 'r':	//repetitions   !para el consumer se va al infinito y se termina con control+c
				REPETITIONS = Integer.parseInt(g.getOptarg());
				System.out.println("Repetitions '-" + (char)c + 
						"' of " +
						((REPETITIONS != 0) ? REPETITIONS : "null") +
						" times ");
				CONFIGURED=true;
				break;
				//
			case 'd':	//duration 
				TESTLIFE = Integer.parseInt(g.getOptarg());
				System.out.println("'-" + (char)c + "' Test life of " +
						((TESTLIFE != 0) ? TESTLIFE : 1) +
						" seconds ");
				CONFIGURED=true;
				break;
				//
			case 'h':
				HEARTBEAT = Integer.parseInt(g.getOptarg());
				System.out.println("'-" + (char)c + "' Heartbeat of " +
						((HEARTBEAT != 0) ? HEARTBEAT : 1) +
						" seconds");			
				CONFIGURED=true;
				break;
				//
			case ':':
				System.out.println("Doh! You need an argument for option " +
						(char)g.getOptopt());
				break;
				//
			case '?':
				System.out.println("The option '" + (char)g.getOptopt() + 
						"' is not valid");
				break;
				//
			default:
				System.out.println("getopt() returned " + c);
				break;
			}
		////////////////////////////////////////////////////////////////////	end_getOPT
		///
		///	--- end_menu
		///
		
		if (CONFIGURED){
			System.out.println("---------------------------------");
			
			//Creating BlockingQueue of size 10
			BlockingQueue<Message> queue = new ArrayBlockingQueue<>(1000);
			
			
	        Producer producer = new Producer(queue,SENDINGFRECUENCY,MESSAGESIZE,TESTLIFE);
	        Consumer consumer = new Consumer(queue,SENDINGFRECUENCY);
	        
	        //starting consumer to consume messages from queue
	        try {
	        	new Thread(consumer).start();
	        	//consumer.iniciarConsumer();
	        	//(new Thread(new Consumer(drop))).start();
	        } catch (Exception e){
	        	System.out.println("Error en el consumer-sending");
	        	e.printStackTrace();
	        }

	        
	        
	        //starting producer to produce messages in queue
	        producer.iniciarProducer();

	        //If there are messages in the queue
//	        if (!queue.isEmpty()) {
//	        	try {
//	        		System.out.println("There are messages to deliver...");
//	        		new Thread(consumer).start();
//	        	} catch (Exception e){
//	        		System.out.println("Error en el consumer-sender");
//	        		e.printStackTrace();
//	        	}
//	        } else 
//	        	System.out.println("Delivery completed");

	        
			
		} else {	
			printUsageAndExit();
		}

		//System.exit(1);
    }
	
	/**
	 * ayuda
	 */
	private static void printUsageAndExit()
	{
		System.out.println("MQTTperf : usage:");
		System.out.println("\tjava -jar mqtperf.jar [-t <messageSize>] [-f <sendingfrequency>] ");
		System.exit(1);
	}
    
}
