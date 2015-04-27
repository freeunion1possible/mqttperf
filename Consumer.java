package edu.mqttdev.concurrency;

import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;


/***
 * Buffer Consumer 
 * and Publisher MQTT
 * 
 * @author 	jorg
 * @date 	2015/01/22
 * @modified date	201/04/24
 */


public class Consumer implements Runnable{
//public class Consumer{
 
private static MqttClient clienteMQTT;
private final static String Topic = "test-topic_mqtt";
private final static int QoSLEVEL = 1;
private final static int heartBeat = 0;

private BlockingQueue<Message> queue;
//private ArrayList<Message> queue;
private int FRECUENCY;
private String BROKER;
private String brokermq;

static List<Message> offlineQueue = new ArrayList<Message>();

	int id=0, intentoConex=0;
	String messageSalida;
	long t_out_queue;

	public Consumer(String brokermq, BlockingQueue<Message> q, int frecuency){
		this.brokermq=brokermq;
		this.BROKER="tcp://"+brokermq+":1883";
        this.queue=q;
        this.FRECUENCY=frecuency;
    }

    
    @Override
    public void run() {
    	
    	String menssaje=null;
    	clienteMQTT = ConnectionMngr.openMQTTconnection(BROKER, heartBeat, true);
		
    	if (clienteMQTT != null) {
			int resetConexion=0;

			while(true){
				Message msg=null;
				id++;
				t_out_queue=0;

				try {			//periodicidad
					Thread.sleep(FRECUENCY);
				} catch (InterruptedException e2) {
					System.out.println("Sleeping problem: " + e2.getMessage());
				}
	         
				if(checkIfNetworkAvailable()){
					
					int capacidad=queue.remainingCapacity();
					System.out.println("capacitat restante del buffering: " + capacidad);
	
					try {
						msg = queue.take();
						menssaje=msg.getMsg();
	
					} catch (Exception e2) {
						System.out.println("Error Getting message: " + e2.getMessage());
					}
		            	
		            if (menssaje!=null){
				        
		            	// can be SAVE IN A FILE?
							           		
		            	t_out_queue = System.currentTimeMillis();
						messageSalida = MessageMaker.addField(menssaje, t_out_queue);
	
	//					boolean conect=false;		//igualar estas variables a su estado cuando inicial
						String cliId="noID";
						MqttTopic topic = null;
	//						
						try{
	//						conect = 	clienteMQTT.isConnected();
							cliId = 	clienteMQTT.getClientId();
					        topic = 	clienteMQTT.getTopic(Topic);
	//				        //System.out.println(" connected: " + conect + "  ID: " + cliId);
						} catch (Exception e3){
							System.out.println("exception with the current clientMQTT: " + e3.getMessage());
						}
	
						MqttMessage message = new MqttMessage(messageSalida.getBytes());
						message.setQos(QoSLEVEL);
						System.out.println("Message [" + id + "] published with QoS [" + message.getQos() + "]  by client: " + cliId + " t_out: " + t_out_queue );
				              
				        //-------------------------------------------------------------------------------------
						MqttDeliveryToken token = null;
						try {
							token = topic.publish(message);		// <----------PUBLISH
						} catch (MqttException e) {
						//-------------------------------------------------------------------------------------
								
							if (e.getReasonCode()==32202) 		//Too many publishes in progress
								System.out.println("---------------------------------------too many publishes in progress");
							else 
								System.out.println("error: " + e.getMessage());
						}
				                
						try {									//la espera del ACK
							token.waitForCompletion(1000);		// 1 seconds
						} catch (MqttException e) {
									
							resetConexion++;
	
							// Enqueue the non-deliver message
							try {
								AddMessageToBackupQueue(msg);
								System.out.println(msg.toString() + " enqueued");
						               
							} catch (Exception e1) {
								e1.printStackTrace();
							}
	
									
							if (e.getReasonCode()==32000){		//Timeout
								System.out.println("Message: " + id  + " is TIMED OUT waiting for an ACK");
				
							} else 
								System.out.println("otro error");
									
							if (resetConexion>=3) {
										
								System.out.println("====================== resetConexion: " + resetConexion);		
	//								try {						//desconectar el cliente
	//									clienteMQTT.disconnect();
	//									System.out.println("Client Disconected");
	//								} catch (MqttException e1) {
	//									e1.printStackTrace();
	//								}
																		
								try {
									establish_new_Connection();
								} catch(Exception e2){
									System.out.println("Error in the new conection: " + e2.getMessage());
								}
									//resetConexion=0;
							}	
						}		//fin de la espera x el ACK
				            
						if(token.isComplete()) {
							System.out.println("     Token: " + token.hashCode() + " -> received? " + token.isComplete());
				            queue.remove(msg);
						} else {
				            System.out.println("Token -msg: " + id + "- don't received");
				        }
		            } //message null
		            else{
		            	System.out.println("no messages to publish");
					}
							
				} else {
		            	System.out.println("There are messages enqueued but no way to deliver");
				}
	       }//while
	    
		}//if cliente=null
		else {
			System.out.println("Cliente MQTT offline, the generated messages will be loss");
		}
    }


    /**
     * when the client loss its normal connection -> we tried to reconnect
     */
    private void establish_new_Connection() {
    	System.out.println("Connection Lost -- trying to re-connect");
    	
    	//int tries=0;
    	boolean flag=true;
    	
    	while (flag) {

        	if (checkIfNetworkAvailable()) {    			
        		System.out.println("Network available!");
        		
        		try {
    				Thread.sleep(FRECUENCY);
    			} catch (InterruptedException e1) {
    				e1.printStackTrace();
    			}
    			
        		
				try {					 
			//		 tries++;
					 intentoConex++;
					 System.out.println("---conexion tried number: " + intentoConex);
					 System.out.println("Cliente conectado");

//					 Thread thread = new Thread() {
//						 @Override
//						 public void run() {
							 clienteMQTT = ConnectionMngr.openMQTTconnection(BROKER, heartBeat, true);
							 if (clienteMQTT != null) {
								 String clienteID=clienteMQTT.getClientId();
								 System.out.println("Now we are connected,  ahora el cliente es: " + clienteID);
								 depletedBufferOffline(clienteMQTT);				 
							 }
//						 }//run
//					 }; //thread
//					 thread.start();
					 
							 if (clienteMQTT != null) {
								 flag=false;		
							 }
					 
					 
				 } catch (Exception e){
					 System.out.println("Service down");
					 e.printStackTrace();
				 }
				 
//				 if (tries>3)	break;
				 
			 } else {
				 System.out.println("Network not available.");
				 //ConnectionMngr.closeMQTTconnection();
				 flag=false;
			 }
    	}
    }
    
	private boolean checkIfNetworkAvailable() {
		try {
			if (Inet4Address.getByName(brokermq).isReachable(1000)==true){
				//System.out.println("The Broker will be reach");
				return true;
		 	}else
				return false;
		} catch (Exception e) {
			return false;
		}
	}
	
	private static void AddMessageToBackupQueue(Message payload) {
		offlineQueue.add(payload);
	}
	
	private static void depletedBufferOffline(MqttClient clientesMQTT) {
		
		String messageSalida;
		long t_out_queue;
		   
		Iterator<Message> it = offlineQueue.iterator();
		MqttTopic topic = clientesMQTT.getTopic(Topic);
	       
        String mensajeString=null;
		while(it.hasNext()) {
			Message obj = (Message)it.next();
			
			mensajeString=obj.getMsg();
			
			t_out_queue = System.currentTimeMillis();
			messageSalida = MessageMaker.addField(mensajeString, t_out_queue);
		
            MqttMessage message = new MqttMessage(messageSalida.getBytes());
            message.setQos(QoSLEVEL);
            System.out.println("Message [" + 100 + "] published with QoS [" + message.getQos() + "]  by client: " + clientesMQTT.getClientId());
	    
			MqttDeliveryToken token = null;
			try {
				token = topic.publish(message);		// <----------PUBLISH
			} catch (MqttException e) {
				if (e.getReasonCode()==32202) 		//Too many publishes in progress
					System.out.println("---------------------------------------too many publishes in progress");
				else 
					System.out.println("error: " + e.getMessage());
			}
			
            try {
            	token.waitForCompletion();			// 1 seconds
			} catch (MqttException e) {
				e.printStackTrace();
			}
            
            if(token.isComplete()) {
				System.out.println("     Token: " + token.hashCode() + " -> received? " + token.isComplete());
                	it.remove();
            }
		}
	}
    
}
