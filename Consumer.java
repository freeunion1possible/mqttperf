package edu.mqttdev.concurrency;

import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
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
 */


public class Consumer implements Runnable{
//public class Consumer{
 
private static MqttClient clienteMQTT;
private final static String Topic = "test-topic_mqtt";
private final static int QoSLEVEL = 1;
private final static String BROKER="tcp://10.0.1.190:1883";
private final static int heartBeat = 0;

private BlockingQueue<Message> queue;
private int FRECUENCY;
//private BlockingQueue<Message> nonDeliverQ;

BlockingQueue<Message> nonDeliverQ = new ArrayBlockingQueue<>(100);
static List<Message> offlineQueue = new ArrayList<Message>();

	public Consumer(BlockingQueue<Message> q, int frecuency){
        this.queue=q;
        this.FRECUENCY=frecuency;
    }
 
    int id=0, intentoConex=0;
    @Override
    public void run() {
    	
    //public void iniciarConsumer(){
		
    	clienteMQTT = ConnectionMngr.openMQTTconnection(BROKER, heartBeat, true);
		if (clienteMQTT != null) {
				Message msg=null;
	            //consuming messages until exit message is received
	            //while((msg = queue.take()).getMsg() != "exit"){
	            int resetConexion=0;
	            while(true){
	            	id++;
	            	
	            	try {
						Thread.sleep(FRECUENCY);
					} catch (InterruptedException e2) {
						// TODO Auto-generated catch block
						System.out.println("Sleeping problem: " + e2.getMessage());
					}
	            	
	            	String menssaje=null;
	            	try {
						msg = queue.take();
		            	//System.out.println("Consumed "+msg.getMsg());
		            	menssaje=msg.getMsg();

					} catch (InterruptedException e2) {
						System.out.println("Error Getting message: " + e2.getMessage());
					}
	            	
	            	if (menssaje!=null){
			            	boolean conect = clienteMQTT.isConnected();
			            	String cliId = clienteMQTT.getClientId();
			            	System.out.println(" connected: " + conect + "ID: " + cliId);
			            	
			            	MqttTopic topic = clienteMQTT.getTopic(Topic);
			                MqttMessage message = new MqttMessage(menssaje.getBytes());
			                message.setQos(QoSLEVEL);
			                	System.out.println("Message [" + id + "] published with QoS [" + message.getQos() + "]  by client: " + clienteMQTT.getClientId());
			              
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
			                	token.waitForCompletion(500);		// 1 seconds
							} catch (MqttException e) {
								
								resetConexion++;
								System.out.println("                resetConexion: " + resetConexion);
								
								if (e.getReasonCode()==32000){		//Timeout
									 System.out.println("Message: " + id  + " is TIMED OUT waiting for an ACK");
								
									 // Enqueue the non-deliver message
									 try {
										 //nonDeliverQ.put(msg);
										 AddMessageToBackupQueue(msg);
										 System.out.println(msg.toString() + " enqueued");
						                
									 } catch (Exception e1) {
										 e1.printStackTrace();
									 }
			
								} else 
									 System.out.println("otro error");
								
								if (resetConexion==3)
									try {
										clienteMQTT.disconnect();
										System.out.println("Client Disconected");
									} catch (MqttException e1) {
										e1.printStackTrace();
									}
								
									try {
										establish_new_Connection();
									} catch(Exception e2){
										System.out.println("Error in the new conection: " + e2.getMessage());
									}
													 
							}	
			                
			                if(token.isComplete()) {
			                	System.out.println("     Token: " + token.hashCode() + " -> received? " + token.isComplete());
			                	queue.remove(msg);
			                } else {
			                	System.out.println("Token -msg: " + id + "- don't received");
			                }
	            	}else{
	            		System.out.println("There aren't messages");
	            	}
	            	
	               
	            }//while
	    
		}//if cliente=null
		else {
			System.out.println("Cliente MQTT offline, the generated messages will be loss");
		}
        
    }

   
    /**
     *  si el cliente no esta conectado -> intentamos conectarnos
     */
    private void establish_new_Connection() {
    	System.out.println("Connection Lost -- trying to re-connect");
    	
    	int tries=0;
    	boolean flag=true;
    	
    	while (flag) {

    		try {
				Thread.sleep(FRECUENCY);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			
    		if (checkIfNetworkAvailable() && !clienteMQTT.isConnected()) {
		
				 try {
					 tries++;
					 intentoConex++;
					 System.out.println("intento de conexion numero: " + intentoConex);
					 
					 Thread thread = new Thread() {
						 @Override
						 public void run() {
							 clienteMQTT = ConnectionMngr.openMQTTconnection(BROKER, heartBeat, true);
							 if (clienteMQTT != null) {
								 String clienteID=clienteMQTT.getClientId();
								 System.out.println("Now we are connected,  ahora el cliente es: " + clienteID);
								 depletedBufferOffline(clienteMQTT);				 
							 }
						 }//run
					 }; //thread
					 thread.start();
					 flag=false;
					 
				 } catch (Exception e){
					 System.out.println("Service down");
				 }
				 
				 if (tries>3)
					 break;
				 
			 } else {
				 String clienteID=clienteMQTT.getClientId();
				 System.out.println("The client are connected with ID: " + clienteID);
				 flag=false;
			 }
    	}
    }
    
	private boolean checkIfNetworkAvailable() {
		try {
			Inet4Address.getByName("10.0.1.190");
			return true;
		} catch (Exception e) {
			return false;
		}
	}
	
	private static void AddMessageToBackupQueue(Message payload) {
		offlineQueue.add(payload);
	}
	
	private static void depletedBufferOffline(MqttClient clientesMQTT) {
		
		Iterator<Message> it = offlineQueue.iterator();
		MqttTopic topic = clientesMQTT.getTopic(Topic);
	       
        String mensajeString=null;
		while(it.hasNext()) {
			Message obj = (Message)it.next();
			
			mensajeString=obj.getMsg();
					
            MqttMessage message = new MqttMessage(mensajeString.getBytes());
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
            	token.waitForCompletion(500);		// 1 seconds
			} catch (MqttException e) {
				e.printStackTrace();
			}
		
		}
	}
    
}
