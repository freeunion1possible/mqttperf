package edu.mqttdev.concurrency;

import java.util.concurrent.BlockingQueue;


/***
 * Producer data to the buffer 
 * 
 * @author 	jorg
 * @date 	2015/01/22
 */

//public class Producer implements Runnable{
public class Producer{
 
    private BlockingQueue<Message> queue;
//    private BlockingQueue<Message> nonDeliverQ;
    
    private int tamanyo; 
    private int frecuency;
    private int testlife;
     
    public Producer(BlockingQueue<Message> q, int FRECUENCY, int SIZE, int TESTLIFE) {
    	this.queue=q;
    	this.frecuency=FRECUENCY;
        this.tamanyo=SIZE;
        this.testlife=TESTLIFE;
	}
    
	//private int TESTLIFE=60;
	
    //trash=MessageMaker
//    @Override
//    public void run() {
//    	
//    }
    /////otra opcion
    public void iniciarProducer() {
        //produce messages
    	//Producir en base a tiempos
    	//con lo especificado por la linea de comandos
    	
		long t_ini, t_current = 0, t_exp_life;
		
		t_ini = System.currentTimeMillis();
		t_exp_life = t_ini + (testlife * 1000);
		
		int i=0;				//int i=0; para el 1er mensaje dentro del while

		while (t_current < t_exp_life){
		
			i++;
			final StringBuilder tmp = new StringBuilder();
			tmp.append(i);
				
			Thread thread = new Thread() {
				String a = tmp.toString();
				int ii = Integer.parseInt(a);
			
				@Override
				public void run() {
					System.out.println("Thread number: " + ii  );
			
					String response = null;
				
					// Prepare the message
					try {
						long t1 = System.currentTimeMillis();
								 
						if (ii==1)	response = MessageMaker.getMessage(1, ii, t1, frecuency);
						else 		response = MessageMaker.getMessage(tamanyo, ii, t1, frecuency);
						System.out.println("t1="+t1);
					} catch (Exception e) {
						System.out.println(e.toString());
						response = "";
					}
		
		    		Message msg = new Message(response);
		
		    		// Enqueue the message
		    		try {
		                queue.put(msg);
		                //System.out.println("Produced " + msg.getMsg());

		                int capacidad=queue.remainingCapacity();
		            	System.out.println("capacitat restante de la cola del productor: " + capacidad);
		
		            } catch (InterruptedException e) {
		                e.printStackTrace();
		            }

				}//end_run 
			}; //end_thread
			thread.start();
    		
    		try {
				Thread.sleep(frecuency);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    		t_current = System.currentTimeMillis();
    			
    	}//end_while
    	
		
		String responsExit = MessageMaker.getMessage(-100, 10000, 0, -100);
		Message msgExit = new Message(responsExit);

		// Enqueue the message
		try {
            queue.put(msgExit);
            //System.out.println("Produced " + msgExit.getMsg());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

		
		//adding exit message
//        Message msg = new Message("exit");
//        try {
//            queue.put(msg);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

    }
}
