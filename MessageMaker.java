package edu.mqttdev.concurrency;

import org.json.JSONException;
import org.json.JSONObject;

public class MessageMaker {

    public MessageMaker(){
    	//constructor
    }

    /**
     * Message content generator to fit its size to specified message size in Bytes.
     * @param size - the size
     * @param id - the message id
     * @param t1 - the production time stamp 
     * @return a String in JSON format
     */
    
	protected static String getMessage(int size, int id, long t1, int period){

		int heartBeat=1;
		int testlife=1;
		
    	//resto 104 (exceso) porque he comprobado que en todos los mensajes tienen 104 Bytes de sobra
    	int overflow = 104;		
    	String strToComplete = null;
    	if (size>overflow)
    		strToComplete = createDataSize(size-overflow);
    	else 
    		strToComplete = createDataSize(overflow);

    	String response;
    	response = writeJson(heartBeat, size, period, id, t1, testlife, strToComplete);
    	return response;
    }
 
	
    /**
	 * para hacer los mensajes JSON 
	 * @param _js_heartbeat
	 * @param _js_size
	 * @param _js_frecuency
	 * @param _js_numberMsg
	 * @param _js_timeT1
	 * @param _js_bb
	 * @return
	 */
    private static String writeJson(double _js_heartbeat, int _js_size, int _js_frecuency, int _js_numberMsg, long _js_timeT1, int _js_testlife, String _js_bb) {
		JSONObject object = new JSONObject();
		
		try {
			object.put("heartbeat", 	_js_heartbeat);
			object.put("size", 			_js_size);
			object.put("frecuency", 	_js_frecuency);
			object.put("id", 			_js_numberMsg);
			object.put("timeT1", 		_js_timeT1);
			object.put("repetitions", 	_js_testlife);
			object.put("complete", 		_js_bb);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
		return object.toString();
	}

	/**
	 * In UTF-8 characters need between 1 and 4 bytes. So, you can store between 1024 and 4096 UTF-8 characters in 4KB. 	
	 * @param msgSize
	 * @return
	 */
	private static String createDataSize(int msgSize) {
		  StringBuilder sb = new StringBuilder(msgSize);
		  for (int i=0; i<msgSize; i++) {
		    sb.append('a');
		  }
		  return sb.toString();
	}
	
}
