package com.ociweb.protocoltest.data;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;

public class SequenceExampleASimpleReadWrite {

    public static void write(SequenceExampleA obj, DataOutputBlobWriter writer) {
        
        writer.writePackedInt(obj.user);
        writer.writePackedInt(obj.year);
        writer.writePackedInt(obj.month);
        writer.writePackedInt(obj.date);
        writer.writePackedInt(obj.sampleCount);
        
        
        //leading mask byte for compression style.
        int map = 0;
        
        long timeCache = 0;
        int measureCache = 0;
        int lastId = 0;
        
        int count = obj.sampleCount; 
        for(int i=0; i<count; i++) {
            SequenceExampleASample item = obj.samples[i];
            
            //map
            // id - did inc vs value
            // time - use zero or dif //mostly near last time
            // measure = use zero or dif //mostly near last measurement
            // action = use default or value //mostly the same few odd cases
            
            map = 0;
            int defaultAction = 1;
            
            map = (map | ( ( item.id == (++lastId)  ) ? 0 : 1 ))<<1;  
            map = (map | ( ( item.time != 0  ) ? 0 : 1 ))<<1;  
            map = (map | ( ( item.measurement != 0  ) ? 0 : 1 ))<<1;  
            map = (map | ( ( item.action-defaultAction == 0 ) ? 0 : 1 ))<<1; //or for copy could check prev and store it as active. 
            
            //constant is not demoed here
            
            
            writer.writePackedInt(map);
            
            
            if (i==0) {
                writer.writePackedInt(item.id);
            }
            //writer.writePackedInt(item.id);
            
            writer.writePackedLong(item.time-timeCache);
            timeCache = item.time;
//            writer.writePackedLong(item.time);
            
            
            writer.writePackedInt(item.measurement-measureCache);
            measureCache = item.measurement;
            
           // writer.writePackedInt(item.measurement);
            writer.writePackedInt(item.action);            
            
        }
    }
    
    public static void read(SequenceExampleA obj, DataInputBlobReader reader) {
        
        
        obj.user = reader.readPackedInt();
        obj.year = reader.readPackedInt();
        obj.month = reader.readPackedInt();
        obj.date = reader.readPackedInt();
        obj.sampleCount = reader.readPackedInt();
        
        long timeCache = 0;
        int id=0;
        int measureCache = 0;
        
        int count = obj.sampleCount; 
        for(int i=0; i<count; i++) {
            
            int map = reader.readPackedInt();
            
            SequenceExampleASample item = obj.samples[i];
            
            if (i == 0) {                
                id = reader.readPackedInt();
            }
            item.id = id++;
            // item.id = reader.readPackedInt();   /11, 1720 comp 53 ->  13, 1024 comp 75  (time optimized now) 33, 2590 comp 75
            
            long timeDif = reader.readPackedLong();
            item.time =  timeCache+timeDif;
            timeCache = item.time;
//               item.time = reader.readPackedLong();  // 7, 1628 comp 28 -> 11, 1720 comp 53
            
            int measureDif = reader.readPackedInt();
            item.measurement =  measureCache+measureDif;
            measureCache = item.measurement;          
            //item.measurement = reader.readPackedInt();
            
            item.action = reader.readPackedInt();            
            
        }
        
    }
    
    
    
    
}
