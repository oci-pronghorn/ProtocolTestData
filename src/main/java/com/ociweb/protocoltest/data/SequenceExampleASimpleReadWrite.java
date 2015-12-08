package com.ociweb.protocoltest.data;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;

public class SequenceExampleASimpleReadWrite {

    public static void write(SequenceExampleA obj, DataOutputBlobWriter writer) {
        
        writer.writePackedInt(obj.user);
//        writer.writePackedInt(obj.year);
//        writer.writePackedInt(obj.month);
//        writer.writePackedInt(obj.date);
//        writer.writePackedInt(obj.sampleCount);
        
//        int count = obj.sampleCount; 
//        for(int i=0; i<count; i++) {
//            
//            SequenceExampleASample item = obj.samples[i];
//            
//            writer.writePackedInt(item.id);
//            writer.writePackedLong(item.time);
//            writer.writePackedInt(item.measurement);
//            writer.writePackedInt(item.action);            
//            
//        }
    }
    
    public static void read(SequenceExampleA obj, DataInputBlobReader reader) {
        
        obj.user = reader.readPackedInt();
//        obj.year = reader.readPackedInt();
//        obj.month = reader.readPackedInt();
//        obj.date = reader.readPackedInt();
//        obj.sampleCount = reader.readPackedInt();
        
//        int count = obj.sampleCount; 
//        for(int i=0; i<count; i++) {
//            
//            SequenceExampleASample item = obj.samples[i];
//            
//            item.id = reader.readPackedInt();
//            item.time = reader.readPackedLong();
//            item.measurement = reader.readPackedInt();
//            item.action = reader.readPackedInt();            
//            
//        }
        
    }
    
    
    
    
}
