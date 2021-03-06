package com.ociweb.protocoltest.data;

import java.io.IOException;
import java.io.InputStream;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class PhastReader {

    private Pipe<RawDataSchema> workPipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 4, 500000));
    private DataInputBlobReader<RawDataSchema> pipeReader;
    
    
    public final static int valueZero = 0;
    public final static long timeZero = 0;
    public final static int defaultAction = 5;
    
    int[] intDictionary = new int[16];
    long[] longDictionary = new long[16];
    
    private static final int ID_IDX = 0;
    private static final int MEASUREMENT_IDX = 1;
    private static final int USER_IDX = 2;
    private static final int YEAR_IDX = 3;
    private static final int MONTH_IDX = 4;
    private static final int DATE_IDX = 5;
    private static final int SAMPLE_COUNT_IDX = 6;   
    
    private static final int TIME_IDX = 0;
    
    public PhastReader() {        
        workPipe.initBuffers();
        pipeReader = new DataInputBlobReader<RawDataSchema>(workPipe);  
    }
    
    public static SequenceExampleA readFromInputStream(PhastReader reader, SequenceExampleA targetObject) {
         
         PhastReader.read(reader.intDictionary, reader.longDictionary, targetObject, reader.pipeReader); 
         
         return targetObject;
         
    }

    public static void releaseStream(PhastReader reader) {
        Pipe.releaseReadLock(reader.workPipe);        
    }
    
    public static void fetchStreamIntoPipe(PhastReader reader, InputStream in)  {
        try {
             Pipe.addMsgIdx(reader.workPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);            
             Pipe.readFieldFromInputStream(reader.workPipe, in, in.available());            
             Pipe.publishWrites(reader.workPipe);                    
             Pipe.publishAllBatchedWrites(reader.workPipe);
             
             //Now open this for use
             Pipe.takeMsgIdx(reader.workPipe);                
             reader.pipeReader.openLowLevelAPIField();
             
        } catch (IOException e) {
            throw new RuntimeException(e);
        }        
    }

    public static void read(int[] intDictionary, long[] longDictionary, SequenceExampleA obj, DataInputBlobReader reader) {
                
        int templateId = reader.readPackedInt();
        int msgIdx = templateId>>2;
        
        long templateMap = reader.readPackedLong(); //this int can be RLE as well
        obj.user = PhastDecoder.decodeDeltaInt(intDictionary, reader, templateMap, USER_IDX, valueZero, 2);       
             
        
      //  System.out.println(Long.toBinaryString(templateMap));
        
        obj.year = PhastDecoder.decodeCopyInt(intDictionary, reader, templateMap, YEAR_IDX, 4);       
        intDictionary[YEAR_IDX]= obj.year;
        obj.month = PhastDecoder.decodeCopyInt(intDictionary, reader, templateMap, MONTH_IDX, 8);       
        intDictionary[MONTH_IDX]=obj.month;
        obj.date = PhastDecoder.decodeIncInt(intDictionary, reader, templateMap, DATE_IDX, 16);  
        //no need to set inc does it inside call        
                
        
        obj.sampleCount = PhastDecoder.decodeDefaultInt(reader, templateMap, SequenceExampleASchema.FIXED_SAMPLE_COUNT, 32);       
        
        
        longDictionary[TIME_IDX] = 0;
        intDictionary[ID_IDX] = 0; //NOTE: this must be a reset message.
        intDictionary[MEASUREMENT_IDX] = 0;
        
        int count = obj.sampleCount; 
        SequenceExampleA.ensureCapacity(obj, count); 
        
        int runCount = 0;
        long map = 0;
        for(int i=0; i<count; i++) {            
            
            if (--runCount < 0) {
                int fragmentId = reader.readPackedInt();
                map = reader.readPackedLong();
                runCount = (int)(map >> CompressionState.RUN_SHIFT);
                runCount--; //for this usage;
            }
            
            SequenceExampleASample item = obj.samples[i];
            
            item.id = PhastDecoder.decodeIncInt(intDictionary, reader, map, ID_IDX, 2);            
            item.time = PhastDecoder.decodeDeltaLong(longDictionary, reader, map, TIME_IDX, timeZero, 4);
            item.measurement = PhastDecoder.decodeDeltaInt(intDictionary, reader, map, MEASUREMENT_IDX, valueZero, 8);           
            item.action =  PhastDecoder.decodeDefaultInt(reader, map, defaultAction, 16);
            
        }
        
    }
    
}
