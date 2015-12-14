package com.ociweb.protocoltest.data;

import java.io.IOException;
import java.io.OutputStream;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class PhastWriter {

    public static class CompressionState {
            
            public int curPos;
            public int nextPMap;
            public int activePMap;
            public int runCountDown;
            
            public SequenceExampleASample[] samples;        
           
            
            public int defaultAction = 5;
            public int lastId = 0;
            
            public CompressionState() {
                
            }
            
            public void initRun() {
                curPos = 0;
                if (0==samples.length) {
                    nextPMap = 0;
                    runCountDown = 0;
                } else {
                    nextPMap = buildPMap(samples[curPos]);
                }
            }
            
    
            private int buildPMap(SequenceExampleASample item) {
                //TODO: this will be generated based on object and schema.
                try {
                 //   System.out.println("equals? "+item.id+" "+lastId);
                    
    //                 return  8|0|0|1; 
                     
    //                
                    return Branchless.ifEquals(item.id, 1+lastId,          0, 1) |
                       Branchless.ifZero((int)item.time,            2, 0) |
                       Branchless.ifZero(item.measurement,          4, 0) |
                       Branchless.ifEquals(item.action, defaultAction, 0, 8);
                }  finally {
                    lastId = item.id;
                }
            }
    
            
            
            public int runLength() {
                return runCountDown;
            }
            
            public int scanAheadForNext() {
                activePMap = nextPMap;
                runCountDown = 0;
                while(nextPMap==activePMap && ++curPos<samples.length) {
                    runCountDown++;//this is adding one for the previous pass not this one.
                    nextPMap = buildPMap(samples[curPos]);  
                }
                
                activePMap |= (runCountDown<<4);
                if (activePMap < 0) {
                    throw new UnsupportedOperationException();
                }
                
                return activePMap;
            }
        }

    private Pipe<RawDataSchema> workPipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 4, 50000));
    private DataOutputBlobWriter<RawDataSchema> pipeWriter;
    private PhastWriter.CompressionState state = new PhastWriter.CompressionState();
    
    int[] intDictionary = new int[4];
    long[] longDictionary = new long[4];
    
    private static final int ID_IDX = 0;
    private static final int MEASUREMENT_IDX = 1;
    private static final int TIME_IDX = 0;
    
    public PhastWriter() {
        workPipe.initBuffers();
        pipeWriter = new DataOutputBlobWriter<RawDataSchema>(workPipe);
    }
        
    public static  void writeToOuputStream(PhastWriter writer, SequenceExampleA nextObject, OutputStream out) {
        Pipe.addMsgIdx(writer.workPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
        writer.pipeWriter.openField();
        
        PhastWriter.write(writer.intDictionary, writer.longDictionary, writer.state, nextObject, writer.pipeWriter);   
        
        writer.pipeWriter.closeLowLevelField();
        Pipe.confirmLowLevelWrite(writer.workPipe, Pipe.sizeOf(writer.workPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));//not sure needed when I am both threads
        Pipe.publishWrites(writer.workPipe);                    
        try {
            Pipe.takeMsgIdx(writer.workPipe);
            Pipe.writeFieldToOutputStream(writer.workPipe, out);
            Pipe.releaseReads(writer.workPipe);
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }

    public static void write(int[] intDictionary, long[] longDictionary, PhastWriter.CompressionState state, SequenceExampleA obj, DataOutputBlobWriter writer) {
        
        //TODO: write huffman 10 then the template position value
        //TODO: write huffman 0 then pmap and possible Run
        //TODO: Note run is optional and 0 is ok, should stop run on nested members
        
        writer.writePackedInt(obj.user);
        writer.writePackedInt(obj.year);
        writer.writePackedInt(obj.month);
        writer.writePackedInt(obj.date);
        writer.writePackedInt(obj.sampleCount);
        
        //TODO: generated times need to have steps that are a day or so.
        
        //leading mask byte for compression style.
    
        
        longDictionary[TIME_IDX] = 0;
        intDictionary[ID_IDX] = 0; //NOTE: this must be a reset message.
        intDictionary[MEASUREMENT_IDX] = 0;
        
        
        int count = obj.sampleCount; 
        SequenceExampleASample[] samples = obj.samples;
        state.samples = samples;
        state.initRun();
        int runLength = 0;
        long pmapHeader = 0;
        for(int i=0; i<count; i++) {
            
            SequenceExampleASample item = samples[i];
            
            //TODO: urgent. need generator to build sparse array.
            
            //map
            // id - did inc vs value
            // time - use zero or dif //mostly near last time
            // measure = use zero or dif //mostly near last measurement
            // action = use default or value //mostly the same few odd cases
                        
            
            if (--runLength<0) {
                pmapHeader = state.scanAheadForNext();
                runLength = state.runLength()-1; //sub tract one for this first usage. 
                
                writer.writePackedLong(pmapHeader);
            }
            
            encodeIncInt(writer, pmapHeader, item.id, 1); 
            encodeDeltaLong(longDictionary, writer, pmapHeader, TIME_IDX, 2, item.time);
            
            
            
            if (0 == (pmapHeader&4)) {
                writer.writePackedInt(item.measurement-intDictionary[MEASUREMENT_IDX]);
                intDictionary[MEASUREMENT_IDX] = item.measurement;
            }
            // writer.writePackedInt(item.measurement);
            
            
            
            if (0 != (pmapHeader&8)) {
                writer.writePackedInt(item.action); 
            }
            //writer.writePackedInt(item.action); 
            
        }
    }

    private static void encodeDeltaLong(long[] longDictionary, DataOutputBlobWriter writer, long pmapHeader, int idx,
            int bitMask, long value) {
        if (0 == (pmapHeader&bitMask)) {
            writer.writePackedLong(value-longDictionary[idx]);
            longDictionary[idx] = value;             
        }
    }

    private static void encodeIncInt(DataOutputBlobWriter writer, long pmapHeader, int value, int bitMask) {
        if (0 != (pmapHeader&bitMask)) {
            writer.writePackedInt(value);
        }
    }
    
    
}
