package com.ociweb.protocoltest.data;

import java.io.IOException;
import java.io.OutputStream;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class PhastWriter {

    private Pipe<RawDataSchema> workPipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 4, 50000));
    private DataOutputBlobWriter<RawDataSchema> pipeWriter;
    private CompressionState state = new CompressionState();
    
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

    public static void write(int[] intDictionary, long[] longDictionary, CompressionState samplesPMapRLEBuilder, SequenceExampleA obj, DataOutputBlobWriter writer) {
        
        //First (low) bits:
        // 0 <PMAP>
        // 10 <TemplateIDX Position>
        // 110 <Reset step count>
        // 1110 <?? template ID>
        
        int msgIdx = 0; //template
        DataOutputBlobWriter.writePackedInt(writer, (msgIdx<<2)|2);
        
        
        long pmapTemplateHeader = 0; //Do we need a count on top of 1?
        DataOutputBlobWriter.writePackedLong(writer, pmapTemplateHeader);
        
        //first bit is zero to indicate pmap
        PhastEncoder.encodeDeltaInt(intDictionary, writer, pmapTemplateHeader, 2, USER_IDX, obj.user);
        PhastEncoder.encodeDeltaInt(intDictionary, writer, pmapTemplateHeader, 4, YEAR_IDX, obj.year);
        PhastEncoder.encodeDeltaInt(intDictionary, writer, pmapTemplateHeader, 8, MONTH_IDX, obj.month);
        PhastEncoder.encodeDeltaInt(intDictionary, writer, pmapTemplateHeader, 16, DATE_IDX, obj.date);
        PhastEncoder.encodeIntPresent(writer, pmapTemplateHeader, 32, obj.sampleCount);
        
        
        //TODO: generated times need to have steps that are a day or so.
        
        //leading mask byte for compression style.
    
        
        longDictionary[TIME_IDX] = 0;
        intDictionary[ID_IDX] = 0; //NOTE: this must be a reset message.
        intDictionary[MEASUREMENT_IDX] = 0;
        
        
        int count = obj.sampleCount; 
        SequenceExampleASample[] samples = obj.samples;
        samplesPMapRLEBuilder.samples = samples;
        samplesPMapRLEBuilder.initRun();
        int runLength = 0;
        long pmapHeader = 0;
        for(int i=0; i<count; i++) {
            
            SequenceExampleASample item = samples[i];
            
            //map
            // id - did inc vs value
            // time - use zero or dif //mostly near last time
            // measure = use zero or dif //mostly near last measurement
            // action = use default or value //mostly the same few odd cases
                        
            
            if (--runLength<0) {
                pmapHeader = samplesPMapRLEBuilder.scanAheadForNext();
                runLength = samplesPMapRLEBuilder.runLength()-1; //sub tract one for this first usage. 
                
                DataOutputBlobWriter.writePackedInt(writer, 6);//fragmentIdx;
                DataOutputBlobWriter.writePackedLong(writer, pmapHeader);
            }
            
            PhastEncoder.encodeIntPresent(writer, pmapHeader, 2, item.id); 
            PhastEncoder.encodeDeltaLong(longDictionary, writer, pmapHeader, TIME_IDX, 4, item.time);
            PhastEncoder.encodeDeltaInt(intDictionary, writer, pmapHeader, 8, MEASUREMENT_IDX, item.measurement);
            PhastEncoder.encodeIntPresent(writer, pmapHeader, 16, item.action);

        }
    }

    
    
}
