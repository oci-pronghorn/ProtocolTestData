package com.ociweb.protocoltest.data;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;

public class SequenceExampleASimpleReadWrite {

    //TODO: will be code generated from schema from
    
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
                return ifEquals(item.id, 1+lastId,          0, 1) |
                   ifZero((int)item.time,            2, 0) |
                   ifZero(item.measurement,          4, 0) |
                   ifEquals(item.action, defaultAction, 0, 8);
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
    
    //TODO: move to utility class and build speed tests against mixed conditionals
    private static int ifEquals(int x, int y, int a, int b) {
        
      return x==y?a:b; 
      //return onEqu1(x, y, a, b, ((x-y)-1)>>31);
    }
    
    private static int ifZero(int x, int a, int b) {
        return x==0?a:b; 
        //return onZero1(x, a, b, (x-1)>>31);
      }

    private static int onZero1(int x, int a, int b, int tmp) {
        return onEqu2(a, b, ((x>>31)^tmp)&tmp);
      }

    private static int onEqu1(int x, int y, int a, int b, int tmp) {
      return onEqu2(a, b, (((x-y)>>31)^tmp)&tmp);
    }


    private static int onEqu2(int a, int b, int mask) {
        return (a&mask)|(b&(~mask));
    }
    
    public final static int valueZero = 0;
    public final static long timeZero = 0;
    public final static int defaultAction = 5;
    
    private static CompressionState state = new CompressionState();
    
    public static void write(SequenceExampleA obj, DataOutputBlobWriter writer) {
        
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

        long lastTimeForDelta = 0;
        int lastMeasurementForDelta = 0;
        
        int count = obj.sampleCount; 
        SequenceExampleASample[] samples = obj.samples;
        state.samples = samples;
        state.initRun();
        int runLength = 0;
        int pmapHeader = 0;
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
                
                writer.writePackedInt(pmapHeader);
            }
            
            if (0 != (pmapHeader&1)) {
                writer.writePackedInt(item.id);
             //   assert(0==i);
            } //else {
               // assert(0!=i);
            //}
            // writer.writePackedInt(item.id);
            
            if (0 == (pmapHeader&2)) {
                writer.writePackedLong(item.time-lastTimeForDelta);
                lastTimeForDelta = item.time;                
            }
            //  writer.writePackedLong(item.time);
            
            
            if (0 == (pmapHeader&4)) {
                writer.writePackedInt(item.measurement-lastMeasurementForDelta);
                lastMeasurementForDelta = item.measurement;
            }
            // writer.writePackedInt(item.measurement);
            
            
            
            if (0 != (pmapHeader&8)) {
                writer.writePackedInt(item.action); 
            }
            //writer.writePackedInt(item.action); 
            
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
        
        
        int runCount = 0;
        int map = 0;
        for(int i=0; i<count; i++) {
            
            
            if (--runCount<0) {
                map = reader.readPackedInt();
                runCount = map>>4;
                runCount--; //for this usage;
            }
            
            SequenceExampleASample item = obj.samples[i];
            
            
            if (0!=(map&1)) {
                id = reader.readPackedInt();
                //do something
            }
            item.id = id++;
            // item.id = reader.readPackedInt();  
            
            
            if (0==(map&2)) {
                long timeDif = reader.readPackedLong();
                timeCache = timeCache+timeDif;
                item.time = timeCache;
            } else {
                item.time = timeZero;//absent default
            }
            // item.time = reader.readPackedLong();  
            
            
            if (0==(map&4)) {
                int measureDif = reader.readPackedInt();
                measureCache =  measureCache+measureDif;
                item.measurement = measureCache;            
            } else {
                item.measurement = valueZero; //absent default
            }
            //item.measurement = reader.readPackedInt();
            
            if (0==(map&8)) {
                item.action = defaultAction;
            } else {
                item.action = reader.readPackedInt();  
                
            }
            
        }
        
    }
    
    
    
    
}
