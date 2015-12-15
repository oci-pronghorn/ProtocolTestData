package com.ociweb.protocoltest.data;

public class CompressionState {
        
        public int curPos;
        public long nextPMap;
        public long activePMap;
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
        

        private long buildPMap(SequenceExampleASample item) {
            //TODO: this will be generated based on object and schema.
            try {
             //   System.out.println("equals? "+item.id+" "+lastId);
                
//                 return  8|0|0|1; 
                 
//                NOTE: these are ints and should be longs.
                return Branchless.ifEquals(item.id, 1+lastId,          0, 1) |
                   Branchless.ifZero((int)item.time,                   2, 0) |
                   Branchless.ifZero(item.measurement,                 4, 0) |
                   Branchless.ifEquals(item.action, defaultAction,     0, 8);
            }  finally {
                lastId = item.id;
            }
        }

        
        
        public int runLength() {
            return runCountDown;
        }
        
        public long scanAheadForNext() {
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