package com.ociweb.protocoltest.data;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.LowLevelStateManager;

public class SequenceExampleAPopulationFactory extends SequenceExampleAFactory {

    SequenceExampleA target;
    private LowLevelStateManager navState;
    private Pipe<SequenceExampleASchema> input;
    private int activeIdx;
    
    public SequenceExampleAPopulationFactory(Pipe<SequenceExampleASchema> input) {
        this.input = input;
        startup();
    }
    
    @Override
    public void startup() {
        target = new SequenceExampleA();
        target.ensureCapacity(target, SequenceExampleASchema.FIXED_SAMPLE_COUNT);
        navState = new LowLevelStateManager(SequenceExampleASchema.FROM);
        
        assert(0==Pipe.getReleaseBatchSize(input));
    }
    
    
    @Override
    public void run() {
        
        
        while (!Pipe.hasContentToRead(input,7+(6*SequenceExampleASchema.FIXED_SAMPLE_COUNT))) {
            Thread.yield();
        }
            
        do {
            int cursor = LowLevelStateManager.isStartNewMessage(navState) ? Pipe.takeMsgIdx(input) : LowLevelStateManager.activeCursor(navState);
            
            if (0==cursor) {
                processPipeDaySummary(input,navState);
                Pipe.takeValue(input);
               // Pipe.markBytesReadBase(input, Pipe.takeValue(input));
    //            Pipe.batchedReleasePublish(input, Pipe.getWorkingBlobRingTailPosition(input), Pipe.getWorkingTailPosition(input));
               // Pipe.confirmLowLevelRead(input, 7 /* fragment size */);     

            } else if (6==cursor) {
              
                    processPipeDaySummarySamples(input,navState);  
                    Pipe.takeValue(input);
                    //Pipe.markBytesReadBase(input, Pipe.takeValue(input));
        //            Pipe.batchedReleasePublish(input, Pipe.getWorkingBlobRingTailPosition(input), Pipe.getWorkingTailPosition(input));

              //  Pipe.confirmLowLevelRead(input, 6 /* fragment size */);
                
            } else if (-1==cursor) {
                Pipe.takeMsgIdx(input);
                Pipe.takeValue(input);
                return;
               // requestShutdown();
                
            } else {
                throw new UnsupportedOperationException("Unknown message type, rebuid with the new schema.");
                
            }
            
        } while (!LowLevelStateManager.isStartNewMessage(navState));
        
        Pipe.confirmLowLevelRead(input, 7 + (6*SequenceExampleASchema.FIXED_SAMPLE_COUNT));   
        Pipe.batchedReleasePublish(input, Pipe.getWorkingBlobRingTailPosition(input), Pipe.getWorkingTailPosition(input));
                   
           
    }


    private void processPipeDaySummary(Pipe input, LowLevelStateManager navState) {
        businessMethodDaySummary(
                Pipe.takeValue(input),
                Pipe.takeValue(input),
                Pipe.takeValue(input),
                Pipe.takeValue(input),
                LowLevelStateManager.processGroupLength(navState, 0, Pipe.takeValue(input)) );
    }

    private void businessMethodDaySummary(int pUser, int pYear, int pMonth, int pDate, int pSamplesCount) {
        SequenceExampleA.setAll(target, pUser, pYear, pMonth, pDate, pSamplesCount);
    }

    private void processPipeDaySummarySamples(Pipe input, LowLevelStateManager navState) {
        businessMethodDaySummarySamples(
                Pipe.takeValue(input),
                Pipe.takeLong(input),
                Pipe.takeValue(input),
                Pipe.takeValue(input)    );
        if (!LowLevelStateManager.closeSequenceIteration(navState)) {
            return; /* Repeat this fragment*/
        }
        LowLevelStateManager.closeFragment(navState);
    }

    private void businessMethodDaySummarySamples(int pId, long pTime, int pMeasurement, int pAction) {
        SequenceExampleA.setSample(target, activeIdx++, pId, pTime, pMeasurement, pAction);        
    }
    

    @Override
    public SequenceExampleA nextObject() {
        activeIdx = 0;
        run();//blocks till we get data
        return target;
    }

}
