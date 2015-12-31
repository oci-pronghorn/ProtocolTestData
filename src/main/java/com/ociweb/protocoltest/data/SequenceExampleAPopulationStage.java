package com.ociweb.protocoltest.data;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.LowLevelStateManager;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

//TODO: this code is a hand example of what needs to be code generated (base classes for code gen are complete just need this top level logic)
public class SequenceExampleAPopulationStage extends PronghornStage {

    private LowLevelStateManager navState;
    private Pipe<SequenceExampleASchema> input;
    private long messages;
    private boolean usingFactory;

    protected SequenceExampleAPopulationStage(GraphManager graphManager, Pipe<SequenceExampleASchema> input) {
        super(graphManager, input, NONE);
        this.input = input;
        supportsBatchedPublish = true;
        supportsBatchedRelease = false;
        usingFactory = false;
    }


    // GENERATED LOW LEVEL READER 
    // # Low level API is the fastest way of reading from a pipe in a business semantic way. 
    // # Do not change the order that fields are read, this is fixed when using low level. 
    // # Do not remove any field reading, every field must be consumed when using low level. 

    // Details to keep in mind when you expect the schema to change over time
    // # Low level API is CAN be extensiable in the sense which means ignore unrecognized messages. 
    // # Low level API is CAN NOT be extensiable in the sense of dealing with mising or extra/new fields. 
    // # Low level API is CAN NOT be extensiable in the sense of dealing with fields encoded with different types. 
    //private static final int[] FROM_GUID = new int[]{817557882, (-432206532), (-2024756811), 0, (-89930446), (-857692475), 907000901, (-1090612769)};
    private static final long BUILD_TIME = 1450202638146L;

    public void startup() {
        //SequenceExampleASchema.FROM.validateGUID(FROM_GUID);
        navState = new LowLevelStateManager(SequenceExampleASchema.FROM);
    }

    @Override
    public void run() {
        if (usingFactory) {
            return;
        }
        while (Pipe.hasContentToRead(input,7+(6*SequenceExampleASchema.FIXED_SAMPLE_COUNT))) {
            
            int cursor = LowLevelStateManager.isStartNewMessage(navState) ? Pipe.takeMsgIdx(input) : LowLevelStateManager.activeCursor(navState);
            
            if (0==cursor) {
                processPipeDaySummary(input,navState);
               // Pipe.confirmLowLevelRead(input, 7 /* fragment size */);     
                Pipe.releaseReads(input);
            } else if (6==cursor) {
                do {
                    processPipeDaySummarySamples(input,navState);
                    Pipe.releaseReads(input);
                } while (!LowLevelStateManager.isStartNewMessage(navState));
                
              //  Pipe.confirmLowLevelRead(input, 6 /* fragment size */);
                
//            } else if (12==cursor) {
//                System.out.println("end"); //TODO: Remove, do not generate the  1 fragment size is not right
//                processPipeDaySummarySamplesEnd();
//                Pipe.confirmLowLevelRead(input, 1 /* fragment size */);
                
            } else if (-1==cursor) {
                Pipe.takeMsgIdx(input);
                Pipe.takeValue(input);
                requestShutdown();
                
            } else {
                throw new UnsupportedOperationException("Unknown message type, rebuid with the new schema.");
                
            }
            
            if (LowLevelStateManager.isStartNewMessage(navState)) {
                Pipe.confirmLowLevelRead(input, 7 + (6*SequenceExampleASchema.FIXED_SAMPLE_COUNT));   
            }
            
           
        }

    }
    
    
    public SequenceExampleAPopulationFactory getFactory() {
        usingFactory = true;
        input.initBuffers();
        return new SequenceExampleAPopulationFactory(input);
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
        messages++;
        
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
    }

    private void processPipeDaySummarySamplesEnd() {
        businessMethodDaySummarySamplesEnd(
        );
        LowLevelStateManager.closeFragment(navState);
    }

    private void businessMethodDaySummarySamplesEnd() {
    }

    public long totalMessages() {
       return messages;
    }

}
