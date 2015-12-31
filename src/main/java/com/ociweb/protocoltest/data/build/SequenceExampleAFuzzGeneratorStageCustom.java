package com.ociweb.protocoltest.data.build;
import com.ociweb.pronghorn.pipe.stream.LowLevelStateManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.protocoltest.data.SequenceExampleASchema;
import com.ociweb.protocoltest.data.SequenceExampleA;
import com.ociweb.protocoltest.data.SequenceExampleAFactory;
import com.ociweb.protocoltest.data.SequenceExampleAPopulationFactory;
public class SequenceExampleAFuzzGeneratorStageCustom extends PronghornStage {


    /////////////////////////////////////////////////////////////////////
    //WARNING: this class is a copy of the generated code and hand modified to try different optimization techniques to be put back into the code generator
    ///////////////////////////////////////////////////////////////////
    
    
public SequenceExampleAFuzzGeneratorStageCustom(com.ociweb.pronghorn.stage.scheduling.GraphManager gm, Pipe<SequenceExampleASchema>  output) {
    super(gm,NONE,output);
    this.output = output;
    Pipe.from(output).validateGUID(FROM_GUID);
    supportsBatchedPublish = false;
}

private LowLevelStateManager navState;
private Pipe<SequenceExampleASchema> output;
    private int m1;
    private int m2;
    private int m3;
    private int m4;
    private long m5 = 1450151600555L;
    private int m6 = (-1);
    private int m7;
    private int m8;
    private int m9;
    private int mA;
    private int mB;
  //  SequenceExampleA[] working = buildWorkspace();
 //   int workingIdx;
private final int[] FROM_GUID = new int[]{817557882, (-432206532), (-2024756811), 0, (-89930446), (-857692475), 907000901, (-1090612769)};
private final long BUILD_TIME = 1450151600555L;
private static final int DO_NOTHING = -3;

private int nextMessageIdx() {//make private!!!
    return 0;
}


@Override
public void run() {
    
    
    if (Pipe.hasRoomForWrite(output, 7+(6*SequenceExampleASchema.FIXED_SAMPLE_COUNT))) {
      if (LowLevelStateManager.isStartNewMessage(navState) ) {
          Pipe.addMsgIdx(output,nextMessageIdx());
          processDaySummary();
          Pipe.writeTrailingCountOfBytesConsumed(output, Pipe.getWorkingHeadPositionObject(output).value++); 
      }
               
      do {
//              int cursor = LowLevelStateManager.activeCursor(navState);        
//              if (6==cursor) {
                  processDaySummarySamples();
//              } else {
//                  if (0==cursor) {
//                      Pipe.addMsgIdx(output,0);
//                      processDaySummary();
//                  } else {
//                      throw new UnsupportedOperationException("Unknown message type "+cursor+", rebuid with the new schema.");
//                  }
//              }
             //happens at the end of every fragment
            Pipe.writeTrailingCountOfBytesConsumed(output, Pipe.getWorkingHeadPositionObject(output).value++); //increment because this is the low-level API calling
            
      } while (!LowLevelStateManager.isStartNewMessage(navState));
      Pipe.confirmLowLevelWrite(output, 7+(6*SequenceExampleASchema.FIXED_SAMPLE_COUNT));
      Pipe.publishWritesBatched(output);
      
    }
    
    
//    while (Pipe.hasRoomForWrite(output)) {
//        int cursor = LowLevelStateManager.isStartNewMessage(navState) ? nextMessageIdx() : LowLevelStateManager.activeCursor(navState);
//        if (6==cursor) {
//            processDaySummarySamples();
//            Pipe.confirmLowLevelWrite(output, 6/* fragment 6  size 6*/);
//        } else {
//            if (0==cursor) {
//                Pipe.addMsgIdx(output,0);
//                processDaySummary();
//                Pipe.confirmLowLevelWrite(output, 7/* fragment 0  size 6*/);
//            } else if (12==cursor){
//                processDaySummarySamplesEnd();
//                Pipe.confirmLowLevelWrite(output, 1/* fragment 12  size 1*/);
//            } else {
//                throw new UnsupportedOperationException("Unknown message type "+cursor+", rebuid with the new schema.");
//            }
//        }
//        
////        
////        switch(cursor) {
////            case /*processPipe1WriteDaySummarySamples*/6:
////                processDaySummarySamples();
////                Pipe.confirmLowLevelWrite(output, 6/* fragment 6  size 6*/);
////            break;
////            case /*processPipe1WriteDaySummary*/0:
////                Pipe.addMsgIdx(output,0);
////                processDaySummary();
////                Pipe.confirmLowLevelWrite(output, 7/* fragment 0  size 6*/);
////            break;
////            case /*processPipe1WriteDaySummarySamplesEnd*/12:
////                processDaySummarySamplesEnd();
////                Pipe.confirmLowLevelWrite(output, 1/* fragment 12  size 1*/);
////            break;
////            default:
////                testMe(cursor);
////        }
//        Pipe.publishWrites(output);
   // }
}

private void testMe(int cursor) {
    throw new UnsupportedOperationException("Unknown message type "+cursor+", rebuid with the new schema.");
}

private void processDaySummary() {
    processPipe1WriteDaySummary(
        (0xFFF & (mB += 29)),
        (0xFFF & (mA += 23)),
        (0xFFF & (m9 += 19)),
        (0xFFF & (m8 += 17)),
        SequenceExampleASchema.FIXED_SAMPLE_COUNT, output, navState
    );
}

private void processPipe1WriteDaySummary( int pUser,int pYear,int pMonth,int pDate,int pSamplesCount, Pipe output, LowLevelStateManager navState) {
 //   workingIdx = 0x1F & (1+workingIdx);
 //   SequenceExampleA.setAll(working[workingIdx],  pUser,pYear,pMonth,pDate,pSamplesCount);
    Pipe.addIntValue(pUser,output);
    Pipe.addIntValue(pYear,output);
    Pipe.addIntValue(pMonth,output);
    Pipe.addIntValue(pDate,output);

    Pipe.addIntValue(pSamplesCount,output);
    LowLevelStateManager.processGroupLength(navState, 0, pSamplesCount);
}

private void processDaySummarySamples() {
    processPipe1WriteDaySummarySamples(
        ((SequenceExampleASchema.FIXED_SAMPLE_COUNT-1) & (m6 += 1)),
        (0x7FFFFFFFFFFFFFFFL & (m5 += 43200000)),
        (0xFFF & (m4 += 11)),
        (0x7 & (m3 += 7)), output, navState
    );
}

private void processPipe1WriteDaySummarySamples( int pId,long pTime,int pMeasurement,int pAction, Pipe output, LowLevelStateManager navState) {
  //  SequenceExampleA.setSample(working[workingIdx], LowLevelStateManager.interationIndex(navState), pId,pTime,pMeasurement,pAction);
    Pipe.addIntValue(pId,output);
    Pipe.addLongValue(pTime,output);
    Pipe.addIntValue(pMeasurement,output);
    Pipe.addIntValue(pAction,output);
    if (!LowLevelStateManager.closeSequenceIteration(navState)) {
        return; /* Repeat this fragment*/
    }
    LowLevelStateManager.closeFragment(navState);
}

private void processDaySummarySamplesEnd() {
    processPipe1WriteDaySummarySamplesEnd(

    );
}

private void processPipe1WriteDaySummarySamplesEnd() {
    LowLevelStateManager.closeFragment(navState);
}

@Override
public void startup() {
    navState = new LowLevelStateManager(Pipe.from(output));
}
//public SequenceExampleA[] buildWorkspace() {
//    int i = 32;
//    SequenceExampleA[] working = new SequenceExampleA[i];
//    while(--i>=0) {
//        working[i] = new SequenceExampleA();
//    }
//    return working;
//}
//public SequenceExampleA nextObject(){
//    do {
//        run();
//    } while (!LowLevelStateManager.isStartNewMessage(navState));
//    return working[workingIdx];
//}
//public void skip(int i) {
//    while(--i>=0) {
//        nextObject();
//    }
//}
};
