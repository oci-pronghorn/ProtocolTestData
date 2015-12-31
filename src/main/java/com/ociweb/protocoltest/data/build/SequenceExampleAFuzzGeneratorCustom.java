package com.ociweb.protocoltest.data.build;
import com.ociweb.pronghorn.pipe.stream.LowLevelStateManager;
import com.ociweb.protocoltest.data.SequenceExampleA;
import com.ociweb.protocoltest.data.SequenceExampleAFactory;
import com.ociweb.protocoltest.data.SequenceExampleASchema;
public class SequenceExampleAFuzzGeneratorCustom extends SequenceExampleAFactory {

    /////////////////////////////////////////////////////////////////////
    //WARNING: this class is a copy of the generated code and hand modified to try different optimization techniques to be put back into the code generator
    ///////////////////////////////////////////////////////////////////
    
    
public SequenceExampleAFuzzGeneratorCustom() { 
    startup();
}

private LowLevelStateManager navState;
private int mC;
private int mD;
private int mE;
private int mF;
private long m10 = 1450200779804L;
private int m11 = (-1);
private int m12;
private int m13;
private int m14;
private int m15;
private int m16;
private SequenceExampleA[] working = buildWorkspace();
private int workingIdx;
private final long BUILD_TIME = 1450200779804L;
private static final int DO_NOTHING = -3;

private static final boolean reUse = true;
private static final int sizeBits = 14; //use with reUse set this to large value and re-send when null; (was 5)
private static final int size = 1<<sizeBits;
private static final int mask = size-1;

@Override
public void run() {
    
      if (LowLevelStateManager.isStartNewMessage(navState) ) {
          processDaySummary();
      }               
      do {
           processDaySummarySamples(); 
      } while (!LowLevelStateManager.isStartNewMessage(navState));
}

private void processDaySummary() {
    processPipe1WriteDaySummary(
        100000+(0xFFFFFF & (m16 += 231)),     //TODO: add this crazy date generation to the code generator for dates.   
        2015+((m15 += 1) >> 9 ),
        (1+ (0xF&((m14 += 1) >> 5) ) ),
        1+(31 & (m13 += 1)),
        SequenceExampleASchema.FIXED_SAMPLE_COUNT
    );
}

private void processPipe1WriteDaySummary( int pUser,int pYear,int pMonth,int pDate,int pSamplesCount) {
    workingIdx = mask & (1+workingIdx);
    SequenceExampleA.setAll(working[workingIdx],  pUser,pYear,pMonth,pDate,pSamplesCount);

    LowLevelStateManager.processGroupLength(navState, 0, pSamplesCount);
}

private void processDaySummarySamples() {
    processPipe1WriteDaySummarySamples(
        ((SequenceExampleASchema.FIXED_SAMPLE_COUNT-1) & (m11 += 1)),
        (0x7FFFFFFFFFFFFFFFL & (m10 +=  43200000)),
        (0xFFF & (mF += 43)),
        ((15 & (mE += 41))<14) ? 5 : 0
    );
}

private void processPipe1WriteDaySummarySamples( int pId,long pTime,int pMeasurement,int pAction) {
    SequenceExampleA.setSample(working[workingIdx], LowLevelStateManager.interationIndex(navState), pId,pTime,pMeasurement,pAction);
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
    navState = new LowLevelStateManager(SequenceExampleASchema.FROM);
}
public SequenceExampleA[] buildWorkspace() {
    int i = size;
    SequenceExampleA[] working = new SequenceExampleA[i];
    while(--i>=0) {
        working[i] = new SequenceExampleA();
    }
    return working;
}
public SequenceExampleA nextObject(){
    if (reUse) {
        if (working[workingIdx].getSampleCount()>0) {
            return working[workingIdx];
        }
    }
    
    do {
        run();
    } while (!LowLevelStateManager.isStartNewMessage(navState));
    
    return working[workingIdx];
}
public void skip(int i) {
    while(--i>=0) {
        nextObject();
    }
}
};
