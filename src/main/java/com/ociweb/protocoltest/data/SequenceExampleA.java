package com.ociweb.protocoltest.data;

import java.io.IOException;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.util.Appendables;

public class SequenceExampleA {

    private final static Logger log = LoggerFactory.getLogger(SequenceExampleA.class);
    private final static SequenceExampleASample[] DEFAULT_EMPTY = new SequenceExampleASample[0];
    
    int user;
    int year;
    int month;
    int date;
    int sampleCount;
    
    SequenceExampleASample[] samples = DEFAULT_EMPTY;
    
    private transient List<SequenceExampleASample> samplesAsList;

    public String toString() {
        try {
            return appendToString(new StringBuilder()).toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public long estimatedBytes() {
        return (5*4)+(sampleCount*SequenceExampleASample.estimatedBytes());
    }
    
    public static long estimatedBytes(int sampleCount) {
        return (5*4)+(sampleCount*SequenceExampleASample.estimatedBytes());
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SequenceExampleA) {
            SequenceExampleA that = (SequenceExampleA)obj;
            return this.user == that.user &&
                   this.year == that.year &&
                   this.month == that.month &&
                   this.date == that.date &&
                   this.sampleCount == that.sampleCount &&
                   boundedEquals(this.samples,that.samples, that.sampleCount);
                 
        }
        return false;
    }

    private boolean boundedEquals(SequenceExampleASample[] samplesA, 
                                  SequenceExampleASample[] samplesB,
                                  int sampleCount) {
       for(int i = 0; i<sampleCount; i++) {
           
           SequenceExampleASample leftSide = samplesA[i];
           SequenceExampleASample rightSide = samplesB[i];
           
           if (leftSide==null) {
               return rightSide==null;//true when both are null;
           }
           if (!leftSide.equals(rightSide)) {
               log.info("does not equal at index:"+i+"\n  "+leftSide+"  "+rightSide);
               return false;
           }
       }
       return true;
    }

    private <A extends Appendable> A appendToString(A target) throws IOException {
        Appendables.appendValue(target,"User:" ,user,"\n");
        Appendables.appendValue(target,"Year:" ,year,"\n");
        Appendables.appendValue(target,"Month:",month,"\n");
        Appendables.appendValue(target,"Date:" ,date,"\n");
        Appendables.appendValue(target,"SampleCount:",sampleCount,"\n");
        
        for(int i=0; i< sampleCount; i++) {
            target.append("----------------\n");
            samples[i].appendToString(target);
        }
        return target;
    }
    
    public static void setAll(SequenceExampleA that, int user, int year, int month, int date, int sampleCount) {
        that.user = user;
        that.year = year;
        that.month = month;
        that.date = date;
        if (sampleCount>that.sampleCount) {
            that.sampleCount = sampleCount;
            ensureCapacity(that,sampleCount);            
        } else {        
            that.sampleCount = sampleCount;
        }
        
    }
    
    public int getUser() {
        return user;
    }

    public void setUser(int user) {
        this.user = user;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDate() {
        return date;
    }

    public void setDate(int date) {
        this.date = date;
    }

    public int getSampleCount() {
        return sampleCount;
    }
    
    public List<SequenceExampleASample> getSamples() {
        if (null==samplesAsList) {
            samplesAsList = new SampleList();
        }
        return samplesAsList;
    }

    public void setSampleCount(int sampleCount) {
        this.sampleCount = sampleCount;
        ensureCapacity(this, sampleCount);
    }

    public static void ensureCapacity(SequenceExampleA that, int sampleCount) {
        //System.out.println("capacity of "+sampleCount);
        //TODO: NOTE when the sample count is zero we crash because it gets run anyway - FIX ASAP
        
        
        if (null==that.samples || that.samples.length<sampleCount) {
            
            if (sampleCount==0) {
                sampleCount = 1;
            }
            that.samples = new SequenceExampleASample[sampleCount];
            int i = sampleCount;
            while (--i>=0) {
                that.samples[i] = new SequenceExampleASample();
            } 
        }
    }
    
    private class SampleList extends AbstractList<SequenceExampleASample> {

        @Override
        public SequenceExampleASample get(int index) {
            return samples[index];
        }

        @Override
        public int size() {
           return sampleCount;
        }
    }

    public static void setSample(SequenceExampleA obj, int idx, int id, long time, int measurement, int action) {
        //NOTE: logic to turn around count down value, needs cleanup.
        SequenceExampleASample.setAll(obj.samples[obj.sampleCount-(idx+1)], id, time, measurement, action);   
    }
    
}
