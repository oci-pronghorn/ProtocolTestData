package com.ociweb.protocoltest.data;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.code.LoaderUtil;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.StreamRegulator;
import com.ociweb.pronghorn.stage.test.FuzzGeneratorGenerator;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.CPUMonitor;

public class ReadWriteTest {

    private static ThreadFactory factory = new ThreadFactory() {

        @Override
        public Thread newThread(Runnable r) { //TODO: may want to add to protocol test.
           Thread t = new Thread(r);
           t.setPriority(Thread.MAX_PRIORITY);//Must prevent external actvities from getting in the way of what is to be measured.
           return t;
        }
        
    };
    
    public static long estimateOrigBytes;
    
    private static final Logger log = LoggerFactory.getLogger(ReadWriteTest.class);
    
    private static Class generatedClass = null;
    
    //Never call this in production code, this is only done here becaue the code is not yet fully generated.
    private static synchronized SequenceExampleAFactory dynamicLoadOfFactory() {
        try {
            if (null==generatedClass) {
                StringBuilder target = new StringBuilder();
                FuzzGeneratorGenerator ew = new SequenceExampleAGeneratorGenerator(target,  null, 11);
                    
                try {
                    ew.processSchema();
                    System.out.println("dynamicLoadOfFactory() :\n\n"+ target);
                } catch (IOException e) {
                    System.out.println(target);
                    e.printStackTrace();
                    fail();
                } 
                
                System.out.println(target);
                
                generatedClass = LoaderUtil.generateClass(ew.getPackageName(), ew.getClassName(), target, SequenceExampleASchema.instance.getClass());
            }
            
            
            return (SequenceExampleAFactory)generatedClass.newInstance();
            
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }
    
    @Ignore
    public void runSpeedTest() {
        runLoad();
    }
    
    public static void main(String[] arg) {
        runLoad();
    }

    private static void runLoad() {
        //Can we hit 1Gb with shuch a small message? 16 bytes.
        
        //NOTE the fastest handoff is 10-20 million messages per second
        //    this value goes down slowly as mesage size grows 
        //    larger messages will consume more bandwidth up into 10's Gbps
        
        
        final int totalMessageCount = 10000; //large fixed value for running the test
        Histogram histogram = new Histogram(3600000000000L, 3);
        
        
        long bitPerSecond = 100L*1024L*1024L*1024L;
        int maxWrittenChunksInFlight = 10000;//keeping this large lowers the contention on head/tail
        int maxWrittenChunkSizeInBytes= 40*1024;
        StreamRegulator regulator = new StreamRegulator(bitPerSecond, maxWrittenChunksInFlight, maxWrittenChunkSizeInBytes);
                
        CPUMonitor cpuMonitor = new CPUMonitor(100);
        
        ExecutorService executor = Executors.newFixedThreadPool(2,factory);
        
        Runnable p = buildProducer(totalMessageCount, regulator);
        Runnable c = buildConsumer(totalMessageCount, histogram, regulator);
           
        long startTime = System.currentTimeMillis();
        
        cpuMonitor.start();
        executor.execute(p);
        executor.execute(c);
        
        executor.shutdown();//prevent any new submissions to execution service but let those started run.
                 
        try {
            if (!executor.awaitTermination(2000, TimeUnit.SECONDS)) {
                log.error("test time out, no valid results");
                return;
            }
        } catch (InterruptedException e) {
            //Nothing to do Just exit
        }
        Histogram cpuHist = cpuMonitor.stop();
        
        long totalBytesSent =regulator.getBytesWritten();
        long durationInMs = System.currentTimeMillis()-startTime;
        
        long bitsSent = totalBytesSent * 8L;
        float mBitsPerSec = (1000L*bitsSent)/(float)(durationInMs*1024*1024); 
        float kBitsPerSec = (1000L*bitsSent)/(float)(durationInMs*1024); 

        long kmsgPerSec = totalMessageCount/durationInMs;
        
        System.out.println("Latency Value in microseconds");
        histogram.outputPercentileDistribution(System.out, 1000.0);
        
        System.out.println();
        System.out.println("Process CPU Usage (All threads started by this Java instance)");
        cpuHist.outputPercentileDistribution(System.out, CPUMonitor.UNIT_SCALING_RATIO);
        
        //TODO: how do we know the compression ratio? ask the factory for the raw size?
      
        
        float fraction = ((float)totalBytesSent) / ((float)estimateOrigBytes);        
        float compression = 100f*(1f - fraction);
        log.info("Pct compressed {}",compression);
        log.info("Raw bytes :"+estimateOrigBytes);
        
        log.info("K Mgs Per Second {}",kmsgPerSec);
        log.info("Total duration {}ms",durationInMs);
        log.info("TotalBytes {}",totalBytesSent);
        
        log.info("{} Kbps",kBitsPerSec);        
        log.info("{} Mbps",mBitsPerSec);
    }

    public static Runnable buildConsumer(int totalMessageCount, Histogram histogram, StreamRegulator regulator) {
        return new MyConsumer(regulator, totalMessageCount, histogram);
    }

    public static Runnable buildProducer(int totalMessageCount, StreamRegulator regulator) {
        return new MyProducer(regulator, totalMessageCount);
    }
    
    private static class MyConsumer implements Runnable {

        private int totalMessageCount; 
        private Histogram histogram;
        private DataInputBlobReader<RawDataSchema> reader;
        private StreamRegulator regulator;
        private SequenceExampleAFactory factory;
                    
        
        public MyConsumer(StreamRegulator regulator, int totalMessageCount, Histogram histogram) {
            this.totalMessageCount = totalMessageCount;
            this.histogram = histogram;
            this.reader = regulator.getBlobReader();
            this.regulator = regulator;
            this.factory = dynamicLoadOfFactory();
            //this.factory = new TestFactory();
            
        }
        @Override
        public void run() {
                    
            
            SequenceExampleA target = new SequenceExampleA();
            SequenceExampleA.ensureCapacity(target, 1<<11);
            
            PhastReader pReader = new PhastReader();
            
            int i = totalMessageCount;
           
            long lastNow = 0;
            
            SequenceExampleA nextObject = factory.nextObject();
            Histogram h = histogram;
            while (i>0) {
                while (regulator.hasNextChunk() && --i>=0) {
                        lastNow = recordLatency(lastNow, h, reader);     
                    
                        SequenceExampleA result = PhastReader.readFromInputStream(pReader, target, (InputStream) reader);
                       
                        
                        if(!result.equals(nextObject)) {
                            System.out.println("error "+i);  
                        }
                    
                        nextObject = factory.nextObject();                       
                }
                Thread.yield(); //Only happens when the pipe is empty and there is nothing to read, eg consumer is faster than producer.  
            }
             
            
        }
        
        
        
        public static long recordLatency(long lastNow, Histogram h, DataInputBlobReader<RawDataSchema> reader) {
            long timeMessageWasSentDelta = reader.readPackedLong();
            
            lastNow += timeMessageWasSentDelta;                            
            //Note after the message is decoded the latency for the message must be computed using.
            
            long latency = System.nanoTime() - lastNow;
            if (latency>=0 && 0!=lastNow) {//conditional to protect against numerical overflow, see docs on nanoTime();
                h.recordValue(latency);
            }
            return lastNow;
        }
        
    }
   
    private static class MyProducer implements Runnable {

        private DataOutputBlobWriter<RawDataSchema> writer;
        private int totalMessageCount;
        private SequenceExampleAFactory factory;
        private StreamRegulator regulator;
                


        public MyProducer(StreamRegulator regulator, int totalMessageCount) {
            this.regulator = regulator;
            this.writer = regulator.getBlobWriter();
            this.totalMessageCount = totalMessageCount;
            this.factory = dynamicLoadOfFactory();
            //this.factory = new TestFactory();
            
            
        }
        @Override
        public void run() {
           

            long lastNow = 0;           
            
            SequenceExampleA nextObject = factory.nextObject();
            estimateOrigBytes = nextObject.estimatedBytes()*(long)totalMessageCount;  
            
            OutputStream out = (OutputStream)writer;
            
            PhastWriter pWriter = new PhastWriter();
            
            int i = totalMessageCount;
            while (i>0) {
                while (regulator.hasRoomForChunk() && --i>=0) { //Note we are only dec when ther is room for write
                    lastNow = recordSentTime(lastNow, writer);               
                    
                    PhastWriter.writeToOuputStream(pWriter, nextObject, out);
                    
                    nextObject = factory.nextObject();
                    
                }
                Thread.yield(); //we are faster than the consumer
            }
                        
            
        }

        
        public static long recordSentTime(long lastNow, DataOutputBlobWriter<RawDataSchema> writer) {
            long now = System.nanoTime();
            if (now < lastNow) {//defend against the case that this is not "real" time and can move backwards.
                now = lastNow;
            }
            writer.writePackedLong(now-lastNow);                       
            return now;
        }
        
    }
    
}
