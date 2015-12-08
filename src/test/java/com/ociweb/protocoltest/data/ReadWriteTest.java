package com.ociweb.protocoltest.data;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.code.LoaderUtil;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.StreamRegulator;
import com.ociweb.pronghorn.stage.test.FuzzGeneratorGenerator;
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
    
    private static final Logger log = LoggerFactory.getLogger(ReadWriteTest.class);
    
    //Never call this in production code, this is only done here becaue the code is not yet fully generated.
    private static SequenceExampleAFactory dynamicLoadOfFactory() {
        try {
            StringBuilder target = new StringBuilder();
            FuzzGeneratorGenerator ew = new SequenceExampleAGeneratorGenerator(target,  null, 11);

            try {
                ew.processSchema();
            } catch (IOException e) {
                System.out.println(target);
                e.printStackTrace();
                fail();
            } 
            
            Class generateClass = LoaderUtil.generateClass(ew.getPackageName(), ew.getClassName(), target, SequenceExampleASchema.instance.getClass());
            return (SequenceExampleAFactory)generateClass.newInstance();
            
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
    
    @Test
    public void runSpeedTest() {
        
               
        //Can we hit 1Gb with shuch a small message? 16 bytes.
        
        //NOTE the fastest handoff is 10-20 million messages per second
        //    this value goes down slowly as mesage size grows 
        //    larger messages will consume more bandwidth up into 10's Gbps
        
        
        final int totalMessageCount = 100000; //large fixed value for running the test
        Histogram histogram = new Histogram(3600000000000L, 3);
        
        
        long bitPerSecond = 1L*1024L*1024L*1024L;
        int maxWrittenChunksInFlight = 1000000;//keeping this large lowers the contention on head/tail
        int maxWrittenChunkSizeInBytes= 32;//10*1024;
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
                System.exit(-1);
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
        
        
        
        public MyConsumer(StreamRegulator regulator, int totalMessageCount, Histogram histogram) {
            this.totalMessageCount = totalMessageCount;
            this.histogram = histogram;
            this.reader = regulator.getBlobReader();
            this.regulator = regulator;
            
        }
        @Override
        public void run() {
            SequenceExampleA target = new SequenceExampleA();
            
            int i = totalMessageCount;
            StreamRegulator r = regulator; //TODO: may want to make same change in protocol test.
            Histogram h = histogram;
            while (i>0) {
                while (r.hasNextChunk() && --i>=0) {
                    //use something to read the data from the input stream
  
                        SequenceExampleASimpleReadWrite.read(target, reader);    
                    
                        //This should come from one of the fields inside the encoded message
                        try {
                        
                            long timeMessageWasSent;
                            timeMessageWasSent = ( ( (  (long)reader.read()) << 56) |     //Do not keep this code, for example only.         
                                                        ( (0xFFl & reader.read()) << 48) |
                                                        ( (0xFFl & reader.read()) << 40) |
                                                        ( (0xFFl & reader.read()) << 32) |
                                                        ( (0xFFl & reader.read()) << 24) |
                                                        ( (0xFFl & reader.read()) << 16) |
                                                        ( (0xFFl & reader.read()) << 8) |
                                                          (0xFFl & reader.read()) );
                            //Note after the message is decoded the latency for the message must be computed using.
                            long latency = System.nanoTime() - timeMessageWasSent;
                            if (latency>=0) {//conditional to protect against numerical overflow, see docs on nanoTime();
                                h.recordValue(latency);
                            }

                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } 
                        
                        
                        
                }
              //  Thread.yield(); //Only happens when the pipe is empty and there is nothing to read, eg consumer is faster than producer.  
            }
            
            
            
            
            
            
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
            
        }
        @Override
        public void run() {

           // SequenceExampleA nextObject = factory.nextObject();
            
            int i = totalMessageCount;
            while (i>0) {
                while (regulator.hasRoomForChunk() && --i>=0) { //Note we are only dec when ther is room for write
                    
                    //NOTE: the messages sent must contain the timestamp for now so we can compute latency per message 
                    long now = System.nanoTime();
                    
                    SequenceExampleA nextObject = factory.nextObject();
                    
                    SequenceExampleASimpleReadWrite.write(nextObject, writer);
                        
                    try {
                        writer.write((byte)(now >>> 56));
                        writer.write((byte)(now >>> 48));//Do not keep this code, for example only.
                        writer.write((byte)(now >>> 40));//Do not keep this code, for example only.
                        writer.write((byte)(now >>> 32));//Do not keep this code, for example only.
                        writer.write((byte)(now >>> 24));//Do not keep this code, for example only.
                        writer.write((byte)(now >>> 16));//Do not keep this code, for example only.
                        writer.write((byte)(now >>> 8));//Do not keep this code, for example only.
                        writer.write((byte) now);//Do not keep this code, for example only.
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }//Do not keep this code, for example only.
                      
                    
                    
                }
                Thread.yield(); //we are faster than the consumer
            }
            
            
            
        }
        
    }
    
}
