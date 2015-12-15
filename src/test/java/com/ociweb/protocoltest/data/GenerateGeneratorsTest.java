package com.ociweb.protocoltest.data;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.Ignore;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.ociweb.pronghorn.code.LoaderUtil;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.pipe.util.build.TemplateProcessGeneratorLowLevelReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
import com.ociweb.pronghorn.stage.test.ConsoleJSONDumpStage;
import com.ociweb.pronghorn.stage.test.ConsoleSummaryStage;
import com.ociweb.pronghorn.stage.test.FuzzGeneratorGenerator;
import com.ociweb.pronghorn.stage.test.PipeCleanerStage;
import com.ociweb.pronghorn.util.NullAppendable;
import com.ociweb.protocoltest.data.build.SequenceExampleAFuzzGeneratorCustom;
import com.ociweb.protocoltest.data.build.SequenceExampleAFuzzGeneratorStageCustom;

public class GenerateGeneratorsTest {

    /**
     * TODO: must gen runnable without pipe for fast building of of test data, faster than pipes
     *       note using the backing array will work best, or re-usable object
     *       must always produce object for fair test.
     */
    @Test
    public void generateSequenceExampleA() {
        
        StringBuilder target = new StringBuilder();
        FuzzGeneratorGenerator ew = new SequenceExampleAGeneratorGenerator(target,  null, 11);

        try {
            ew.processSchema();
        } catch (IOException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        }        
        
      //  System.out.println(target);
        
        Class clazz = validateCleanCompile(ew.getPackageName(), ew.getClassName(), target, SequenceExampleASchema.instance);
        
        try {
            
            Object obj = clazz.newInstance();
            
            SequenceExampleAFactory factory = (SequenceExampleAFactory)obj;
            factory = new SequenceExampleAFuzzGeneratorCustom();
            
            
            factory.startup();
            
            int count = 20000;
            long startTime = System.currentTimeMillis();
            
            int i = count;
            while (--i>=0) {
                
                SequenceExampleA temp = factory.nextObject();
                                
            }
                        
            
            long duration = System.currentTimeMillis()-startTime;
            
            System.out.println("duration:"+duration);
            
            //calls per second
            long perSec = (1000*count)/duration;
            System.out.println("perSecond:"+perSec);
            
            
        } catch (InstantiationException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        } catch (IllegalAccessException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        } catch (SecurityException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        } catch (IllegalArgumentException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        } 
    }
    
    @Test
    public void generateSequenceExampleAStage() {
        
        StringBuilder target = new StringBuilder();
        
        FuzzGeneratorGenerator ew = new SequenceExampleAGeneratorGenerator(target, 11);

        long estMsgSize = SequenceExampleA.estimatedBytes(1<<11);
        
        try {
            ew.processSchema();
        } catch (IOException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        }        
        
      //  System.out.println(target);
        
        Class clazz = validateCleanCompile(ew.getPackageName(), ew.getClassName(), target, SequenceExampleASchema.instance);
        
        int pipeLength = (2+(1<<11))*160;
        
        try {            
            
            GraphManager gm = new GraphManager();
            
            //NOTE: Since the ConsoleSummaryStage usess the HighLevel API the pipe MUST be large enough to hold and entire message
            //      Would be nice to detect this failure, not sure how.
            Pipe<SequenceExampleASchema> pipe = new Pipe<SequenceExampleASchema>(new PipeConfig<SequenceExampleASchema>(SequenceExampleASchema.instance, pipeLength));           
            
            Constructor constructor =  LoaderUtil.generateClassConstructor(ew.getPackageName(), ew.getClassName(), target, SequenceExampleASchema.class);
            PronghornStage stage = (PronghornStage)constructor.newInstance(gm, pipe); //32K

           // PronghornStage stage = new SequenceExampleAFuzzGeneratorStageCustom(gm,pipe); //45K - remove switch and passed down pipes

            
            SequenceExampleAPopulationStage dump = new SequenceExampleAPopulationStage(gm, pipe);
            
            //PipeCleanerStage dump = new PipeCleanerStage(gm, pipe);
            
//            Appendable out = new NullAppendable();
//            ConsoleSummaryStage dump = new ConsoleSummaryStage(gm, pipe, out );
            
            //ConsoleJSONDumpStage<SequenceExampleASchema> dump = new ConsoleJSONDumpStage<>(gm, pipe);
                        
            GraphManager.enableBatching(gm);
          //  MonitorConsoleStage.attach(gm);
            
            ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
          //  scheduler.playNice=false;
            
            long startup = System.currentTimeMillis();
            scheduler.startup();
                        
            Thread.sleep(2200);
            
            //stage.requestShutdown();
           
            scheduler.shutdown();
            
            scheduler.awaitTermination(3, TimeUnit.SECONDS);
            long durationMS = System.currentTimeMillis()-startup;//measured for better accuracy
            
            long totalBytes = dump.totalMessages()*estMsgSize;//totalBytes();
            long totalMessages = dump.totalMessages();//totalBytes/estMsgSize;//this is an estimate but very close to right
            
            long bitsPerSecond = (8L * totalBytes ) / durationMS;
            System.out.println("kbps: "+bitsPerSecond);
            
            long msgPerSecond = 1000L*totalMessages/durationMS;
            System.out.println("Messages per second:"+msgPerSecond);
            
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    private static Class validateCleanCompile(String packageName, String className, StringBuilder target, MessageSchema schema) {
        try {
        
        Class generateClass = LoaderUtil.generateClass(packageName, className, target, schema.getClass());//NOTE: schema must be class from THHIS project.
        
        if (generateClass.isAssignableFrom(PronghornStage.class)) {
            Constructor constructor =  generateClass.getConstructor(GraphManager.class, Pipe.class);
            assertNotNull(constructor);
        }

        return generateClass;
        
        } catch (ClassNotFoundException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        } catch (NoSuchMethodException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        } catch (SecurityException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        }
        return null;
        
    }
    
//    @Test
//    public void testGenerateLowLevelReaderCleanCompile() {
//        
//        try {
//                        
//            String className = "LowLevelReader";
//            
//            StringBuilder target = new StringBuilder();
//                        
//            //TODO: turn this into a stage generator for building the object production stage
//            //TODO: use the hand coded example first as a template.
//            TemplateProcessGeneratorLowLevelReader simple = new TemplateProcessGeneratorLowLevelReader(SequenceExampleASchema.instance, target);
//            
//            simple.processSchema();
//            
//            System.out.println(target);
//            
//            validateCleanCompile("com.ociweb.pronghorn.test.build",className, target, SequenceExampleASchema.instance );
//            
//        } catch (Exception e) {
//            e.printStackTrace();
//            fail();
//        }
//    }
    
    
    @Ignore
    public void generateSequenceExampleAFactory() {
        
        StringBuilder target = new StringBuilder();
        
        FuzzGeneratorGenerator ew = new SequenceExampleAGeneratorGenerator(target, 11);

        long estMsgSize = SequenceExampleA.estimatedBytes(1<<11);
        
        try {
            ew.processSchema();
        } catch (IOException e) {
            System.out.println(target);
            e.printStackTrace();
            fail();
        }        
        
      //  System.out.println(target);
        
        Class clazz = validateCleanCompile(ew.getPackageName(), ew.getClassName(), target, SequenceExampleASchema.instance);
        
        int pipeLength = (2+(1<<11))*160;
        
        try {            
            
            GraphManager gm = new GraphManager();
            
            //NOTE: Since the ConsoleSummaryStage usess the HighLevel API the pipe MUST be large enough to hold and entire message
            //      Would be nice to detect this failure, not sure how.
            Pipe<SequenceExampleASchema> pipe = new Pipe<SequenceExampleASchema>(new PipeConfig<SequenceExampleASchema>(SequenceExampleASchema.instance, pipeLength));           
            
            //Constructor constructor =  LoaderUtil.generateClassConstructor(ew.getPackageName(), ew.getClassName(), target, SequenceExampleASchema.class);
            //PronghornStage stage = (PronghornStage)constructor.newInstance(gm, pipe); //32K

            PronghornStage stage = new SequenceExampleAFuzzGeneratorStageCustom(gm,pipe); //45K - remove switch and passed down pipes
            
                        
            SequenceExampleAPopulationStage dump = new SequenceExampleAPopulationStage(gm, pipe);
            GraphManager.addNota(gm, GraphManager.SCHEDULE_RATE, 1000000000, dump);
            SequenceExampleAPopulationFactory factory = dump.getFactory();
            
            
            
            
            GraphManager.enableBatching(gm);
            
            ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(gm);
           // scheduler.playNice=false;
            
            long startup = System.currentTimeMillis();
            scheduler.startup();
                
            int totalMessages = 10000;
            int i = totalMessages;
            while (--i>=0) {
                factory.nextObject();
                
                //LockSupport.parkNanos(50);
                
            }
                      
            
            
            //stage.requestShutdown();
           
            scheduler.shutdown();
            
            scheduler.awaitTermination(4, TimeUnit.SECONDS);
            long durationMS = System.currentTimeMillis()-startup;//measured for better accuracy
            
//            long totalBytes = dump.totalMessages()*estMsgSize;//totalBytes();
//            long totalMessages = dump.totalMessages();//totalBytes/estMsgSize;//this is an estimate but very close to right
//            
//            long bitsPerSecond = (8L * totalBytes ) / durationMS;
//            System.out.println("kbps: "+bitsPerSecond);
//            
            long msgPerSecond = 1000L*totalMessages/durationMS;
            System.out.println("Messages per second:"+msgPerSecond);
            
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    
}
