package com.ociweb.protocoltest.data;

public abstract class SequenceExampleAFactory implements Runnable {

    public abstract void startup();
    public abstract SequenceExampleA nextObject();
    
}
