package com.exaple;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class FilterProcess implements Processor<String, String> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(String key, String value) {
        if(value.length() > 5){
            context.forward(key, value);
        }
        context.commit();
    }

    @Override
    public void close() {

    }
}
