package com.edward.flink.datastream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class CustomDataSource implements ParallelSourceFunction<Event>{

    private volatile boolean isRunning = true;



    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {

        String[] users = {"Bob","Lily","Tom"};
        String[] urls = {"./home","./fav", "./prod?id=10"};
        Random random = new Random();

        while(isRunning)
        {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timestamp = Calendar.getInstance().getTimeInMillis();
            sourceContext.collect(new Event(user, url, timestamp));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
