package com.netflix.jmeter.report;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jmeter.engine.event.LoopIterationEvent;
import org.apache.jmeter.engine.util.NoThreadClone;
import org.apache.jmeter.samplers.Remoteable;
import org.apache.jmeter.samplers.SampleEvent;
import org.apache.jmeter.samplers.SampleListener;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestListener;
import org.apache.jmeter.util.Calculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSummariser extends AbstractTestElement implements Serializable, SampleListener, TestListener, NoThreadClone, Remoteable
{
    private static final long serialVersionUID = 3089085300897902045L;
    private static final Logger logger = LoggerFactory.getLogger(AbstractSummariser.class);
    private static final ConcurrentHashMap<String, AbstractRunningSampleWrapper> allTests = new ConcurrentHashMap<String, AbstractRunningSampleWrapper>();
    private static final long INTERVAL = 60 * 1000; // Every Minute

    public AbstractSummariser()
    {
        super();
        initializePlatform();
    }
    
    protected abstract void initializePlatform();
    
    protected abstract AbstractRunningSampleWrapper newRunningSampleWrapper(String label);
    
    public static abstract class AbstractRunningSampleWrapper
    {
        protected volatile Calculator delta;
        protected volatile Calculator previous;
        protected volatile long totalUpdated = 0;
        private String name;

        public AbstractRunningSampleWrapper(String name)
        {
            this.name = name;
            this.delta = new Calculator(name);
            logHeader();
        }

        public void moveDelta()
        {
            previous = delta;
            delta = new Calculator(name);
        }

        public abstract void start();

        public abstract void shutdown();

        public void logHeader()
        {
            StringBuffer buff = new StringBuffer();
            buff.append("Name").append(", ");
            buff.append("Count").append(", ");
            buff.append("Rate").append(", ");
            buff.append("Min").append(", ");
            buff.append("Max").append(", ");
            buff.append("Mean").append(", ");
            buff.append("TotalBytes").append(", ");
            buff.append("StandardDeviation").append(", ");
            buff.append("ErrorPercentage").append(", ");
            buff.append("AvgPageBytes").append(", ");
            buff.append("BytesPerSecond").append(", ");
            buff.append("KBPerSecond").append(", ");
            logger.info(buff.toString());
        }

        public void log()
        {
            StringBuffer buff = new StringBuffer();
            buff.append(name).append(", ");
            buff.append(previous.getCount()).append(", ");
            buff.append(previous.getRate()).append(", ");
            buff.append(previous.getMin()).append(", ");
            buff.append(previous.getMax()).append(", ");
            buff.append(previous.getMean()).append(", ");
            buff.append(previous.getTotalBytes()).append(", ");
            buff.append(previous.getStandardDeviation()).append(", ");
            buff.append(previous.getErrorPercentage()).append(", ");
            buff.append(previous.getAvgPageBytes()).append(", ");
            buff.append(previous.getBytesPerSecond()).append(", ");
            buff.append(previous.getKBPerSecond()).append(", ");
            logger.info(buff.toString());
        }
    }

    public void sampleOccurred(SampleEvent e)
    {
        if (e.getResult() == null || e.getResult() == null)
            return;

        SampleResult s = e.getResult();
        long now = System.currentTimeMillis();// in seconds
        AbstractRunningSampleWrapper totals;
        synchronized (allTests)
        {
            String label = s.getSampleLabel();
            if ((totals = allTests.get(label)) == null)
            {
                totals = newRunningSampleWrapper(label);
                totals.start();
                allTests.put(label, totals);
            }
        }

        synchronized (totals)
        {
            totals.delta.addSample(s);
            if ((now > totals.totalUpdated + INTERVAL))
            {
                totals.moveDelta();
                totals.totalUpdated = now;
                totals.log();
            }
        }
    }

    @Override
    public void sampleStarted(SampleEvent e)
    {
    }

    @Override
    public void sampleStopped(SampleEvent e)
    {
    }

    @Override
    public void testStarted()
    {
        testStarted("local");
    }

    @Override
    public void testEnded()
    {
        testEnded("local");
    }

    @Override
    public void testStarted(String host)
    {
        allTests.clear();
    }

    public void testEnded(String host)
    {
        for (AbstractRunningSampleWrapper wrapper : allTests.values())
        {
            wrapper.log();
            wrapper.shutdown();
        }
        allTests.clear();
    }

    public void testIterationStart(LoopIterationEvent event)
    {
    }
}