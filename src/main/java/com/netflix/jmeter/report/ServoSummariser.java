package com.netflix.jmeter.report;

import java.io.File;
import java.util.concurrent.TimeUnit;

import com.netflix.servo.monitor.Monitors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.publish.BasicMetricFilter;
import com.netflix.servo.publish.CounterToRateMetricTransform;
import com.netflix.servo.publish.FileMetricObserver;
import com.netflix.servo.publish.MetricObserver;
import com.netflix.servo.publish.MonitorRegistryMetricPoller;
import com.netflix.servo.publish.PollRunnable;
import com.netflix.servo.publish.PollScheduler;

public class ServoSummariser extends AbstractSummariser
{
    private static final long serialVersionUID = 6638743483539533164L;
    private static final Logger logger = LoggerFactory.getLogger(ServoSummariser.class);
    private static boolean initalized = false;

    @Override
    protected void initializePlatform()
    {
        if (initalized)
            return;

        try
        {
            PollScheduler scheduler = PollScheduler.getInstance();
            scheduler.start();
            MetricObserver fileObserver = new FileMetricObserver("stats", new File("."));
            MetricObserver transform = new CounterToRateMetricTransform(fileObserver, 2, TimeUnit.MINUTES);
            PollRunnable task = new PollRunnable(new MonitorRegistryMetricPoller(), BasicMetricFilter.MATCH_ALL, transform);
            scheduler.addPoller(task, 1, TimeUnit.MINUTES);
        }
        catch (Throwable e)
        {
            // dont do anything... just eat.
            logger.error("Epic Plugin was not intialized: ", e);
        }
        initalized = true;
    }

    @Override
    protected AbstractRunningSampleWrapper newRunningSampleWrapper(String label)
    {
        return new ServoRunningSampleWrapper(label);
    }

    public static class ServoRunningSampleWrapper extends AbstractRunningSampleWrapper
    {
        public final String name;

        public ServoRunningSampleWrapper(String name)
        {
            super(name);
            this.name = ("JMeter_" + name).replace(" ", "_");
        }

        @Monitor(name = "ErrorPercentage", type = DataSourceType.GAUGE)
        public double getErrorPercentage()
        {
            return previous.getErrorPercentage();
        }

        @Monitor(name = "SampleCount", type = DataSourceType.GAUGE)
        public int getCount()
        {
            return previous.getCount();
        }

        @Monitor(name = "Rate", type = DataSourceType.GAUGE)
        public double getRate()
        {
            return previous.getRate();
        }

        @Monitor(name = "Mean", type = DataSourceType.GAUGE)
        public double getMean()
        {
            return previous.getMean();
        }

        @Monitor(name = "Min", type = DataSourceType.GAUGE)
        public long getMin()
        {
            return previous.getMin();
        }

        @Monitor(name = "Max", type = DataSourceType.GAUGE)
        public long getMax()
        {
            return previous.getMax();
        }

        @Monitor(name = "TotalBytes", type = DataSourceType.GAUGE)
        public long getTotalBytes()
        {
            return previous.getTotalBytes();
        }

        @Monitor(name = "StandardDeviation", type = DataSourceType.GAUGE)
        public double getStandardDeviation()
        {
            return previous.getStandardDeviation();
        }

        @Monitor(name = "AvgPageBytes", type = DataSourceType.GAUGE)
        public double getAvgPageBytes()
        {
            return previous.getAvgPageBytes();
        }

        @Monitor(name = "BytesPerSecond", type = DataSourceType.GAUGE)
        public double getBytesPerSecond()
        {
            return previous.getBytesPerSecond();
        }

        @Monitor(name = "KBPerSecond", type = DataSourceType.GAUGE)
        public double getKBPerSecond()
        {
            return previous.getKBPerSecond();
        }

        @Override
        public void start()
        {
            Monitors.registerObject(name, this);
        }

        @Override
        public void shutdown()
        {
            Monitors.unregisterObject(name, this);
        }
    }
}
