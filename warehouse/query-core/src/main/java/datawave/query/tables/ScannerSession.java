package datawave.query.tables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import datawave.query.tables.AccumuloResource.ResourceFactory;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.query.tables.stats.StatsListener;
import datawave.query.tables.stats.ScanSessionStats.TIMERS;
import datawave.webservice.query.Query;

import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * This will handles running a scan against a set of ranges. The actual scan is performed in a separate thread which places the results in a result queue. The
 * result queue is polled in the actual next() and hasNext() calls. Note that the uncaughtExceptionHandler from the Query is used to pass exceptions up which
 * will also fail the overall query if something happens. If this is not desired then a local handler should be set.
 */
public class ScannerSession implements Iterator<Entry<Key,Value>>, Runnable {
    
    /**
     * last seen key, used for moving across the sliding window of ranges.
     */
    protected Key lastSeenKey;
    
    /**
     * Stack of ranges for us to progress through within this scanner queue.
     */
    protected ConcurrentLinkedQueue<Range> ranges;
    
    /**
     * Result queue, providing us objects
     */
    protected ArrayBlockingQueue<Entry<Key,Value>> resultQueue;
    
    /**
     * Current entry to return. this will be popped from the result queue.
     */
    protected Entry<Key,Value> currentEntry;
    
    /**
     * Delegates scanners to us, blocking if none are available or used by other sources.
     */
    protected ResourceQueue sessionDelegator;
    
    /**
     * Last range in our sorted list of ranges.
     */
    protected Range lastRange;
    
    /**
     * Current range that we are using.
     */
    protected Range currentRange;
    
    protected volatile boolean forceClose = false;
    
    /**
     * 
     * 
     * Scanner specific configuration items.
     * 
     * 
     */
    
    /**
     * Table to which this scanner will connect.
     */
    protected String tableName;
    
    /**
     * Authorization set
     */
    protected Set<Authorizations> auths;
    
    /**
     * Max results to return at any given time.
     */
    protected int maxResults;
    
    protected Class<? extends AccumuloResource> delegatedResourceInitializer;
    
    /**
     * Scanner options.
     */
    protected SessionOptions options = null;
    
    protected Query settings;
    
    private static final Logger log = Logger.getLogger(ScannerSession.class);
    
    protected ScanSessionStats stats = null;
    
    protected ExecutorService statsListener = null;
    
    protected boolean accrueStats;
    
    protected AccumuloResource delegatedResource = null;
    
    protected boolean isFair = true;
    
    protected QueryUncaughtExceptionHandler uncaughtExceptionHandler = null;
    
    protected AtomicBoolean started = new AtomicBoolean();
    protected AtomicBoolean running = new AtomicBoolean();
    private List<ServiceListener> listeners = Collections.emptyList();
    
    /**
     * Constructor
     * 
     * @param tableName
     *            incoming table name
     * @param auths
     *            set of authorizations.
     * @param delegator
     *            scanner queue
     * @param maxResults
     */
    public ScannerSession(String tableName, Set<Authorizations> auths, ResourceQueue delegator, int maxResults, Query settings) {
        this(tableName, auths, delegator, maxResults, settings, new SessionOptions(), null);
    }
    
    public ScannerSession(String tableName, Set<Authorizations> auths, ResourceQueue delegator, int maxResults, Query settings, SessionOptions options,
                    Collection<Range> ranges) {
        
        Preconditions.checkNotNull(options);
        Preconditions.checkNotNull(delegator);
        
        this.options = options;
        
        // build a stack of ranges
        this.ranges = new ConcurrentLinkedQueue<>();
        
        this.tableName = tableName;
        this.auths = auths;
        
        if (null != ranges && !ranges.isEmpty()) {
            List<Range> rangeList = Lists.newArrayList(ranges);
            Collections.sort(rangeList);
            
            this.ranges.addAll(ranges);
            lastRange = Iterables.getLast(rangeList);
            
        }
        
        resultQueue = Queues.newArrayBlockingQueue(maxResults);
        
        sessionDelegator = delegator;
        
        currentEntry = null;
        
        this.maxResults = maxResults;
        
        this.settings = settings;
        
        if (this.settings != null) {
            this.uncaughtExceptionHandler = this.settings.getUncaughtExceptionHandler();
        }
        
        // ensure we have an exception handler
        if (this.uncaughtExceptionHandler == null) {
            this.uncaughtExceptionHandler = new QueryUncaughtExceptionHandler();
        }
        
        delegatedResourceInitializer = RunningResource.class;
        
    }
    
    /**
     * overridden in order to set the UncaughtExceptionHandler on the Thread that is created to run the ScannerSession
     */
    // TODO set the thread name
    protected Executor executor() {
        return command -> {
            String name = this.getClass().getSimpleName();
            Preconditions.checkNotNull(name);
            Preconditions.checkNotNull(command);
            Thread result = MoreExecutors.platformThreadFactory().newThread(command);
            try {
                result.setName(name);
                result.setUncaughtExceptionHandler(uncaughtExceptionHandler);
            } catch (SecurityException e) {
                // OK if we can't set the name in this environment.
            }
            result.start();
        };
    }
    
    /**
     * We shouldn't need to synchronize unless we care if the value changes
     */
    public boolean isRunning() {
        return running.get();
    }
    
    /**
     * Sets the ranges for the given scannersession.
     * 
     * @param ranges
     * @return
     */
    public ScannerSession setRanges(Collection<Range> ranges) {
        Preconditions.checkNotNull(ranges);
        // ensure that we are not already running
        Preconditions.checkArgument(!isRunning());
        List<Range> rangeList = Lists.newArrayList(ranges);
        Collections.sort(rangeList);
        this.ranges.clear();
        this.ranges.addAll(rangeList);
        lastRange = Iterables.getLast(rangeList);
        return this;
        
    }
    
    /**
     * Sets the ranges for the given scannersession.
     * 
     * @param ranges
     * @return
     */
    public ScannerSession setRanges(Iterable<Range> ranges) {
        Preconditions.checkNotNull(ranges);
        // ensure that we are not already running
        Preconditions.checkArgument(!isRunning());
        List<Range> rangeList = Lists.newArrayList(ranges);
        Collections.sort(rangeList);
        this.ranges.clear();
        this.ranges.addAll(rangeList);
        lastRange = Iterables.getLast(rangeList);
        return this;
        
    }
    
    // public ScannerSession fetchColumnFamily(Text familY)
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ScannerSession) {
            EqualsBuilder builder = new EqualsBuilder();
            builder.append(ranges, ((ScannerSession) obj).ranges);
            builder.append(tableName, ((ScannerSession) obj).tableName);
            builder.append(auths, ((ScannerSession) obj).auths);
            return builder.isEquals();
        }
        
        return false;
    }
    
    /**
     * Start the session
     */
    public synchronized void start() {
        if (!listeners.isEmpty()) {
            listeners.forEach(ServiceListener::starting);
        }
        running.set(true);
        started.set(true);
    }
    
    /**
     * Stop the session
     */
    public synchronized void stop() {
        if (!listeners.isEmpty()) {
            listeners.forEach(ServiceListener::stopping);
        }
        running.set(false);
    }
    
    public void addListener(ServiceListener listener) {
        if (listeners.isEmpty()) {
            listeners = new ArrayList<>();
        }
        this.listeners.add(listener);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#hasNext()
     * 
     * Note that this method needs to check the uncaught exception handler and propogate any set throwables.
     */
    @Override
    public boolean hasNext() {
        
        /**
         * Let's take a moment to look through all states S
         */
        
        // if we are new, let's start and wait
        if (!started.get()) {
            // we have just started, so let's start and wait
            // until we've completed the start process
            if (null != stats)
                initializeTimers();
            start();
        }
        
        // isFlushNeeded is only in the case of when we are finished
        boolean isFlushNeeded = false;
        log.trace("hasNext" + isRunning());
        
        try {
            
            if (null != stats)
                stats.getTimer(TIMERS.HASNEXT).resume();
            
            while (null == currentEntry && (isRunning() || !resultQueue.isEmpty() || ((isFlushNeeded = flushNeeded()) == true))) {
                
                // log.trace("hasNext" + isRunning());
                
                try {
                    /**
                     * Poll for one second. We're in a do/while loop that will break iff we are no longer running or there is a current entry available.
                     */
                    currentEntry = resultQueue.poll(getPollTime(), TimeUnit.SECONDS);
                    
                } catch (InterruptedException e) {
                    log.trace("hasNext" + isRunning() + " interrupted");
                    log.error("Interrupted before finding next", e);
                    throw new RuntimeException(e);
                }
                // if we pulled no data and we are not running, and there is no data in the queue
                // we can flush if needed and retry
                
                log.trace("hasNext" + isRunning() + " " + flushNeeded());
                if (currentEntry == null && (!isRunning() && resultQueue.isEmpty()))
                    isFlushNeeded = flushNeeded();
            }
        } finally {
            if (null != stats) {
                try {
                    stats.getTimer(TIMERS.HASNEXT).suspend();
                } catch (Exception e) {
                    log.error("Failed to suspend timer", e);
                }
            }
            if (uncaughtExceptionHandler.getThrowable() != null) {
                log.error("Exception discovered on hasNext call", uncaughtExceptionHandler.getThrowable());
                throw new RuntimeException(uncaughtExceptionHandler.getThrowable());
            }
        }
        
        return (null != currentEntry);
    }
    
    protected long getPollTime() {
        return 1;
    }
    
    /**
     * Place all timers in a suspended state.
     */
    protected void initializeTimers() {
        stats.getTimer(TIMERS.HASNEXT).start();
        stats.getTimer(TIMERS.HASNEXT).suspend();
        
        stats.getTimer(TIMERS.SCANNER_ITERATE).start();
        stats.getTimer(TIMERS.SCANNER_ITERATE).suspend();
        
        stats.getTimer(TIMERS.SCANNER_START).start();
        stats.getTimer(TIMERS.SCANNER_START).suspend();
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#next()
     * 
     * Note that this method needs to check the uncaught exception handler and propogate any set throwables.
     */
    @Override
    public Entry<Key,Value> next() {
        try {
            Entry<Key,Value> retVal = currentEntry;
            currentEntry = null;
            return retVal;
        } finally {
            if (uncaughtExceptionHandler.getThrowable() != null) {
                log.error("Exception discovered on next call", uncaughtExceptionHandler.getThrowable());
                throw new RuntimeException(uncaughtExceptionHandler.getThrowable());
            }
        }
    }
    
    /**
     * Override this for your specific implementation.
     * 
     * @param lastKey
     * @param previousRange
     */
    public Range buildNextRange(final Key lastKey, final Range previousRange) {
        return new Range(lastKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME), true, previousRange.getEndKey(), previousRange.isEndKeyInclusive());
    }
    
    /**
     * set the resource class.
     * 
     * @param clazz
     */
    public void setResourceClass(Class<? extends AccumuloResource> clazz) {
        delegatedResourceInitializer = clazz;
    }
    
    /**
     * FindTop -- Follows the logic outlined in the comments, below. Effectively, we continue
     * 
     * @throws Exception
     * 
     */
    protected void findTop() throws Exception {
        if (ranges.isEmpty() && lastSeenKey == null) {
            
            if (flushNeeded()) {
                flush();
                return;
            }
            stop();
            
            return;
        }
        
        try {
            
            if (resultQueue.remainingCapacity() == 0) {
                return;
            }
            
            /**
             * Even though we were delegated a resource, we have not actually been provided the plumbing to run it. Note, below, that we initialize the resource
             * through the resource factory from a running resource.
             */
            if (null != stats)
                stats.getTimer(TIMERS.SCANNER_START).resume();
            
            delegatedResource = sessionDelegator.getScannerResource();
            
            // if we have just started or we are at the end of the current range. pop the next range
            if (lastSeenKey == null || (currentRange != null && currentRange.getEndKey() != null && lastSeenKey.compareTo(currentRange.getEndKey()) >= 0)) {
                currentRange = ranges.poll();
                // short circuit and exit
                if (null == currentRange) {
                    lastSeenKey = null;
                    return;
                }
            } else {
                // adjust the end key range.
                currentRange = buildNextRange(lastSeenKey, currentRange);
                
                if (log.isTraceEnabled())
                    log.trace("Building " + currentRange + " from " + lastSeenKey);
            }
            
            if (log.isTraceEnabled()) {
                log.trace(lastSeenKey + ", using current range of " + lastRange);
                log.trace(lastSeenKey + ", using current range of " + currentRange);
            }
            
            delegatedResource = ResourceFactory.initializeResource(delegatedResourceInitializer, delegatedResource, tableName, auths, currentRange).setOptions(
                            options);
            
            Iterator<Entry<Key,Value>> iter = delegatedResource.iterator();
            
            // do not continue if we've reached the end of the corpus
            
            if (!iter.hasNext()) {
                if (log.isTraceEnabled())
                    log.trace("We've started, but we have nothing to do on " + tableName + " " + auths + " " + currentRange);
                if (log.isTraceEnabled())
                    log.trace("We've started, but we have nothing to do");
                lastSeenKey = null;
                return;
            }
            
            int retrievalCount = 0;
            try {
                if (null != stats)
                    stats.getTimer(TIMERS.SCANNER_ITERATE).resume();
                retrievalCount = scannerInvariant(iter);
            } finally {
                if (null != stats) {
                    stats.incrementKeysSeen(retrievalCount);
                    stats.getTimer(TIMERS.SCANNER_ITERATE).suspend();
                }
                
            }
            
        } catch (IllegalArgumentException e) {
            /**
             * If we get an illegal argument exception, we know that the ScannerSession extending class created a start key after our end key, which means that
             * we've finished with this range. As a result, we set lastSeenKey to null, so that on our next pass through, we pop the next range from the queue
             * and continue or finish. We're going to timeslice and come back as know this range is likely finished.
             */
            if (log.isTraceEnabled())
                log.trace(lastSeenKey + " is lastseenKey, previous range is " + currentRange, e);
            
            lastSeenKey = null;
            return;
            
        } catch (Exception e) {
            if (forceClose) {
                // if we force close, then we can ignore the exception
                if (log.isTraceEnabled()) {
                    log.trace("Ignoring exception because we have been closed", e);
                }
            } else {
                log.error("Failed to find top", e);
                throw e;
            }
            
        } finally {
            
            if (null != stats)
                stats.getTimer(TIMERS.SCANNER_START).suspend();
            
            synchronized (sessionDelegator) {
                if (null != delegatedResource) {
                    sessionDelegator.close(delegatedResource);
                    delegatedResource = null;
                }
            }
            
        }
    }
    
    protected int scannerInvariant(final Iterator<Entry<Key,Value>> iter) {
        int retrievalCount = 0;
        
        Entry<Key,Value> myEntry = null;
        
        while (iter.hasNext()) {
            myEntry = iter.next();
            
            try {
                if (!resultQueue.offer(myEntry, 200, TimeUnit.MILLISECONDS))
                    break;
            } catch (InterruptedException exception) {
                break;
            }
            
            lastSeenKey = myEntry.getKey();
            // do not continue if we have reached the capacity of the queue
            // or we are 1.5x the maxResults ( to ensure fairness to other threads
            if (resultQueue.remainingCapacity() == 0 || (isFair && retrievalCount >= Math.ceil(maxResults * 1.5))) {
                if (log.isTraceEnabled())
                    log.trace("stopping because we're full after adding " + resultQueue.remainingCapacity() + " " + retrievalCount + " " + maxResults);
                break;
            }
            retrievalCount++;
        }
        
        return retrievalCount;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        // do nothing.
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.google.common.util.concurrent.AbstractExecutionThreadService#run()
     * 
     * Note that this method must set exceptions on the uncaughtExceptionHandler, otherwise any failures will be completed ignored/dropped.
     */
    @Override
    public void run() {
        try {
            while (isRunning()) {
                
                findTop();
            }
            
            flush();
        } catch (Exception e) {
            uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), e);
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Set the scanner options
     * 
     * @param options
     */
    public ScannerSession setOptions(SessionOptions options) {
        Preconditions.checkNotNull(options);
        this.options = options;
        return this;
        
    }
    
    /**
     * Return scanner options.
     * 
     * @return
     */
    public SessionOptions getOptions() {
        return this.options;
    }
    
    /**
     * Methods, below, are solely for testing.
     */
    
    /**
     * Test method.
     * 
     * @throws InterruptedException
     */
    protected void waitUntilCapacity() throws InterruptedException {
        while (resultQueue.remainingCapacity() > 0) {
            Thread.sleep(500);
        }
    }
    
    /**
     * Returns the current range object for testing.
     * 
     * @return
     */
    protected Range getCurrentRange() {
        return currentRange;
    }
    
    protected void flush() {
        
    }
    
    protected boolean flushNeeded() {
        return false;
    }
    
    /**
     * Get last Range.
     * 
     * @return
     */
    protected Range getLastRange() {
        return lastRange;
    }
    
    /**
     * Get last key.
     * 
     * @return
     */
    protected Key getLastKey() {
        return lastSeenKey;
    }
    
    public ScanSessionStats getStatistics() {
        return stats;
    }
    
    public void setDelegatedInitializer(Class<? extends AccumuloResource> delegatedResourceInitializer) {
        this.delegatedResourceInitializer = delegatedResourceInitializer;
    }
    
    public ScannerSession applyStats(ScanSessionStats stats) {
        if (null != stats) {
            Preconditions.checkArgument(this.stats == null);
            this.stats = stats;
            statsListener = Executors.newFixedThreadPool(1);
            addListener(new StatsListener(stats, statsListener));
        }
        
        return this;
    }
    
    public void close() {
        forceClose = true;
        stop();
        synchronized (sessionDelegator) {
            if (null != delegatedResource) {
                try {
                    sessionDelegator.close(delegatedResource);
                    delegatedResource = null;
                } catch (Exception e) {
                    log.error("Failed to close session", e);
                }
            }
        }
    }
    
    public void setFairness(boolean fairness) {
        isFair = fairness;
        
    }
    
    public void setMaxResults(int maxResults) {
        this.maxResults = maxResults;
        
    }
    
}
