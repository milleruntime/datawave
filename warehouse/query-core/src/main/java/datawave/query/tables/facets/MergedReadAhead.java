package datawave.query.tables.facets;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections4.QueueUtils;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

/**
 * 
 */
public class MergedReadAhead<T> implements Iterator<T>, Closeable, Runnable {
    
    private static final Logger log = Logger.getLogger(MergedReadAhead.class);
    
    private Iterator<T> iter;
    
    protected Queue buf = null;
    
    protected FacetedConfiguration facetedConfig;
    
    private AtomicBoolean removeEntry = new AtomicBoolean(false);
    private AtomicBoolean isRunning = new AtomicBoolean();
    private AtomicBoolean started = new AtomicBoolean();
    
    public MergedReadAhead(FacetedConfiguration facetedConfig, final Iterator<T> iter, Function<T,T> functionalMerge, List<Predicate<T>> filters) {
        
        this.facetedConfig = facetedConfig;
        
        this.iter = Iterators.transform(iter, functionalMerge);
        
        for (Predicate<T> predicate : filters) {
            this.iter = Iterators.filter(this.iter, predicate);
        }
        
        buf = QueueUtils.synchronizedQueue(new CircularFifoQueue(1));
        
        start();
    }
    
    /**
     * Start the session
     */
    public void start() {
        started.set(true);
        isRunning.getAndSet(true);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        if (!facetedConfig.isStreaming) {
            while (isRunning.get())
                ;
            
        }
        while (buf.isEmpty() && isRunning.get()) {
            
        }
        
        return !buf.isEmpty();
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#next()
     */
    @Override
    public T next() {
        T val = (T) buf.peek();
        if (removeEntry.get() == true)
            buf.remove();
        return val;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    public void finalize() throws Throwable {
        try {
            close();
        } finally {
            super.finalize();
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
        isRunning.set(false);
        
        if (log.isTraceEnabled()) {
            log.trace("Closing thread");
        }
        
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        while (iter.hasNext()) {
            T d = iter.next();
            if (null != d)
                buf.add(d);
            else if (log.isTraceEnabled())
                log.trace("Data was empty");
        }
        removeEntry.set(true);
    }
    
}
