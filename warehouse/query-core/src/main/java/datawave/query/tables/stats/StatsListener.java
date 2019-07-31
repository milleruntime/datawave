package datawave.query.tables.stats;

import java.util.concurrent.ExecutorService;

import datawave.query.tables.ServiceListener;
import datawave.query.tables.stats.ScanSessionStats.TIMERS;

public class StatsListener implements ServiceListener {
    
    protected ScanSessionStats stats;
    private ExecutorService statsListener;
    
    public StatsListener(ScanSessionStats stats, ExecutorService statsListener) {
        this.stats = stats;
        this.statsListener = statsListener;
    }
    
    @Override
    public void starting() {
        if (null != stats) {
            stats.getTimer(TIMERS.RUNTIME).start();
        }
    }
    
    @Override
    public void stopping() {
        stats.getTimer(TIMERS.RUNTIME).stop();
        statsListener.shutdownNow();
    }
    
    @Override
    public void failed(String from, Throwable failure) {
        stats.stopOnFailure();
        statsListener.shutdownNow();
    }
}
