package com.example.ehpoc;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/** Background work in this app runs on named, single-threaded daemon schedulers, so none can hold the JVM open. */
final class DaemonScheduler {

    private DaemonScheduler() {
    }

    static ScheduledExecutorService singleThread(String threadName) {
        return Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, threadName);
            thread.setDaemon(true);
            return thread;
        });
    }
}
