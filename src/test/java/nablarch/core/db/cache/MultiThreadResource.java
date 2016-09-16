package nablarch.core.db.cache;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.rules.ExternalResource;

/**
 * スレッドプールを提供するクラス。
 *
 * @author T.Kawasaki
 */
public class MultiThreadResource extends ExternalResource {

    public ExecutorService service;

    public final int numberOfThread;

    public MultiThreadResource(int numberOfThread) {
        this.numberOfThread = numberOfThread;
        this.service = Executors.newFixedThreadPool(numberOfThread);
    }

    public void execute(int count, Runnable runnable) {
        for (int i = 0; i < count; i++) {
            service.execute(runnable);
        }
    }

    public void terminateAndWait(int waitSec) {
        shutdown(waitSec);
    }


    @Override
    protected void after() {
        terminateAndWait(10);
    }

    private void shutdown(int waitSec) {
        if (service == null || service.isTerminated()) {
            return;
        }
        if (!service.isShutdown()) {
            service.shutdown();
        }

        try {
            boolean success = service.awaitTermination(waitSec, TimeUnit.SECONDS);
            if (success) {
                return;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        service.shutdownNow();
    }
}
