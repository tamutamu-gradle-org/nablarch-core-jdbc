package nablarch.core.cache.expirable;

import java.util.Date;

import nablarch.core.cache.expirable.ExpirableCacheListener.NopExpirableCacheListener;

import org.junit.Test;

/**
 * {@link NopExpirableCacheListener}のテストクラス。
 *
 * @author T.Kawasaki
 */
public class NopExpirableCacheListenerTest {

    /** コールバックメソッド起動時、何もおこらないこと。*/
    @Test
    public void testCallback() {
        NopExpirableCacheListener<String> target = new NopExpirableCacheListener<String>();
        final String key = "key";
        final Date expiredDate = new Date();
        final Date now = new Date();

        target.onCacheHit(key, now);
        target.onCacheAdded(key, expiredDate);
        target.onCacheNotHit(key);
        target.onExpire(key, now, expiredDate);
        target.onRemove(key);
        target.onClear();
    }


}
