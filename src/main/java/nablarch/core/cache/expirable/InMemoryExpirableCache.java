package nablarch.core.cache.expirable;

import java.util.Collections;
import java.util.Map;

import nablarch.core.util.map.LRUMap;

/**
 * キャッシュをメモリ上に保持する{@link ExpirableCache}実装クラス。
 *
 * @param <K> キャッシュキーの型
 * @param <V> キャッシュ値の型
 * @author T.Kawasaki
 */
public class InMemoryExpirableCache<K, V> extends ExpirableCacheTemplate<K, V> {

    /** キャッシュの実体 */
    private Map<K, Expirable<V>> cache;

    /**
     * キャッシュ上限値を設定する。
     * ここで設定された件数を超過してキャッシュに値が設定された場合、
     * 最も参照されていないエントリが削除される。
     *
     * @param max 上限値
     */
    public void setCacheSize(int max) {
        cache = createCacheContainer(max);
    }

    /**
     * キャッシュの実体となるMapを生成する。
     * 本クラスでは{@link LRUMap}が使用される。
     * 本メソッドをオーバライドすることで使用するMap実装を変更することができる。
     * <br/>
     * 本クラスをスレッドセーフにするには、このメソッドが返却するMapインスタンスを
     * スレッドセーフとしなければならない。
     *
     * @param max 最大上限件数
     * @return キャッシュの実体となるMap
     */
    protected Map<K, Expirable<V>> createCacheContainer(int max) {
        // sizeのチェックはLRUMapのコンストラクタに委譲
        LRUMap<K, Expirable<V>> map = new LRUMap<K, Expirable<V>>(max);
        return Collections.synchronizedMap(map);
    }

    /** {@inheritDoc} */
    @Override
    protected Expirable<V> getFromCache(K key) {
        checkStatus();
        return cache.get(key);
    }

    /** {@inheritDoc} */
    @Override
    protected Expirable<V> removeFromCache(K key) {
        checkStatus();
        return cache.remove(key);
    }

    /** {@inheritDoc} */
    @Override
    protected void addToCache(K key, Expirable<V> expirable) {
        checkStatus();
        cache.put(key, expirable);
    }

    /** {@inheritDoc} */
    @Override
    protected void clearCache() {
        checkStatus();
        cache.clear();
    }

    /**
     * インスタンスの状態をチェックする。
     * インスタンスが処理実行可能な事前条件を満たしていることを
     * 確認する。
     *
     * @throws IllegalArgumentException インスタンスの状態が不正な場合
     */
    private void checkStatus() throws IllegalArgumentException {
        if (cache == null) {
            throw new IllegalStateException("cacheSize must be set.");
        }
    }

}
