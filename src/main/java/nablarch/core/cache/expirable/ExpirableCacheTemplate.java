package nablarch.core.cache.expirable;

import java.util.Date;

import nablarch.core.date.SystemTimeProvider;
import nablarch.core.cache.expirable.ExpirableCacheListener.NopExpirableCacheListener;
import nablarch.core.util.annotation.Published;

/**
 * 有効期限付きキャッシュ({@link ExpirableCache})を実装するためのテンプレートクラス。
 * 有効期限付きキャッシュの基本的な枠組みを提供する。
 * サブクラスにて、具体的な格納先（Map、KVS等）へのアクセス処理を実装する。
 *
 * @param <K> キャッシュキー
 * @param <V> キャッシュ値
 * @author T.Kawasaki
 */
@Published(tag = "architect")
public abstract class ExpirableCacheTemplate<K, V> implements ExpirableCache<K, V> {

    /** 有効期限無しを表す日時 */
    protected static final Date UNLIMITED = new Date(Long.MAX_VALUE);

    /** システム日時提供クラス */
    private SystemTimeProvider systemTimeProvider;

    /** キャッシュリスナー */
    private ExpirableCacheListener<K> listener = new NopExpirableCacheListener<K>();

    /**
     * 現在日時を取得する。
     *
     * @return 現在日時
     */
    protected final Date getNowDate() {
        return systemTimeProvider.getDate();
    }

    /** {@inheritDoc} */
    @Override
    public V getIfNotExpired(K key) {
        checkStatus();

        Expirable<V> expirable = getFromCache(key);
        if (expirable == null) {
            listener.onCacheNotHit(key);
            return null;
        }

        Date now = getNowDate();
        if (expirable.isExpired(now)) {
            listener.onExpire(key, now, expirable.getExpiredDate());
            removeFromCache(key);
            return null;
        }

        listener.onCacheHit(key, now);
        return expirable.getContent();
    }

    /** {@inheritDoc} */
    @Override
    public void add(K key, V value, Date timeout) {
        checkStatus();
        addToCache(key, new Expirable<V>(value, timeout));
        listener.onCacheAdded(key, timeout);
    }

    /** {@inheritDoc} */
    @Override
    public void addUnlimited(K key, V value) {
        add(key, value, UNLIMITED);
    }

    /** {@inheritDoc} */
    @Override
    public void remove(K key) {
        checkStatus();
        Expirable<V> removed = removeFromCache(key);
        callListenerOnRemove(key, removed);
    }

    /** {@inheritDoc} */
    @Override
    public void clear() {
        checkStatus();
        listener.onClear();
        clearCache();
    }

    /** {@inheritDoc} */
    @Override
    public void setCacheListener(ExpirableCacheListener<K> listener) {
        this.listener = listener;
    }

    /**
     * キャッシュから値を取得する。
     *
     * @param key キー
     * @return キーに対応する値を格納した{@link Expirable}
     */
    protected abstract Expirable<V> getFromCache(K key);

    /**
     * 指定したキーに対応するエントリを削除する。
     *
     * @param key キー
     * @return キーに対応する値を格納した{@link Expirable}
     */
    protected abstract Expirable<V> removeFromCache(K key);

    /**
     * キャッシュに値を設定する。
     * @param key キー
     * @param expirable 値を格納した{@link Expirable}
     */
    protected abstract void addToCache(K key, Expirable<V> expirable);

    /**
     * キャッシュの全エントリを削除する。
     */
    protected abstract void clearCache();


    /**
     * エントリ削除時にリスナーをコールバックする。
     *
     * @param key     削除対象エントリのキー
     * @param removed 削除された値
     */
    void callListenerOnRemove(K key, Expirable<V> removed) {
        if (removed != null) {
            // removed == nullの時、スレッド競合が発生しているので、
            // 最初のスレッドのみコールバックを起動する。
            listener.onRemove(key);
        }
    }

    /**
     * インスタンスの状態をチェックする。
     * インスタンスが処理実行可能な事前条件を満たしていることを
     * 確認する。
     *
     * @throws IllegalArgumentException インスタンスの状態が不正な場合
     */
    private void checkStatus() throws IllegalArgumentException {
        if (systemTimeProvider == null) {
            throw new IllegalStateException("systemTimeProvider must be set.");
        }
    }

    /**
     * システム日時提供クラスを取得する。
     *
     * @param systemTimeProvider システム日時提供クラス
     */
    public void setSystemTimeProvider(SystemTimeProvider systemTimeProvider) {
        this.systemTimeProvider = systemTimeProvider;
    }

}
