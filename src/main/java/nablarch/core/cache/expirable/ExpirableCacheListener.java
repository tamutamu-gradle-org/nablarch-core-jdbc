package nablarch.core.cache.expirable;

import java.util.Date;

import nablarch.core.util.annotation.Published;

/**
 * 有効期限付きキャッシュのリスナーインタフェース。
 * 本インタフェースの各メソッドは、{@link ExpirableCache}実装クラスからコールバックされる。
 *
 * @param <K> キャッシュキーの型
 * @author T.Kawasaki
 */
@Published(tag = "architect")
public interface ExpirableCacheListener<K> {

    /**
     * キャッシュに値取得の要求が来て、
     * キャッシュにヒットした場合のコールバックメソッド。
     * @param key キャッシュキー
     * @param now 現在日時
     */
    void onCacheHit(K key, Date now);

    /**
     * キャッシュに値取得の要求が来たが、
     * キャッシュに値が存在しなかった場合のコールバックメソッド。
     *
     * @param key キャッシュキー
     */
    void onCacheNotHit(K key);

    /**
     * キャッシュ有効期限切れが検知された場合のコールバックメソッド。
     *
     * @param key キャッシュキー
     * @param now 現在日時
     * @param expiredDate 有効期限
     */
    void onExpire(K key, Date now, Date expiredDate);

    /**
     * キャッシュに値が設定された場合のコールバックメソッド。
     *
     * @param key キャッシュキー
     * @param expiredDate 有効期限
     */
    void onCacheAdded(K key, Date expiredDate);

    /**
     * キャッシュに対して削除要求が発生した場合のコールバックメソッド
     * @param key キャッシュキー
     */
    void onRemove(K key);

    /** キャッシュクリア要求が発生した場合のコールバックメソッド。*/
    void onClear();

    /**
     * 何の処理も行わないリスナー実装クラス。
     * 特にコールバック処理が必要ない場合、本クラスを使用する。
     *
     *
     * @param <K> キャッシュキーの型
     */
    public static class NopExpirableCacheListener<K> implements ExpirableCacheListener<K> {

        /** {@inheritDoc} */
        @Override
        public void onCacheHit(K key, Date now) {
        }

        /** {@inheritDoc} */
        @Override
        public void onCacheNotHit(K key) {
        }

        /** {@inheritDoc} */
        @Override
        public void onExpire(K key, Date now, Date expiredDate) {
        }

        /** {@inheritDoc} */
        @Override
        public void onCacheAdded(K key, Date expiredDate) {
        }

        /** {@inheritDoc} */
        @Override
        public void onRemove(K key) {
        }

        /** {@inheritDoc} */
        @Override
        public void onClear() {
        }


    }

}
