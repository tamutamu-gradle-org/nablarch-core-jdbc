package nablarch.core.cache.expirable;

import java.util.Date;

import nablarch.core.util.annotation.Published;

/**
 * 有効期限付きキャッシュ。
 *
 * @param <K> キャッシュキー
 * @param <V> キャッシュ値
 * @author T.Kawasaki
 */
public interface ExpirableCache<K, V> {

    /**
     * キャッシュから値を取得する。
     * キャッシュに値が存在しない場合または有効期限切れの場合はnullを返却する。
     *
     * @param key キャッシュキー
     * @return キャッシュされた値
     */
    V getIfNotExpired(K key);


    /**
     * キャッシュに値を設定する。
     *
     * @param key     キャッシュキー
     * @param value   キャッシュされる値
     * @param timeout 有効期限
     */
    void add(K key, V value, Date timeout);

    /**
     * キャッシュに有効期限無しで値を設定する。
     *
     * @param key   キャッシュキー
     * @param value キャッシュされる値
     */
    void addUnlimited(K key, V value);

    /**
     * キャッシュから値を削除する。
     *
     * @param key 削除対象キャッシュキー
     */
    @Published(tag = "architect")   // 明示的にキャッシュをクリアするため
    void remove(K key);

    /**
     * キャッシュをクリアする。
     */
    @Published(tag = "architect")   // テスト用
    void clear();

    /**
     * キャッシュリスナークラスを設定する。
     * 本インタフェース実装クラスは、各イベント発生時にこのリスナーをコールバックする。
     *
     * @param listener リスナークラス。
     */
    @Published(tag = "architect")   // 独自のリスナーを登録する場合
    void setCacheListener(ExpirableCacheListener<K> listener);
}
