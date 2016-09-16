package nablarch.core.db.cache;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import nablarch.core.cache.expirable.Expirable;
import nablarch.core.cache.expirable.ExpirableCacheListener;
import nablarch.core.cache.expirable.InMemoryExpirableCache;
import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;
import nablarch.core.util.Builder;
import nablarch.core.util.map.LRUMap;
import nablarch.core.util.map.LRUMap.RemoveListener;

/**
 * メモリ上にキャッシュを保持する結果セットキャッシュ実装クラス。
 *
 * @author T.Kawasaki
 */
public class InMemoryResultSetCache
        extends InMemoryExpirableCache<ResultSetCacheKey, SqlResultSet>
        implements ResultSetCache {

    /** ロガー */
    private static final Logger LOGGER = LoggerManager.get("RS_CACHE");

    /** ログ出力を行うリスナー */
    private final ResultSetCacheLoggingListener listener = new ResultSetCacheLoggingListener();

    /** デフォルトコンストラクタ。 */
    public InMemoryResultSetCache() {
        if (isLoggerEnabled()) {
            // ログレベルDEBUGが有効の場合はログ出力リスナーを使用する。
            setCacheListener(listener);
        }
        // ログが有効でない場合は設定しないのでログ出力されない。
    }

    /** {@inheritDoc} */
    @Override
    protected Map<ResultSetCacheKey, Expirable<SqlResultSet>> createCacheContainer(int max) {
        LRUMap<ResultSetCacheKey, Expirable<SqlResultSet>> lruMap;
        if (isLoggerEnabled()) {
            // ログレベルDEBUGが有効の場合はログ出力リスナーを使用する。
            lruMap = new LRUMap<ResultSetCacheKey, Expirable<SqlResultSet>>(max, listener);
        } else {
            // ログが有効でない場合は設定しないのでログ出力されない。
            lruMap = new LRUMap<ResultSetCacheKey, Expirable<SqlResultSet>>(max);
        }
        // マルチスレッドで使用できるよう同期化する。
        // LRUMapは元がLinkedHashMapなので、
        // java.util.concurrentのクラスに置き換えることは難しい。
        return Collections.synchronizedMap(lruMap);
    }

    /**
     * ログ出力可能であるか判定する。
     *
     * @return ログ出力可能である場合、真
     */
    boolean isLoggerEnabled() {
        return LOGGER.isDebugEnabled();
    }

    /**
     * 各種イベント発生時にログ出力を行うリスナー実装クラス。
     */
    private static class ResultSetCacheLoggingListener implements
            ExpirableCacheListener<ResultSetCacheKey>,
            RemoveListener<ResultSetCacheKey, Expirable<SqlResultSet>> {

        /** ログ出力時のDateのフォーマット形式 */
        private static final String DATE_FORMAT = "yyyy/MM/dd hh:mm:ss:SSS";

        /** {@inheritDoc} */
        @Override
        public void onCacheHit(ResultSetCacheKey key, Date now) {
            log("cache hit: key=[", key, "], current=[", fmt(now), "]");
        }

        /** {@inheritDoc} */
        @Override
        public void onCacheNotHit(ResultSetCacheKey key) {
            log("cache not hit: key=[", key, "]");
        }

        /** {@inheritDoc} */
        @Override
        public void onExpire(ResultSetCacheKey key, Date now, Date expiredDate) {
            log("cache entry expired: key=[", key, "],",
                "expire=[", fmt(expiredDate), "], current=[", fmt(now), "]");
        }

        /** {@inheritDoc} */
        @Override
        public void onCacheAdded(ResultSetCacheKey key, Date expiredDate) {
            log("cache entry added: key=[", key, "], expire=[", fmt(expiredDate), "]");
        }

        /** {@inheritDoc} */
        @Override
        public void onRemove(ResultSetCacheKey key) {
            log("cache entry removed: key=[", key, "]");
        }

        /** {@inheritDoc} */
        @Override
        public void onClear() {
            log("cache cleared.");
        }

        /** {@inheritDoc} */
        @Override
        public void onRemoveEldest(ResultSetCacheKey key, Expirable<SqlResultSet> value) {
            log("the eldest entry removed: key=[" + key, "]");
        }

        /**
         * ログ出力を行う。
         *
         * @param msgs メッセージ(連結される）
         */
        private void log(Object... msgs) {
            LOGGER.logDebug(Builder.concat(msgs));
        }

        /**
         * 日時をフォーマットする。
         * @param date フォーマット元のDate
         * @return フォーマットされた日時文字列
         */
        private String fmt(Date date) {
            // CHANGE: commonへの依存性を切るため、DateUtilの使用をやめる。
            SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
            return dateFormat.format(date);
        }

    }
}
