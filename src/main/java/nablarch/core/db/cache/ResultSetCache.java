package nablarch.core.db.cache;

import nablarch.core.cache.expirable.ExpirableCache;
import nablarch.core.db.statement.SqlResultSet;

/**
 * 結果セットを格納対象とするキャッシュインタフェース。
 *
 * @author T.Kawasaki
 */
public interface ResultSetCache extends ExpirableCache<ResultSetCacheKey, SqlResultSet> {
}
