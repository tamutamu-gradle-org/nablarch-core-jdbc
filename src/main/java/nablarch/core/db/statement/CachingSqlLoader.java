package nablarch.core.db.statement;

import nablarch.core.cache.BasicStaticDataCache;
import nablarch.core.cache.StaticDataLoader;

import java.util.Map;

/**
 * ロードしたSQLをキャッシュするクラス。
 *
 * @author T.Kawasaki
 */
class CachingSqlLoader {

    /** SQL保持オブジェクト */
    private final BasicStaticDataCache<Map<String, String>> sqlCache = new BasicStaticDataCache<Map<String, String>>();

    /** SQLローダクラス */
    private StaticDataLoader<Map<String, String>> sqlLoader;


    /**
     * SQL文をロードするクラスを設定する。
     * @param sqlLoader SQL文をロードするクラス
     */
    public void setSqlLoader(StaticDataLoader<Map<String, String>> sqlLoader) {
        this.sqlLoader = sqlLoader;
        sqlCache.setLoader(sqlLoader);
        // 初期化時のロードは行わない。
        sqlCache.setLoadOnStartup(false);
        sqlCache.initialize();
    }

    /**
     * 指定されたSQLリソースに対応するSQL文を取得する。
     *
     * @param sqlResource SQLリソース(SQLリソース名 + "#" + SQL_ID)
     * @return SQL文
     */
    public String getSql(String sqlResource) {
        if (sqlLoader == null) {
            throw new IllegalStateException("SqlLoader was not specified.");
        }
        int index = sqlResource.indexOf("#");
        if (index != -1) {
            String id = sqlResource.substring(index + 1);
            String resource = sqlResource.substring(0, index);
            Map<String, String> value = sqlCache.getValue(resource);
            String sql = value.get(id);
            if (sql == null) {
                throw new IllegalArgumentException(
                        "sql is not found. sql resource = [" + sqlResource + "]");
            }
            return sql;
        } else {
            throw new IllegalArgumentException(String.format(
                    "sql resource format'%s' is invalid. valid format is 'sql resource + # + SQL_ID'.",
                    sqlResource));
        }
    }
}
