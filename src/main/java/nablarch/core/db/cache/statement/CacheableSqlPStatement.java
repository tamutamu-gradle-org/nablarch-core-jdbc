package nablarch.core.db.cache.statement;

import java.sql.PreparedStatement;
import java.util.Date;
import java.util.List;

import nablarch.core.cache.expirable.ExpirationSetting;
import nablarch.core.db.cache.ResultSetCache;
import nablarch.core.db.cache.ResultSetCacheKey;
import nablarch.core.db.statement.BasicSqlPStatement;
import nablarch.core.db.statement.ParameterHolder;
import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.db.statement.exception.SqlStatementException;

/**
 * キャッシュ機構を備えた{@link nablarch.core.db.statement.SqlPStatement}実装クラス。
 *
 * @author T.Kawasaki
 */
public class CacheableSqlPStatement extends BasicSqlPStatement {

    /** キャッシュ */
    private ResultSetCache cache;

    /** 有効期限設定 */
    private ExpirationSetting expirationSetting;

    /** SQL ID */
    private final String sqlId;

    /**
     * コンストラクタ。
     * 本クラスではSQLIDが必須である。その他の値はスーパクラスに渡される。
     *
     * @param sql       SQL
     * @param statement ステートメント
     * @param sqlId     SQL ID
     * @see BasicSqlPStatement#BasicSqlPStatement(String, PreparedStatement)
     */
    public CacheableSqlPStatement(String sql, PreparedStatement statement, String sqlId) {
        super(sql, statement);
        this.sqlId = sqlId;
    }

    /**
     * コンストラクタ。
     * 本クラスではSQLIDが必須である。その他の値はスーパクラスに渡される。
     *
     * @param sql       SQL
     * @param statement ステートメント
     * @param nameList  名前付き変数のリスト
     * @param sqlId     SQL ID
     * @see BasicSqlPStatement#BasicSqlPStatement(String, PreparedStatement, List)
     */
    public CacheableSqlPStatement(String sql,
                                  PreparedStatement statement,
                                  List<String> nameList,
                                  String sqlId) {
        super(sql, statement, nameList);
        this.sqlId = sqlId;
    }

    /**
     * {@inheritDoc}
     * 本クラスでは、DBアクセスを行う前にキャッシュからの値取得を試行する。
     * キャッシュに値がある場合はキャッシュされた{@link SqlResultSet}が返却される。
     * キャッシュにヒットしない場合、有効期限切れの場合、DBアクセスを行い、
     * キャッシュに値を設定する。
     */
    @Override
    protected SqlResultSet doRetrieve(int startPos, int max)
            throws SqlStatementException {
        SqlResultSet result = getFromCacheOrRetrieve(startPos, max);
        return new ImmutableSqlResultSet(result);
    }

    /**
     * キャッシュから値取得を試行し、キャッシュミス時はDBアクセスを行う。
     *
     * @param startPos 開始位置
     * @param max      最大件数
     * @return 結果セット
     */
    private SqlResultSet getFromCacheOrRetrieve(int startPos, int max) {
        ResultSetCacheKey key = buildCacheKey(startPos, max);
        SqlResultSet resultSet = cache.getIfNotExpired(key);
        if (resultSet == null) {  // キャッシュミス
            // 同時実行性を優先するため、
            // ここに複数スレッドが同時に到達する可能性を許容する。
            // （その場合、同じクエリが発行される）
            resultSet = super.doRetrieve(startPos, max);
            addToCache(key, resultSet);
        }
        return resultSet;
    }

    /**
     * 以下の要素からキャッシュキーの組み立てを行う。
     * <ul>
     * <li>コンストラクタで設定されたSQL ID</li>
     * <li>これまでにステートメントに設定されたバインドパラメータ</li>
     * <li>開始位置(引数)</li>
     * <li>最大件数(引数)</li>
     * </ul>
     *
     * @param startPos 開始位置
     * @param max      最大件数
     * @return キャッシュキー
     */
    private ResultSetCacheKey buildCacheKey(int startPos, int max) {
        BoundParameters params = new BoundParameters(getParameters());
        return new ResultSetCacheKey(sqlId, params, startPos, max);
    }

    /**
     * キャッシュに値を追加する。
     *
     * @param key   キャッシュキー
     * @param value キャッシュ値
     */
    private void addToCache(ResultSetCacheKey key, SqlResultSet value) {
        Date timeout = expirationSetting.getExpiredDate(sqlId);
        cache.add(key, value, timeout);
    }

    /**
     * {@inheritDoc}
     * 本クラスでは、ログレベルに関係なく{@link ParameterHolder}を生成する。
     */
    @Override
    protected ParameterHolder createParamHolder() {
        return new ParameterHolder();
    }

    /**
     * キャッシュを設定する（必須）。
     *
     * @param cache キャッシュ
     */
    void setResultSetCache(ResultSetCache cache) {
        this.cache = cache;
    }

    /**
     * キャッシュ有効期限設定を設定する（必須）。
     *
     * @param expiration キャッシュ有効期限設定
     */
    void setCacheExpiration(ExpirationSetting expiration) {
        this.expirationSetting = expiration;
    }
}
