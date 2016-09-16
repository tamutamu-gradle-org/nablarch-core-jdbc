package nablarch.core.db.cache.statement;

import java.sql.Connection;
import java.sql.SQLException;

import nablarch.core.db.DbExecutionContext;
import nablarch.core.db.cache.ResultSetCache;
import nablarch.core.cache.expirable.ExpirationSetting;
import nablarch.core.db.statement.BasicStatementFactory;
import nablarch.core.db.statement.ParameterizedSqlPStatement;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlParameterParser;

/**
 * キャッシュ機能を備えた{@link nablarch.core.db.statement.StatementFactory}実装クラス。
 *
 * @author T.Kawasaki
 */
public class CacheableStatementFactory extends BasicStatementFactory {

    /** 有効期限設定 */
    private ExpirationSetting expirationSetting;

    /** キャッシュ */
    private ResultSetCache resultSetCache;

    /**
     * {@inheritDoc}
     * 指定されたSQL IDがキャッシュ対象かどうかを判定し、
     * キャッシュ対象である場合は、キャッシュ機能を備えた{@link CacheableSqlPStatement}を返却する。
     * キャッシュ対象でない場合、スーパクラスのメソッドが起動される。
     *
     * @param sqlId 下記形式のSQL_ID（SQLリソース名 + "#" + SQL_ID）
     * @return {@link SqlPStatement}実装クラスのインスタンス
     * @see BasicStatementFactory#getSqlPStatementBySqlId(String, Connection, DbExecutionContext)
     */
    @Override
    public SqlPStatement getSqlPStatementBySqlId(String sqlId, Connection con, DbExecutionContext context) throws SQLException {
        checkStatus();
        if (!isCacheTarget(sqlId)) {
            return super.getSqlPStatementBySqlId(sqlId, con, context);
        }
        String sql = getSql(sqlId);
        CacheableSqlPStatement p = new CacheableSqlPStatement(sql, con.prepareStatement(sql), sqlId);
        setCommonPropsTo(p, context);
        p.setAdditionalInfo(String.format("SQL_ID = [%s]", sqlId));
        setRSCacheAttrTo(p);         // キャッシュ設定
        return p;
    }

    /**
     * {@inheritDoc}
     * 指定されたSQL IDがキャッシュ対象かどうかを判定し、
     * キャッシュ対象である場合は、キャッシュ機能を備えた{@link CacheableSqlPStatement}を返却する。
     * キャッシュ対象でない場合、スーパクラスのメソッドが起動される。
     *
     * @param sqlId 下記形式のSQL_ID（SQLリソース名 + "#" + SQL_ID）
     * @param con   コネクション
     * @return {@link ParameterizedSqlPStatement}実装クラスのインスタンス
     * @see BasicStatementFactory#getParameterizedSqlPStatementBySqlId(String, Connection, DbExecutionContext)
     */
    @Override
    public ParameterizedSqlPStatement getParameterizedSqlPStatementBySqlId(
            String sqlId, Connection con, DbExecutionContext context) throws SQLException {
        checkStatus();
        if (!isCacheTarget(sqlId)) {
            return super.getParameterizedSqlPStatementBySqlId(sqlId, con, context);
        }
        String sql = getSql(sqlId);
        return getParameterizedSqlPStatementBySqlId(sql, sqlId, con, context);
    }


    /**
     * {@inheritDoc}
     * 指定されたSQL IDがキャッシュ対象かどうかを判定し、
     * キャッシュ対象である場合は、キャッシュ機能を備えた{@link CacheableSqlPStatement}を返却する。
     * キャッシュ対象でない場合、スーパクラスのメソッドが起動される。
     *
     * @param original オリジナルのSQL
     * @param sqlId    SQL ID（SQLリソース名 + "#" + SQL_ID）
     * @param con      コネクション
     * @return {@link ParameterizedSqlPStatement}実装クラスのインスタンス
     * @see BasicStatementFactory#getParameterizedSqlPStatementBySqlId(String, String, Connection, DbExecutionContext)
     */
    @Override
    public ParameterizedSqlPStatement getParameterizedSqlPStatementBySqlId(
            String original, String sqlId, Connection con, DbExecutionContext context) throws SQLException {
        checkStatus();
        if (!isCacheTarget(sqlId)) {
            return super.getParameterizedSqlPStatementBySqlId(original, sqlId, con, context);
        }

        // 名前付きバインド変数の置き換え
        SqlParameterParser parser = createParser();
        parser.parse(original);
        String parsedSql = parser.getSql();
        CacheableSqlPStatement sqlp = new CacheableSqlPStatement(
                parsedSql,
                con.prepareStatement(parsedSql),
                parser.getNameList(),
                sqlId);
        setCommonPropsTo(sqlp, context); // 共通設定
        setObjectFieldPropsTo(sqlp);     // オブジェクトのフィールドの値を扱う場合の設定
        setLikeConditionPropsTo(sqlp);   // like条件用の設定
        setRSCacheAttrTo(sqlp);          // 結果セットキャッシュ設定
        // 追加情報にSQLIDとオリジナルのSQLを設定する。
        sqlp.setAdditionalInfo(buildAdditionalInfoForSqlID(sqlId, original));
        return sqlp;
    }

    /**
     * 指定されたステートメントにキャッシュに関する以下の属性を設定する。
     * <ul>
     * <li>有効期限設定</li>
     * <li>キャッシュ</li>
     * </ul>
     *
     * @param sqlp 設定対象となるステートメント
     */
    protected void setRSCacheAttrTo(CacheableSqlPStatement sqlp) {
        sqlp.setCacheExpiration(expirationSetting);
        sqlp.setResultSetCache(resultSetCache);
    }

    /**
     * 指定されたSQL IDがキャッシュ対象かどうか判定する。
     *
     * @param sqlId 判定対象となるSQL ID
     * @return キャッシュ対象である場合、真
     */
    private boolean isCacheTarget(String sqlId) {
        return expirationSetting.isCacheEnable(sqlId);
    }

    /**
     * キャッシュ有効期限設定を設定する（必須）。
     * 本メソッドはDIコンテナから起動されることを想定している。
     *
     * @param expirationSetting 有効期限設定
     */
    public void setExpirationSetting(ExpirationSetting expirationSetting) {
        this.expirationSetting = expirationSetting;
    }

    /**
     * キャッシュを設定する（必須）。
     * 本メソッドはDIコンテナから起動されることを想定している。
     *
     * @param resultSetCache キャッシュ
     */
    public void setResultSetCache(ResultSetCache resultSetCache) {
        this.resultSetCache = resultSetCache;
    }

    /**
     * ステータスのチェックを行う。
     * 必要なプロパティが全て設定されていることを確認する。
     */
    private void checkStatus() {
        if (expirationSetting == null) {
            throw new IllegalStateException("expirationSetting must be set.");
        }
        if (resultSetCache == null) {
            throw new IllegalStateException("resultSetCache must be set.");
        }

    }
}
