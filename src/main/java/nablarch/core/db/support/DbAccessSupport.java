package nablarch.core.db.support;

import java.util.Map;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.ParameterizedSqlPStatement;
import nablarch.core.db.statement.ResultSetIterator;
import nablarch.core.db.statement.SqlCStatement;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.util.annotation.Published;

/**
 * クラスパス上のSQLが記述されたリソースファイル(SQLリソース)から、
 * 実行可能なSQLステートメントを取得するサポートクラス。
 *
 * @author hisaaki sioiri
 */
@Published
public class DbAccessSupport {

    /** SQLリソース名 */
    private final String sqlResource;

    /**
     * コンストラクタ。
     * 現在のインスタンスクラス名からSQLリソースを導出する。
     */
    public DbAccessSupport() {
        sqlResource = getClass().getName() + '#';
    }

    /**
     * コンストラクタ。
     * 指定されたクラスオブジェクトのクラス名からSQLリソースを導出する。
     * <p/>
     * 下記のように呼び出しを行う。
     * <pre>
     * DbAccessSupport dbSupport = new DbAccessSupport(getClass());
     * </pre>
     *
     * @param clazz DBアクセス処理を行うクラスノクラスオブジェクト
     */
    public DbAccessSupport(Class<?> clazz) {
        sqlResource = clazz.getName() + '#';
    }

    /**
     * 指定されたSQL_IDから{@link nablarch.core.db.statement.ParameterizedSqlPStatement}を生成する。
     *
     * @param sqlId SQL_ID
     * @return 生成した {@link nablarch.core.db.statement.ParameterizedSqlPStatement}
     */
    public final ParameterizedSqlPStatement getParameterizedSqlStatement(
            String sqlId) {
        AppDbConnection conn = DbConnectionContext.getConnection();
        return conn.prepareParameterizedSqlStatementBySqlId(
                makeSqlResourceId(sqlId));
    }

    /**
     * 指定されたSQL_IDと条件から{@link nablarch.core.db.statement.ParameterizedSqlPStatement}を生成する。
     *
     * @param sqlId SQL_ID
     * @param condition 条件をもつオブジェクト
     * @return 生成した {@link nablarch.core.db.statement.ParameterizedSqlPStatement}
     */
    public final ParameterizedSqlPStatement getParameterizedSqlStatement(
            String sqlId, Object condition) {
        AppDbConnection conn = DbConnectionContext.getConnection();
        return conn.prepareParameterizedSqlStatementBySqlId(
                makeSqlResourceId(sqlId), condition);
    }

    /**
     * 指定されたSQL_IDから{@link nablarch.core.db.statement.SqlPStatement}を生成する。
     *
     * @param sqlId SQL_ID
     * @return 生成した{@link nablarch.core.db.statement.SqlPStatement}
     */
    public final SqlPStatement getSqlPStatement(String sqlId) {
        AppDbConnection conn = DbConnectionContext.getConnection();
        return conn.prepareStatementBySqlId(makeSqlResourceId(sqlId));
    }

    /**
     * 指定されたSQL_IDから件数取得（カウント）用のSQL文を生成して実行する。
     *
     * 本メソッドは、外部から条件を指定する必要のないSQL文の場合に使用する。
     * 条件を指定する必要がある場合には、{@link #countByParameterizedSql(String, Object)}を使用すること。
     *
     * @param sqlId SQL_ID
     * @return 件数
     */
    public final int countByStatementSql(String sqlId) {
        AppDbConnection con = DbConnectionContext.getConnection();
        SqlPStatement statement = con.prepareCountStatementBySqlId(makeSqlResourceId(sqlId));
        ResultSetIterator rs = statement.executeQuery();
        return getCountQueryResult(rs);
    }

    /**
     * 指定されたSQL_IDと条件から件数取得(カウント)用のSQL文を生成して実行する。
     *
     * @param sqlId SQL_ID
     * @param condition 条件をもつオブジェクト
     * @return 件数
     */
    @SuppressWarnings("unchecked")
    public final int countByParameterizedSql(String sqlId, Object condition) {
        AppDbConnection conn = DbConnectionContext.getConnection();
        ParameterizedSqlPStatement stmt = conn
                .prepareParameterizedCountSqlStatementBySqlId(
                        makeSqlResourceId(sqlId), condition);
        ResultSetIterator rs;
        if (condition instanceof Map<?, ?>) {
            rs = stmt.executeQueryByMap((Map<String, ?>) condition);
        } else {
            rs = stmt.executeQueryByObject(condition);
        }
        return getCountQueryResult(rs);
    }

    /**
     * 件数取得クエリから結果を取得する。
     *
     * @param rs 件数取得クエリの実行結果
     * @return 件数
     */
    private int getCountQueryResult(ResultSetIterator rs) {
        try {
            if (rs.next()) {
                return rs.getInteger(1);
            } else {
                throw new IllegalStateException("Count query didn't return result.");
            }
        } finally {
            rs.close();
        }
    }

    /**
     * 指定されたSQL_IDと{@link nablarch.core.db.support.ListSearchInfo}から件数取得及び検索を実行する。
     * 検索結果の件数は、指定された{@link ListSearchInfo}オブジェクトに設定する。
     *
     * @param sqlId SQL_ID
     * @param condition {@link ListSearchInfo}オブジェクト
     * @return 検索結果
     * @throws TooManyResultException ページング付きの検索において検索結果件数が検索結果の最大件数(上限)を超えた場合。
     * 検索結果の最大件数(上限)の設定については、{@link ListSearchInfo#ListSearchInfo()}を参照。
     */
    public final SqlResultSet search(String sqlId,
            ListSearchInfo condition) throws TooManyResultException {
        int count = countByParameterizedSql(sqlId, condition);
        if (condition.getMaxResultCount() < count) {
            throw new TooManyResultException(condition.getMaxResultCount(),
                    count);
        }
        condition.setResultCount(count);
        return getParameterizedSqlStatement(sqlId, condition).retrieve(
                condition.getStartPosition(), condition.getMax(), condition);
    }

    /**
     * 指定されたSQL_IDから{@link SqlCStatement}を生成する。
     *
     * @param sqlId SQL_ID
     * @return ステートメント
     */
    public final SqlCStatement getSqlCStatement(String sqlId) {
        final AppDbConnection connection = DbConnectionContext.getConnection();
        return connection.prepareCallBySqlId(makeSqlResourceId(sqlId));
    }

    /**
     * SQL_IDからSQLリソースIDを作成する。
     *
     * @param sqlId SQL_ID
     * @return SQLリソースID
     */
    private String makeSqlResourceId(String sqlId) {
        return sqlResource + sqlId;
    }
}

