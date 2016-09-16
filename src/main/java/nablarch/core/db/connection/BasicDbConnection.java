package nablarch.core.db.connection;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nablarch.core.db.DbAccessException;
import nablarch.core.db.DbExecutionContext;
import nablarch.core.db.dialect.Dialect;
import nablarch.core.db.statement.ParameterizedSqlPStatement;
import nablarch.core.db.statement.SelectOption;
import nablarch.core.db.statement.SqlCStatement;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlStatement;
import nablarch.core.db.statement.StatementFactory;
import nablarch.core.db.transaction.JdbcTransactionTimeoutHandler;
import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;

/**
 * {@link TransactionManagerConnection}の基本実装クラス。
 * <p/>
 * 本オブジェクトは、マルチスレッド環境で使用されることは想定しない。すなわち、スレッドアンセーフなオブジェクトである。
 *
 * @author Hisaaki Sioiri
 * @see java.sql.Connection
 */
public class BasicDbConnection implements TransactionManagerConnection {

    /** ロガー */
    private static final Logger LOGGER = LoggerManager.get(BasicDbConnection.class);

    /** データベース接続オブジェクト */
    private final Connection con;

    /** クローズされているか否か */
    private boolean isClose;

    /** statements(リソース開放用) */
    private List<SqlStatement> statements = new ArrayList<SqlStatement>();

    /** Statement生成用Factoryクラス */
    private StatementFactory factory;

    /** SqlPStatementのキャッシュ可否 */
    private boolean statementReuse;

    /** SqlPStatementのキャッシュ */
    private Map<String, SqlStatement> cacheStatements = null;

    /** {@link nablarch.core.db.DbAccessException}ファクトリオブジェクト */
    private DbAccessExceptionFactory dbAccessExceptionFactory;

    /** トランザクションタイムアウトタイマー */
    private JdbcTransactionTimeoutHandler jdbcTransactionTimeoutHandler;

    /** DBアクセス時の実行コンテキスト */
    private DbExecutionContext context;

    /**
     * 指定されたデータ接続を保持するオブジェクトを生成する。
     *
     * @param con データベース接続オブジェクト
     */
    public BasicDbConnection(Connection con) {
        this.con = con;
        isClose = false;
    }

    /**
     * データベース接続の初期化処理を行う。<br>
     * <p>
     * 下記処理を行う。<br>
     * <ul>
     * <li>Auto commitモードを無効化</li>
     * </ul>
     * </p>
     */
    @Override
    public void initialize() {
        try {
            con.setAutoCommit(false);
        } catch (SQLException e) {
            throw new DbAccessException("failed to initialize.", e);
        }
    }

    /**
     * 現在のデータベース接続に対してcommitを実行する。
     *
     * @see java.sql.Connection#commit()
     */
    @Override
    public void commit() {
        try {
            con.commit();
        } catch (SQLException e) {
            throw new DbAccessException("failed to commit.", e);
        }
    }

    /** 現在のデータベース接続に対してrollbackを実行する。 */
    @Override
    public void rollback() {
        try {
            con.rollback();
        } catch (SQLException e) {
            throw dbAccessExceptionFactory.createDbAccessException("failed to rollback.", e, this);
        }
    }

    /**
     * データベース接続の終了処理を行う。<br>
     * 本処理では、下記処理を行う。
     * <ol>
     * <li>ロールバック処理(未確定のトランザクション情報は全て破棄する。)</li>
     * <li>本クラスで生成された{@link nablarch.core.db.statement.SqlStatement}のクローズ処理</li>
     * <li>ステートメントキャッシュが有効な場合のキャッシュのクリア処理</li>
     * <li>{@link #closeConnection()}の呼び出し</li>
     * </ol>
     *
     * @see java.sql.Statement#close
     */
    @Override
    public void terminate() {
        if (isClose) {
            return;
        }
        try {
            // 未コミットの情報はロールバックする。
            rollback();
            closeStatements();
            // キャッシュ情報をクリアする。
            statements = null;
            cacheStatements = null;
            isClose = true;
        } finally {
            try {
                closeConnection();
            } catch (SQLException e) {
                // rollback()/closeStatements() で実行時例外が送出された場合、
                // 元例外が送出されなくなるが、同種の例外であるため障害解析への影響は
                // 無いものとしてそのまま再送出する。
                throw new DbAccessException("failed to terminate.", e);
            }
        }
    }

    /**
     * コネクションをクローズする。<br/>
     * {@link java.sql.Connection#close()}を呼び出す。
     *
     * @throws SQLException SQL例外
     */
    protected void closeConnection() throws SQLException {
        con.close();
    }

    /**
     * アイソレーションレベルを設定する。<br>
     *
     * @param level アイソレーションレベル({@link java.sql.Connection}で定義されている次の定数のうち１つを設定する。)
     * <ul>
     * <li>{@link Connection#TRANSACTION_NONE}</li>
     * <li>{@link java.sql.Connection#TRANSACTION_READ_COMMITTED}</li>
     * <li>{@link java.sql.Connection#TRANSACTION_READ_UNCOMMITTED}</li>
     * <li>{@link java.sql.Connection#TRANSACTION_REPEATABLE_READ}</li>
     * <li>{@link java.sql.Connection#TRANSACTION_SERIALIZABLE}</li>
     * </ul>
     * @see java.sql.Connection
     */
    @Override
    public void setIsolationLevel(int level) {
        try {
            con.setTransactionIsolation(level);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setTransactionIsolation.",
                    e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public SqlPStatement prepareStatement(String sql) {
        return (SqlPStatement) new BasicDbConnection.StatementCreator() {
            @Override
            SqlStatement createSqlStatement(String sql) throws SQLException {
                return factory.getSqlPStatement(sql, con, getContext());
            }

            @Override
            String getErrorMessage(String sql) {
                return "failed to prepareStatement. SQL = [" + sql + ']';
            }
        }
        .create(sql);
    }

    /** {@inheritDoc} */
    @Override
    public SqlPStatement prepareStatement(String sql, final SelectOption selectOption) {
        sql = convertPaginationSql(sql, selectOption);
        return (SqlPStatement) new BasicDbConnection.StatementCreator() {
            @Override
            SqlStatement createSqlStatement(String sql) throws SQLException {
                return factory.getSqlPStatement(sql, con, getContext(), selectOption);
            }
            @Override
            String getErrorMessage(String sql) {
                return MessageFormat.format("failed to prepareStatement. SQL = [{0}], {1}", sql, selectOption);
            }
        }
        .create(sql, selectOption);
    }

    /** {@inheritDoc} */
    @Override
    public SqlPStatement prepareStatement(String sql, final int autoGeneratedKeys) {
        return (SqlPStatement) new BasicDbConnection.StatementCreator() {
            @Override
            SqlStatement createSqlStatement(String sql) throws SQLException {
                return factory.getSqlPStatement(sql, con, autoGeneratedKeys, getContext());
            }

            @Override
            String getErrorMessage(String sql) {
                return MessageFormat.format("failed to prepareStatement. SQL = [{0}], autoGeneratedKeys = [{1}]",
                        sql, autoGeneratedKeys);
            }
        }
        .create(sql, autoGeneratedKeys);
    }

    /** {@inheritDoc} */
    @Override
    public SqlPStatement prepareStatement(String sql, final int[] columnIndexes) {
        return (SqlPStatement) new StatementCreator() {
            @Override
            SqlStatement createSqlStatement(String sql) throws SQLException {
                return factory.getSqlPStatement(sql, con, columnIndexes, getContext());
            }

            @Override
            String getErrorMessage(String sql) {
                return MessageFormat.format("failed to prepareStatement. SQL = [{0}], columnIndexes = [{1}]",
                        sql, Arrays.toString(columnIndexes));
            }
        }
        .create(sql, Arrays.toString(columnIndexes));
    }

    /** {@inheritDoc} */
    @Override
    public SqlPStatement prepareStatement(String sql, final String[] columnNames) {
        return (SqlPStatement) new StatementCreator() {
            @Override
            SqlStatement createSqlStatement(String sql) throws SQLException {
                return factory.getSqlPStatement(sql, con, columnNames, getContext());
            }

            @Override
            String getErrorMessage(String sql) {
                return MessageFormat.format("failed to prepareStatement. SQL = [{0}], columnNames = [{1}]",
                        sql, Arrays.toString(columnNames));
            }
        }
        .create(sql, (Object[]) columnNames);
    }

    /** {@inheritDoc} */
    @Override
    public SqlPStatement prepareStatementBySqlId(String sqlId) {
        return (SqlPStatement) new BasicDbConnection.StatementCreator() {
            @Override
            SqlStatement createSqlStatement(String sql) throws SQLException {
                return factory.getSqlPStatementBySqlId(sql, con, getContext());
            }
            @Override
            String getErrorMessage(String sql) {
                return "failed to prepareStatementBySqlId. SQL_ID = [" + sql + ']';
            }
        }
        .create(sqlId);
    }

    /** {@inheritDoc} */
    @Override
    public SqlPStatement prepareStatementBySqlId(final String sqlId, final SelectOption selectOption) {
        String sql = factory.getVariableConditionSqlBySqlId(sqlId, null);
        sql = convertPaginationSql(sql, selectOption);
        return (SqlPStatement) new BasicDbConnection.StatementCreator() {
            @Override
            SqlStatement createSqlStatement(String sql) throws SQLException {
                return factory.getSqlPStatement(sql, con, getContext(), selectOption);
            }
            @Override
            String getErrorMessage(String sql) {
                return MessageFormat.format("failed to prepareStatementBySqlId. SQL_ID = [{0}], {1}" , sqlId, selectOption);
            }
        }
        .create(sql, selectOption);
    }


    /** {@inheritDoc} */
    @Override
    public ParameterizedSqlPStatement prepareParameterizedSqlStatement(String sql) {
        return (ParameterizedSqlPStatement) new BasicDbConnection.StatementCreator() {
            @Override
            SqlStatement createSqlStatement(String sql) throws SQLException {
                return factory.getParameterizedSqlPStatement(sql, con, getContext());
            }

            @Override
            String getErrorMessage(String sql) {
                return "failed to prepareParameterizedSqlStatement. SQL = [" + sql + ']';
            }
        }
        .create(sql);
    }

    /** {@inheritDoc} */
    @Override
    public ParameterizedSqlPStatement prepareParameterizedSqlStatement(String sql, final SelectOption selectOption) {
        sql = convertPaginationSql(sql, selectOption);
        return (ParameterizedSqlPStatement) new BasicDbConnection.StatementCreator() {
            @Override
            SqlStatement createSqlStatement(String sql) throws SQLException {
                return factory.getParameterizedSqlPStatement(sql, con, getContext(), selectOption);
            }

            @Override
            String getErrorMessage(String sql) {
                return MessageFormat.format("failed to prepareParameterizedSqlStatement. SQL = [{0}], {1}", sql, selectOption);
            }
        }
        .create(sql, selectOption);
    }

    /** {@inheritDoc} */
    @Override
    public ParameterizedSqlPStatement prepareParameterizedSqlStatementBySqlId(
            String sqlId) {
        return (ParameterizedSqlPStatement) new BasicDbConnection.StatementCreator() {
            @Override
            SqlStatement createSqlStatement(String sql) throws SQLException {
                return factory.getParameterizedSqlPStatementBySqlId(sql, con, getContext());
            }

            @Override
            String getErrorMessage(String sql) {
                return "failed to prepareParameterizedSqlStatementBySqlId. SQL_ID = [" + sql + ']';
            }
        }
        .create(sqlId);
    }

    /** {@inheritDoc} */
    @Override
    public ParameterizedSqlPStatement prepareParameterizedSqlStatementBySqlId(
            final String sqlId, final SelectOption selectOption) {
        String sql = factory.getVariableConditionSqlBySqlId(sqlId, null);
        sql = convertPaginationSql(sql, selectOption);
        return (ParameterizedSqlPStatement) new BasicDbConnection.StatementCreator() {
            @Override
            SqlStatement createSqlStatement(String sql) throws SQLException {
                return factory.getParameterizedSqlPStatement(sql, con, getContext(), selectOption);
            }

            @Override
            String getErrorMessage(String sql) {
                return MessageFormat.format("failed to prepareParameterizedSqlStatementBySqlId. SQL_ID = [{0}], {1}", sqlId, selectOption);
            }
        }
        .create(sql, selectOption);
    }


    /** {@inheritDoc} */
    @Override
    public ParameterizedSqlPStatement prepareParameterizedSqlStatement(
            String sql,
            Object condition) {
        String variableConditionSql = factory.getVariableConditionSql(sql,
                condition);
        return prepareParameterizedSqlStatement(variableConditionSql);
    }


    /** {@inheritDoc} */
    @Override
    public ParameterizedSqlPStatement prepareParameterizedSqlStatement(
            String sql, Object condition, SelectOption selectOption) {
        String variableConditionSql = factory.getVariableConditionSql(sql,
                condition);
        return prepareParameterizedSqlStatement(variableConditionSql, selectOption);
    }

    /** {@inheritDoc} */
    @Override
    public ParameterizedSqlPStatement prepareParameterizedSqlStatementBySqlId(
            final String sqlId, Object condition) {

        String variableConditionSql = factory.getVariableConditionSqlBySqlId(
                sqlId, condition);

        return (ParameterizedSqlPStatement) new BasicDbConnection.StatementCreator() {

            @Override
            SqlStatement createSqlStatement(String sql) throws SQLException {
                return factory.getParameterizedSqlPStatementBySqlId(sql, sqlId, con, getContext());
            }

            @Override
            String getErrorMessage(String sql) {
                return "failed to prepareParameterizedSqlStatementBySqlId. SQL_ID = [" + sqlId + ']';
            }
        }
        .create(variableConditionSql);
    }

    /** {@inheritDoc} */
    @Override
    public ParameterizedSqlPStatement prepareParameterizedSqlStatementBySqlId(
            final String sqlId, Object condition, final SelectOption selectOption) {

        String variableConditionSql = factory.getVariableConditionSqlBySqlId(
                sqlId, condition);
        variableConditionSql = convertPaginationSql(variableConditionSql, selectOption);
        return (ParameterizedSqlPStatement) new BasicDbConnection.StatementCreator() {

            @Override
            SqlStatement createSqlStatement(String sql) throws SQLException {
                return factory.getParameterizedSqlPStatementBySqlId(sql, sqlId, con, getContext(), selectOption);
            }

            @Override
            String getErrorMessage(String sql) {
                return MessageFormat.format("failed to prepareParameterizedSqlStatementBySqlId. SQL_ID = [{0}], {1}", sqlId, selectOption);
            }
        }
        .create(variableConditionSql, selectOption);
    }


    /**
     * {@inheritDoc}
     *
     * 件数取得用のSQLへの変換は、{@link Dialect#convertCountSql(String)}で行う。
     */
    @Override
    public ParameterizedSqlPStatement prepareParameterizedCountSqlStatementBySqlId(
            final String sqlId, Object condition) {

        String variableConditionSql = context.getDialect()
                .convertCountSql(factory.getVariableConditionSqlBySqlId(sqlId, condition));
        return (ParameterizedSqlPStatement) new BasicDbConnection.StatementCreator() {
            @Override
            SqlStatement createSqlStatement(String sql) throws SQLException {
                return factory.getParameterizedSqlPStatementBySqlId(sql, sqlId, con, getContext());
            }

            @Override
            String getErrorMessage(String sql) {
                return "failed to prepareParameterizedCountSqlStatementBySqlId. SQL_ID = [" + sqlId + ']';
            }
        }
        .create(variableConditionSql);
    }

    /**
     * {@inheritDoc}
     *
     * 件数取得用のSQLへの変換は、{@link Dialect#convertCountSql(String)}で行う。
     */
    @Override
    public SqlPStatement prepareCountStatementBySqlId(final String sqlId) {
        String variableConditionSql = context.getDialect()
                .convertCountSql(factory.getVariableConditionSqlBySqlId(sqlId, null));
        return (SqlPStatement) new BasicDbConnection.StatementCreator() {
            @Override
            SqlStatement createSqlStatement(String sql) throws SQLException {
                return factory.getParameterizedSqlPStatementBySqlId(sql, sqlId, con, getContext());
            }

            @Override
            String getErrorMessage(String sql) {
                return "failed to prepareCountStatementBySqlId. SQL_ID = [" + sqlId + ']';
            }
        }
        .create(variableConditionSql);
    }

    @Override
    public SqlCStatement prepareCall(String sql) {
        return (SqlCStatement) new BasicDbConnection.StatementCreator() {
            @Override
            SqlStatement createSqlStatement(String sql) throws SQLException {
                return factory.getSqlCStatement(sql, con, getContext());
            }

            @Override
            String getErrorMessage(String sql) {
                return "failed to prepareCall. SQL = [" + sql + ']';
            }
        }
        .create(sql);
    }

    @Override
    public SqlCStatement prepareCallBySqlId(final String sqlId) {
        return (SqlCStatement) new BasicDbConnection.StatementCreator() {
            @Override
            SqlStatement createSqlStatement(String paramSqlId) throws SQLException {
                return factory.getSqlCStatementBySqlId(paramSqlId, con, getContext());
            }

            @Override
            String getErrorMessage(String paramSqlId) {
                return "failed to prepareCallBySqlId. SQL_ID = [" + paramSqlId + ']';
            }
        }
        .create(sqlId);
    }

    /**
     * キャッシュから{@link SqlStatement}を取得して返却する。<br/>
     * <p/>
     * {@link #setStatementReuse(boolean)}にfalseを設定している場合や、
     * キャッシュにオブジェクトが存在しない場合は、nullを返却する。
     *
     * @param sql SQL文
     * @return キャッシュから取得した{@link SqlStatement}オブジェクト
     */
    private SqlStatement getCacheStatement(String sql) {
        SqlStatement ps = null;
        if (statementReuse) {
            if (cacheStatements == null) {
                cacheStatements = new HashMap<String, SqlStatement>();
            } else {
                ps = cacheStatements.get(sql);
            }
        }
        if (ps == null || ps.isClosed()) {
            return null;
        }
        return ps;
    }

    /**
     * キャッシュに{@link SqlStatement}を追加する。
     *
     * @param cacheKey キャッシューキー
     * @param statement SqlStatementオブジェクト
     */
    private void addCache(String cacheKey, SqlStatement statement) {
        // リソース解放用にstatementへの参照を保持しておく
        statements.add(statement);
        if (statementReuse) {
            cacheStatements.put(cacheKey, statement);
        }
    }

    /**
     * ステートメントをクローズする。<br>
     * 本データベースオブジェクトから生成されたステートメントオブジェクトを一括でクローズする。
     */
    private void closeStatements() {
        RuntimeException err = null;
        List<SqlStatement> temp = statements;
        statements = null;
        for (SqlStatement statement : temp) {
            try {
                statement.close();
            } catch (Throwable e) {
                LOGGER.logInfo("failed to closeStatements failed.", e);
                if (err == null) {
                    err = new RuntimeException("failed to closeStatements.", e);
                }
            }
        }
        if (err != null) {
            throw err;
        }
    }

    /**
     * {@link StatementFactory}実装クラスを設定する。
     *
     * @param factory {@link StatementFactory}実装クラス
     */
    public void setFactory(final StatementFactory factory) {
        this.factory = factory;
    }

    /**
     * ステートメントのキャッシュ有無を設定する。<br>
     *
     * @param statementReuse ステートメントのキャッシュ有無
     */
    public void setStatementReuse(boolean statementReuse) {
        this.statementReuse = statementReuse;
    }

    /**
     * {@link nablarch.core.db.DbAccessException}ファクトリオブジェクトを設定する。
     *
     * @param dbAccessExceptionFactory {@link nablarch.core.db.DbAccessException}ファクトリオブジェクト
     */
    public void setDbAccessExceptionFactory(DbAccessExceptionFactory dbAccessExceptionFactory) {
        this.dbAccessExceptionFactory = dbAccessExceptionFactory;
    }

    /**
     * ステートメントを生成するクラス。
     */
    private abstract class StatementCreator {

        /**
         * {@link SqlPStatement}を生成する。
         *
         * @param sql ステートメントを生成するためのSQL文
         * @param additionalList ステートメントを生成するための追加情報
         * @return 生成したステートメント
         */
        public SqlStatement create(String sql, Object... additionalList) {
            String cacheKey = buildCacheKey(sql, additionalList);
            SqlStatement statement = getCacheStatement(cacheKey);
            if (statement == null) {
                try {
                    statement = createSqlStatement(sql);
                } catch (SQLException e) {
                    throw new DbAccessException(getErrorMessage(sql), e);
                }
                addCache(cacheKey, statement);
            }
            statement.setJdbcTransactionTimeoutHandler(jdbcTransactionTimeoutHandler);
            return statement;
        }

        /**
         * SQLをキャッシュするためのキー値を生成する。
         * @param sql SQL
         * @param additionalList 追加情報
         * @return キャッシュキー
         */
        private String buildCacheKey(String sql, Object... additionalList) {
            if (additionalList.length == 0) {
                return sql;
            }
            StringBuilder key = new StringBuilder(sql);
            key.append("; additional info:");
            for (Object additional : additionalList) {
                key.append(additional);
                key.append(' ');
            }
            return key.toString();
        }

        /**
         * {@link SqlStatement}を生成する。
         *
         * @param sql {@link SqlStatement}を生成するためのSQL文
         * @return 生成したステートメントオブジェクト
         * @throws SQLException ステートメントの生成に失敗した場合
         */
        abstract SqlStatement createSqlStatement(String sql) throws SQLException;

        /**
         * ステートメントの生成に失敗した時のエラーメッセージ
         *
         * @param sql SQL文
         * @return エラーメッセージ
         */
        abstract String getErrorMessage(String sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setJdbcTransactionTimeoutHandler(JdbcTransactionTimeoutHandler jdbcTransactionTimeoutHandler) {
        this.jdbcTransactionTimeoutHandler = jdbcTransactionTimeoutHandler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection() {
        return con;
    }

    @Override
    public Dialect getDialect() {
        return context.getDialect();
    }

    /**
     * コンテキストを設定する。
     *
     * @param context コンテキスト
     */
    public void setContext(DbExecutionContext context) {
        this.context = context;
    }

    /**
     * コンテキストを取得する。
     *
     * @return DB実行コンテキスト
     */
    private DbExecutionContext getContext() {
        return this.context;
    }

    /**
     * ページング用のSQLに変換する。
     *
     * @param sql 変換するSQL
     * @param selectOption 検索条件オプション
     * @return dialectがoffsetをサポートする場合、検索範囲を設定したSQL
     */
    private String convertPaginationSql(String sql, final SelectOption selectOption) {
        if (isConvertToPaginatingSql(selectOption)) {
            sql = getDialect().convertPaginationSql(sql, selectOption);
        }
        return sql;
    }

    /**
     * ページングの変換処理をかけるかどうかを判定する。
     *
     * @param selectOption 検索処理オプション
     * @return ページングの変換処理をかけてよい場合はtrue.
     */
    private boolean isConvertToPaginatingSql(SelectOption selectOption)  {
        return getDialect().supportsOffset()
            && (selectOption.getStartPosition() > 1 || selectOption.getLimit() > 0);
    }

    @Override
    public void removeStatement(SqlStatement statement) {
        if (statements != null) {
            statements.remove(statement);
        }
    }
}

