package nablarch.core.db.statement;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nablarch.core.beans.BeanUtil;
import nablarch.core.db.DbAccessException;
import nablarch.core.db.DbExecutionContext;
import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.dialect.Dialect;
import nablarch.core.db.statement.ParameterHolder.NopParameterHolder;
import nablarch.core.db.statement.ParameterHolder.ParamValue;
import nablarch.core.db.statement.exception.SqlStatementException;
import nablarch.core.db.transaction.JdbcTransactionTimeoutHandler;
import nablarch.core.db.util.DbUtil;
import nablarch.core.exception.IllegalOperationException;
import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;
import nablarch.core.util.StringUtil;

/**
 * {@link java.sql.PreparedStatement}のをラップしたクラス。<br>
 * 本クラスは、JDK5で実装済みのインタフェースのみを提供する。
 *
 * @author Hisaaki Sioiri
 * @see java.sql.PreparedStatement
 */
public class BasicSqlPStatement implements SqlPStatement, ParameterizedSqlPStatement {

    /** SQLログを出力するロガー */
    private static final Logger SQL_LOGGER = LoggerManager.get("SQL");

    /** ロガー */
    private static final Logger LOGGER = LoggerManager.get(BasicSqlPStatement.class);

    /** クラス名 */
    private static final String CLASS_NAME = BasicSqlPStatement.class.getName();

    /** SQL文 */
    private final String sql;

    /** Statement */
    private final PreparedStatement statement;

    /** 名前付きバインド変数の情報 */
    private final List<NamedParameterHolder> namedParameterHolderList = new ArrayList<NamedParameterHolder>();

    /** パラメータホルダー */
    protected nablarch.core.db.statement.ParameterHolder paramHolder = createParamHolder();        // SUPPRESS CHECKSTYLE サブクラスで使用するフィールドのため。

    /** バッチパラメータホルダー */
    private final BatchParameterHolder batchParameterHolder = createBatchParamHolder();

    /** SqlStatementExceptionFactory */
    private SqlStatementExceptionFactory sqlStatementExceptionFactory;

    /** Statementがcloseされているか否か */
    private boolean closed;

    /** オブジェクトのフィールドへの値自動設定用ハンドラー */
    private List<AutoPropertyHandler> updatePreHookObjectHandlerList;

    /** バッチサイズ */
    private int batchSize = 0;

    /** like条件のエスケープ文字 */
    private char likeEscapeChar;

    /** like条件のエスケープ対象文字リスト */
    private char[] likeEscapeTargetCharList;

    /** 付加情報 */
    private String additionalInfo;

    /** トランザクションタイムアウトヘルパー */
    private JdbcTransactionTimeoutHandler jdbcTransactionTimeoutHandler;

    /** DBアクセス時の実行時のコンテキスト */
    private DbExecutionContext context;

    /** 検索条件オプション */
    private SelectOption selectOption;

    /**
     * コンストラクタ。
     *
     * @param sql SQL文
     * @param statement PreparedStatement
     */
    public BasicSqlPStatement(String sql, PreparedStatement statement) {
        this(sql, statement, null);
    }

    /**
     * コンストラクタ。<br>
     * 名前付きバインド変数を持つSQL用
     *
     * @param sql SQL文
     * @param statement PreparedStatement
     * @param nameList 名前付き変数のリスト
     */
    public BasicSqlPStatement(String sql, PreparedStatement statement, List<String> nameList) {
        this.sql = sql;
        this.statement = statement;

        if (nameList != null) {
            for (String name : nameList) {
                namedParameterHolderList.add(new NamedParameterHolder(name));
            }
        }
        closed = false;
    }

    /** {@inheritDoc} */
    @Override
    public SqlResultSet retrieve() throws SqlStatementException {
        return doRetrieve(1, 0);
    }

    @Override
    public SqlResultSet retrieve(final int startPos, final int max) throws SqlStatementException {
        verifyNoSelectOption();
        return doRetrieve(startPos, max);
    }

    /**
     * 簡易検索処理を実行する。
     *
     * @param startPos 検索開始位置
     * @param max 最大取得件数
     */
    protected SqlResultSet doRetrieve(final int startPos, final int max) throws SqlStatementException {
        final int start;
        final int limit;
        if (needsClientSidePagination()) {
            start = selectOption.getStartPosition();
            limit = selectOption.getLimit();
        } else {
            start = startPos;
            limit = max;
        }
        return new BasicSqlPStatement.SqlExecutor<SqlResultSet>() {

            /** 検索開始ポジション */
            private int searchStartPos;

            /** SQL実行時間 */
            private long executeTime;

            /** fetch時間 */
            private long fetchTime;

            @Override
            void preprocess() {
                searchStartPos = start <= 0 ? 1 : start;
                setMaxRows(limit <= 0 ? 0 : limit + searchStartPos - 1);
                // 取得最大件数が指定されている場合は、フェッチサイズを最大取得件数にする。
                setFetchSize(limit <= 0 ? getFetchSize() : limit);
            }

            @Override
            SqlResultSet execute() throws SQLException {
                long executeStart = System.currentTimeMillis();
                ResultSet rs = statement.executeQuery();
                executeTime = System.currentTimeMillis() - executeStart;
                SqlResultSet result;
                Throwable error = null; //ステートメント実行中に発生した実行時例外/エラー

                try {
                    long fetchStart = System.currentTimeMillis();
                    result = createSqlResultSet(new ResultSetIterator(rs, getResultSetConvertor(), context), start, limit);
                    fetchTime = System.currentTimeMillis() - fetchStart;

                } catch (RuntimeException e) {
                    error = e;
                    throw e;
                } catch (Error e) {
                    error = e;
                    throw e;
                } finally {
                    try {
                        rs.close();
                    } catch (Throwable e) {
                        LOGGER.logWarn("failed to close result set.", e);
                        if (error == null) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                return result;
            }

            @Override
            void writeStartLog() {
                SQL_LOGGER.logDebug(SqlLogUtil.startRetrieve(CLASS_NAME + '#' + getSqlType(), sql,
                        searchStartPos, limit, getQueryTimeout(), getFetchSize(), additionalInfo));
                writeParameter();
            }

            @Override
            void writeEndLog(long executeTime, SqlResultSet result) {
                SQL_LOGGER.logDebug(SqlLogUtil.endRetrieve(CLASS_NAME + '#' + getSqlType(),
                        this.executeTime, fetchTime, result.size()));
            }

            @Override
            String getSqlType() {
                return "retrieve";
            }
        }
        .doSql();
    }

    /**
     * {@link SqlResultSet}を生成する。
     * オーバライドすることで、生成する{@link SqlResultSet}クラスを切り替えることができる。
     *
     * @param rs 元となる{@link ResultSetIterator}
     * @param startPos 読み出し開始位置
     * @param max 読み込み最大件数
     * @return 生成したインスタンス
     */
    protected SqlResultSet createSqlResultSet(ResultSetIterator rs, int startPos, int max) {
        return new SqlResultSet(rs, startPos, max);
    }


    /** {@inheritDoc} */
    @Override
    public SqlResultSet retrieve(Map<String, ?> data) throws SqlStatementException {
        return doRetrieve(1, 0, data);
    }

    /** {@inheritDoc} */
    @Override
    public SqlResultSet retrieve(int startPos, int max,
            Map<String, ?> data) throws SqlStatementException {
        verifyNoSelectOption();
        return doRetrieve(startPos, max, data);
    }

    /**
     * Mapを検索条件に設定して簡易検索処理を行う。
     *
     * @param startPos 検索処理開始位置
     * @param max 最大取得件数
     * @param data 検索条件
     * @return 取得結果
     * @throws SqlStatementException 実行時の例外
     */
    private SqlResultSet doRetrieve(int startPos, int max,
            Map<String, ?> data) throws SqlStatementException {
        try {
            setMap(data);
            return doRetrieve(startPos, max);
        } catch (SQLException e) {
            throw sqlStatementExceptionFactory
                    .createSqlStatementException("failed to retrieve.", e, context);
        }
    }

    /** {@inheritDoc } */
    @Override
    public SqlResultSet retrieve(Object data) throws SqlStatementException {
        return doRetrieve(1, 0, data);
    }

    /** {@inheritDoc } */
    @Override
    public SqlResultSet retrieve(int startPos, int max, Object data) throws SqlStatementException {
        verifyNoSelectOption();
        return doRetrieve(startPos, max, data);
    }

    /**
     * Objectを検索条件に設定して簡易検索処理を行う。
     *
     * @param startPos 検索処理開始位置
     * @param max 最大取得件数
     * @param data 検索条件
     * @return 取得結果
     * @throws SqlStatementException 実行時の例外
     */
    private SqlResultSet doRetrieve(int startPos, int max, Object data) throws SqlStatementException {
        try {
            setObject(data);
            return doRetrieve(startPos, max);
        } catch (SQLException e) {
            throw sqlStatementExceptionFactory
                    .createSqlStatementException("failed to retrieve.", e, context);
        }
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetIterator executeQueryByMap(Map<String, ?> data) throws SqlStatementException {
        try {
            setMap(data);
            return executeQuery();
        } catch (SQLException e) {
            throw sqlStatementExceptionFactory
                    .createSqlStatementException("failed to executeQueryByMap.", e, context);
        }
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetIterator executeQueryByObject(Object data) throws SqlStatementException {
        try {
            setObject(data);
            return executeQuery();
        } catch (SQLException e) {
            throw sqlStatementExceptionFactory
                    .createSqlStatementException("failed to executeQueryByObject.", e, context);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int executeUpdateByMap(Map<String, ?> data) throws SqlStatementException {
        try {
            setMap(data);
            return executeUpdate();
        } catch (SQLException e) {
            throw sqlStatementExceptionFactory
                    .createSqlStatementException("failed to executeUpdateByMap.", e, context);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void addBatchMap(Map<String, ?> data) {
        try {
            setMap(data);
            statement.addBatch();
            batchSize++;
            batchParameterHolder.add(paramHolder);
            paramHolder = createParamHolder();
        } catch (SQLException e) {
            throw new DbAccessException("failed to addBatchMap.", e);
        }
    }


    /** {@inheritDoc} */
    @Override
    public int executeUpdateByObject(Object data) throws SqlStatementException {
        try {
            setObject(data);
            return executeUpdate();
        } catch (SQLException e) {
            throw sqlStatementExceptionFactory.createSqlStatementException(
                    "failed to executeUpdateByObject. object name = [" + data.getClass().getName()
                            + "]", e, context);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void addBatchObject(Object data) {
        try {
            setObject(data);
            statement.addBatch();
            batchSize++;
            batchParameterHolder.add(paramHolder);
            paramHolder = createParamHolder();
        } catch (SQLException e) {
            throw new DbAccessException("failed to addBatchObject.", e);
        }
    }

    /**
     * {@link BatchParameterHolder}インスタンスを生成する。
     *
     * @return {@link BatchParameterHolder}インスタンス
     */
    protected BatchParameterHolder createBatchParamHolder() {
        if (isTraceLogEnabled()) {
            // ログ出力用にパラメータを保持しておく。
            return new BatchParameterHolder();
        }
        // ログ出力しない場合、パラメータをPreparedStatementとは別に保持する必要がない。
        return BatchParameterHolder.NopBatchParamHolder.getInstance();
    }

    /**
     * {@link nablarch.core.db.statement.ParameterHolder}インスタンスを生成する。
     *
     * @return {@link nablarch.core.db.statement.ParameterHolder}インスタンス
     */
    protected nablarch.core.db.statement.ParameterHolder createParamHolder() {
        if (isTraceLogEnabled()) {
            // ログ出力用にパラメータを保持しておく。
            return new nablarch.core.db.statement.ParameterHolder();
        }
        // ログ出力しない場合、パラメータをPreparedStatementとは別に保持する必要がない。
        return NopParameterHolder.getInstance();
    }

    /**
     * トレースログが出力可能か判定する。
     *
     * @return 出力可能な場合、真
     */
    protected boolean isTraceLogEnabled() {
        return SQL_LOGGER.isTraceEnabled();
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetIterator executeQuery() throws SqlStatementException {
        return new BasicSqlPStatement.SqlExecutor<ResultSetIterator>() {
            @Override
            void preprocess() {
                if (needsClientSidePagination()) {
                    int offset = selectOption.getOffset() < 0 ? 0 : selectOption.getOffset();
                    setMaxRows(offset + selectOption.getLimit());
                }
            }

            @Override
            ResultSetIterator execute() throws SQLException {
                ResultSetIterator iter = new ResultSetIterator(statement.executeQuery(), getResultSetConvertor(), context);
                iter.setStatement(BasicSqlPStatement.this);
                if (needsClientSidePagination()) {
                    for (int i = 0; (i < selectOption.getOffset()) && iter.next(); i++);
                }
                return iter;
            }

            @Override
            void writeStartLog() {
                SQL_LOGGER.logDebug(SqlLogUtil.startExecuteQuery(CLASS_NAME + '#' + getSqlType(), sql, additionalInfo));
                writeParameter();
            }

            @Override
            void writeEndLog(long executeTime, ResultSetIterator result) {
                if (SQL_LOGGER.isDebugEnabled()) {
                    SQL_LOGGER.logDebug(SqlLogUtil.endExecuteQuery(CLASS_NAME + '#' + getSqlType(), executeTime));
                }
            }

            @Override
            String getSqlType() {
                return "executeQuery";
            }
        }
        .doSql();
    }

    /** {@inheritDoc} */
    @Override
    public int executeUpdate() throws SqlStatementException {
        return new BasicSqlPStatement.SqlExecutor<Integer>() {
            @Override
            Integer execute() throws SQLException {
                return statement.executeUpdate();
            }

            @Override
            void writeStartLog() {
                SQL_LOGGER.logDebug(SqlLogUtil.startExecuteUpdate(CLASS_NAME + '#' + getSqlType(), sql,
                        additionalInfo));
                writeParameter();
            }

            @Override
            void writeEndLog(long executeTime, Integer result) {
                SQL_LOGGER.logDebug(SqlLogUtil.endExecuteUpdate(CLASS_NAME + '#' + getSqlType(), executeTime, result));
            }

            @Override
            String getSqlType() {
                return "executeUpdate";
            }
        }
        .doSql();
    }

    /** {@inheritDoc} */
    @Override
    public void setNull(final int parameterIndex, final int sqlType) {
        try {
            statement.setNull(parameterIndex, sqlType);
            paramHolder.add(parameterIndex, (Object) null);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setNull.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setBoolean(final int parameterIndex, final boolean x) {
        try {
            statement.setBoolean(parameterIndex, x);
            paramHolder.add(parameterIndex, x);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setBoolean.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setByte(final int parameterIndex, final byte x) {
        try {
            statement.setByte(parameterIndex, x);
            paramHolder.add(parameterIndex, x);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setByte.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setShort(final int parameterIndex, final short x) {
        try {
            statement.setShort(parameterIndex, x);
            paramHolder.add(parameterIndex, x);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setShort.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setInt(final int parameterIndex, final int x) {
        try {
            statement.setInt(parameterIndex, x);
            paramHolder.add(parameterIndex, x);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setInt.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setLong(final int parameterIndex, final long x) {
        try {
            statement.setLong(parameterIndex, x);
            paramHolder.add(parameterIndex, x);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setLong.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setFloat(final int parameterIndex, final float x) {
        try {
            statement.setFloat(parameterIndex, x);
            paramHolder.add(parameterIndex, x);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setFloat.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setDouble(final int parameterIndex, final double x) {
        try {
            statement.setDouble(parameterIndex, x);
            paramHolder.add(parameterIndex, x);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setDouble.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setBigDecimal(final int parameterIndex, final BigDecimal x) {
        try {
            statement.setBigDecimal(parameterIndex, x);
            paramHolder.add(parameterIndex, x);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setBigDecimal.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setString(final int parameterIndex, final String x) {
        try {
            statement.setString(parameterIndex, x);
            paramHolder.add(parameterIndex, x);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setString.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setBytes(final int parameterIndex, final byte[] x) {
        try {
            statement.setBytes(parameterIndex, x);
            paramHolder.add(parameterIndex, x);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setBytes.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setDate(final int parameterIndex, final Date x) {
        try {
            statement.setDate(parameterIndex, x);
            paramHolder.add(parameterIndex, x);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setDate.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setTime(final int parameterIndex, final Time x) {
        try {
            statement.setTime(parameterIndex, x);
            paramHolder.add(parameterIndex, x);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setTime.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x) {
        try {
            statement.setTimestamp(parameterIndex, x);
            paramHolder.add(parameterIndex, x);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setTimestamp.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x, final int length) {
        try {
            statement.setAsciiStream(parameterIndex, x, length);
            paramHolder.add(parameterIndex, x);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setAsciiStream.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x, final int length) {
        try {
            paramHolder.add(parameterIndex, x);
            statement.setBinaryStream(parameterIndex, x, length);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setBinaryStream.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void clearParameters() throws DbAccessException {
        try {
            statement.clearParameters();
        } catch (SQLException e) {
            throw new DbAccessException("failed to clearParameters.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType) {
        try {
            final Object dbValue = convertToDatabase(x);
            statement.setObject(parameterIndex, dbValue, targetSqlType);
            paramHolder.add(parameterIndex, dbValue);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setObject.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setObject(final int parameterIndex, final Object x) {
        try {
            final Object dbValue = convertToDatabase(x);
            statement.setObject(parameterIndex, dbValue);
            paramHolder.add(parameterIndex, dbValue);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setObject.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean execute() throws SqlStatementException {
        return new BasicSqlPStatement.SqlExecutor<Boolean>() {
            @Override
            Boolean execute() throws SQLException {
                return statement.execute();
            }

            @Override
            void writeStartLog() {
                SQL_LOGGER.logDebug(SqlLogUtil.startExecute(CLASS_NAME + '#' + getSqlType(), sql, additionalInfo));
                writeParameter();
            }

            @Override
            void writeEndLog(long executeTime, Boolean result) {
                SQL_LOGGER.logDebug(SqlLogUtil.endExecute(CLASS_NAME + '#' + getSqlType(), executeTime));
            }

            @Override
            String getSqlType() {
                return "execute";
            }
        }
        .doSql();
    }

    /** {@inheritDoc} */
    @Override
    public void addBatch() throws SqlStatementException {
        try {
            statement.addBatch();
            batchSize++;
            batchParameterHolder.add(paramHolder);
            paramHolder = createParamHolder();
        } catch (SQLException e) {
            throw new DbAccessException("failed to addBatch.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int getBatchSize() {
        return batchSize;
    }

    /** {@inheritDoc}未実装機能 */
    @Override
    public void setCharacterStream(final int parameterIndex, final Reader reader,
            final int length) {
        throw new UnsupportedOperationException(
                "BasicSqlPStatement#setCharacterStream is unsupported.");
    }

    /** {@inheritDoc}未実装機能 */
    @Override
    public void setRef(final int parameterIndex, final Ref x) {
        throw new UnsupportedOperationException("BasicSqlPStatement#setRef is unsupported.");
    }

    /** {@inheritDoc}未実装機能 */
    @Override
    public void setBlob(final int parameterIndex, final Blob x) {
        throw new UnsupportedOperationException("BasicSqlPStatement#setBlob is unsupported.");
    }

    /** {@inheritDoc}未実装機能 */
    @Override
    public void setClob(final int parameterIndex, final Clob x) {
        throw new UnsupportedOperationException("BasicSqlPStatement#setClob is unsupported.");
    }

    /** {@inheritDoc}未実装機能 */
    @Override
    public void setArray(final int parameterIndex, final Array x) {
        throw new UnsupportedOperationException("BasicSqlPStatement#setArray is unsupported.");
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetaData getMetaData() {
        try {
            return statement.getMetaData();
        } catch (SQLException e) {
            throw new DbAccessException("failed to getMetaData.", e);
        }
    }

    /** {@inheritDoc}未実装機能 */
    @Override
    public void setDate(final int parameterIndex, final Date x, final Calendar cal) {
        throw new UnsupportedOperationException("BasicSqlPStatement#setDate is unsupported.");
    }

    /** {@inheritDoc}未実装機能 */
    @Override
    public void setTime(final int parameterIndex, final Time x, final Calendar cal) {
        throw new UnsupportedOperationException("BasicSqlPStatement#setTime is unsupported.");
    }

    /** {@inheritDoc}未実装機能 */
    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x, final Calendar cal) {
        throw new UnsupportedOperationException("BasicSqlPStatement#setTimestamp is unsupported.");
    }

    /** {@inheritDoc}未実装機能 */
    @Override
    public void setNull(final int parameterIndex, final int sqlType, final String typeName) {
        throw new UnsupportedOperationException("BasicSqlPStatement#setNull is unsupported.");
    }

    /** {@inheritDoc}未実装機能 */
    @Override
    public void setURL(final int parameterIndex, final URL x) {
        throw new UnsupportedOperationException("BasicSqlPStatement#setURL is unsupported.");
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        try {
            statement.close();
            context.getConnection().removeStatement(this);
            closed = true;
        } catch (SQLException e) {
            throw new DbAccessException("failed to close.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxRows() {
        try {
            return statement.getMaxRows();
        } catch (SQLException e) {
            throw new DbAccessException("failed to getMaxRows.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setMaxRows(final int max) {
        try {
            statement.setMaxRows(max);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setMaxRows.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int getQueryTimeout() {
        try {
            return statement.getQueryTimeout();
        } catch (SQLException e) {
            throw new DbAccessException("failed to getQueryTimeout.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void setQueryTimeout(final int seconds) {
        try {
            statement.setQueryTimeout(seconds);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setQueryTimeout.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public ResultSet getResultSet() {
        try {
            return statement.getResultSet();
        } catch (SQLException e) {
            throw new DbAccessException("failed to getResultSet.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int getUpdateCount() {
        try {
            return statement.getUpdateCount();
        } catch (SQLException e) {
            throw new DbAccessException("failed to getUpdateCount.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean getMoreResults() {
        try {
            return statement.getMoreResults();
        } catch (SQLException e) {
            throw new DbAccessException("failed to getMoreResults.", e);
        }
    }

    /**
     * {@inheritDoc}
     * 未実装機能
     */
    @Override
    public void setFetchDirection(final int direction) {
        throw new UnsupportedOperationException(
                "BasicSqlPStatement#setFetchDirection is unsupported.");
    }

    /**
     * {@inheritDoc}
     * 未実装機能
     */
    @Override
    public int getFetchDirection() {
        throw new UnsupportedOperationException(
                "BasicSqlPStatement#getFetchDirection is unsupported.");
    }

    /** {@inheritDoc} */
    @Override
    public void setFetchSize(final int rows) {
        try {
            statement.setFetchSize(rows);
        } catch (SQLException e) {
            throw new DbAccessException("failed to setFetchSize.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int getFetchSize() {
        try {
            return statement.getFetchSize();
        } catch (SQLException e) {
            throw new DbAccessException("failed to getFetchSize.", e);
        }
    }

    /**
     * {@inheritDoc}
     * 未実装機能
     */
    @Override
    public int getResultSetConcurrency() {
        throw new UnsupportedOperationException(
                "BasicSqlPStatement#getResultSetConcurrency is unsupported.");
    }

    /**
     * {@inheritDoc}
     * 未実装機能
     */
    @Override
    public int getResultSetType() {
        throw new UnsupportedOperationException(
                "BasicSqlPStatement#getResultSetType is unsupported.");
    }

    /** {@inheritDoc} */
    @Override
    public void clearBatch() {
        try {
            statement.clearBatch();
            batchSize = 0;
        } catch (SQLException e) {
            throw new DbAccessException("failed to clearBatch.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int[] executeBatch() throws SqlStatementException {
        return new BasicSqlPStatement.SqlExecutor<int[]>() {
            @Override
            int[] execute() throws SQLException {
                int[] result = statement.executeBatch();
                batchSize = 0;
                return result;
            }

            @Override
            void writeStartLog() {
                SQL_LOGGER.logDebug(SqlLogUtil.startExecuteBatch(CLASS_NAME + '#' + getSqlType(), sql, additionalInfo));
                writeBatchParameter();
            }

            @Override
            void writeEndLog(long executeTime, int[] result) {
                SQL_LOGGER.logDebug(SqlLogUtil.endExecuteBatch(CLASS_NAME + "#executeBatch", executeTime,
                        result.length));
            }

            @Override
            String getSqlType() {
                return "executeBatch";
            }
        }
        .doSql();
    }

    /**
     * {@inheritDoc}
     * 未実装機能
     */
    @Override
    public boolean getMoreResults(final int current) {
        throw new UnsupportedOperationException(
                "BasicSqlPStatement#getMoreResults is unsupported.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getGeneratedKeys() {
        try {
            return statement.getGeneratedKeys();
        } catch (SQLException e) {
            throw new DbAccessException("failed to getGeneratedKeys.", e);
        }
    }

    /**
     * {@inheritDoc}
     * 未実装機能
     */
    @Override
    public int getResultSetHoldability() {
        throw new UnsupportedOperationException(
                "BasicSqlPStatement#getResultSetHoldability is unsupported.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean isClosed() {
        return closed;
    }

    /**
     * 現時点でのバインドパラメータを取得する。
     *
     * @return バインドパラメータ
     */
    protected final Map<String, ParamValue> getParameters() {
        return paramHolder.getParameters();
    }

    /** パラメータオブジェクト(バッチ実行用)をログ出力する。 */
    private void writeBatchParameter() {
        if (!SQL_LOGGER.isTraceEnabled()) {
            return;
        }
        String params = batchParameterHolder.toString();
        batchParameterHolder.clear();
        SQL_LOGGER.logTrace(CLASS_NAME + "#Parameters" + params);
    }

    /** パラメータオブジェクトをログ出力する。 */
    private void writeParameter() {
        if (!SQL_LOGGER.isTraceEnabled()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(CLASS_NAME).append("#Parameters");
        paramHolder.appendParameters(sb);
        SQL_LOGGER.logTrace(sb.toString());
    }

    /**
     * 指定されたMapの情報をバインド変数に設定する。
     *
     * @param map バインド変数への設定情報を持つMap
     * @throws SQLException データベースアクセス例外が発生した場合
     */
    private void setMap(Map<String, ?> map) throws SQLException {
        for (int i = 0; i < namedParameterHolderList.size(); i++) {
            final NamedParameterHolder namedParameterHolder = namedParameterHolderList.get(i);
            if (!map.containsKey(namedParameterHolder.getParameterName())) {
                throw new IllegalArgumentException(
                        String.format("SQL parameter was not found in Object. parameter name=[%s]",
                                namedParameterHolder.getParameterName()));
            }

            Object value = map.get(namedParameterHolder.getParameterName());
            if (namedParameterHolder.isLikeParameter()) {
                value = likeEscape(
                        value == null ? "" : value.toString(),
                        namedParameterHolder.isBackWardMatch(),
                        namedParameterHolder.isForwardMatch());
            } else if (namedParameterHolder.isArray()) {
                final Integer position = namedParameterHolder.getArrayPosition();
                if (position == null) {
                    value = null;
                } else {
                    value = DbUtil.getArrayValue(value, position);
                }
            }

            final Object dbValue = convertToDatabase(value);
            statement.setObject(i + 1, dbValue);
            paramHolder.add(namedParameterHolder.getParameterName(), dbValue);
        }
    }

    /**
     * データベースへ出力する値に変換する。
     * @param value 変換対象の値
     * @return 出力する値
     */
    private Object convertToDatabase(final Object value) {
        final Class<?> javaType = value == null ? null : value.getClass();
        final Dialect dialect = context.getDialect();
        return dialect.convertToDatabase(value, javaType);
    }

    /**
     * オブジェクトの属性情報をバインドパラメータに設定する。<br>
     *
     * @param data オブジェクト
     * @throws SQLException データベースアクセス例外が発生した場合
     */
    private void setObject(Object data) throws SQLException {

        if (updatePreHookObjectHandlerList != null) {
            for (AutoPropertyHandler anUpdatePreHookObjectHandlerList : updatePreHookObjectHandlerList) {
                anUpdatePreHookObjectHandlerList.handle(data);
            }
        }

        setMap(BeanUtil.createMapAndCopy(data));
    }

    /**
     * like条件をエスケープする。<br/>
     *
     * @param src エスケープ前の文字列
     * @param first 先頭に%を付加する場合はtrue
     * @param last 末尾に%を付加する場合はtrue
     * @return エスケープ後の文字列
     */
    private String likeEscape(String src, boolean first, boolean last) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < src.length(); i++) {
            char c = src.charAt(i);
            if (c == likeEscapeChar) {
                sb.append(likeEscapeChar).append(c);
                continue;
            }
            for (char aLikeEscapeTargetCharList : likeEscapeTargetCharList) {
                if (c == aLikeEscapeTargetCharList) {
                    sb.append(likeEscapeChar);
                    break;
                }
            }
            sb.append(c);
        }
        String dst = sb.toString();
        if (first) {
            dst = '%' + dst;
        }
        if (last) {
            dst = dst + '%';
        }
        return dst;
    }

    /**
     * SqlStatementExceptionFactoryを設定する。
     *
     * @param sqlStatementExceptionFactory SqlStatementExceptionFactory
     */
    public void setSqlStatementExceptionFactory(
            SqlStatementExceptionFactory sqlStatementExceptionFactory) {
        this.sqlStatementExceptionFactory = sqlStatementExceptionFactory;
    }

    /**
     * オブジェクトのフィールドへの値自動設定用ハンドラーを設定する。<br>
     * オブジェクトのフィールドの値をバインド変数に設定する事前処理として、
     * このハンドラーを使用してフィールドに値の自動設定を行う。
     *
     * @param updatePreHookObjectHandlerList オブジェクトハンドラー
     */
    public void setUpdatePreHookObjectHandlerList(
            List<AutoPropertyHandler> updatePreHookObjectHandlerList) {
        this.updatePreHookObjectHandlerList = updatePreHookObjectHandlerList;
    }

    /**
     * like条件のエスケープ文字を設定する。<br>
     *
     * @param likeEscapeChar like条件のエスケープ文字
     */
    public void setLikeEscapeChar(char likeEscapeChar) {
        this.likeEscapeChar = likeEscapeChar;
    }

    /**
     * like条件のエスケープ対象の文字リスト。<br>
     * <br>
     * 例：エスケープ対象の文字が、「%,％,_,＿」の場合<br>
     * <pre>
     * char[] escapeChar = {'%', '％', '_', '＿'};
     * setLikeEscapeTargetCharList(escapeChar);
     * </pre>
     *
     * @param likeEscapeTargetCharList エスケープ対象の文字を表す正規表現
     */
    public void setLikeEscapeTargetCharList(char[] likeEscapeTargetCharList) {
        this.likeEscapeTargetCharList = likeEscapeTargetCharList;
    }

    /**
     * 付加情報を設定する。<br>
     * ここで設定された情報は、付加情報としてSQLログに出力する。
     *
     * @param additionalInfo ログ出力用の補足情報
     */
    public void setAdditionalInfo(String additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    /** {@inheritDoc} */
    @Override
    public void setJdbcTransactionTimeoutHandler(JdbcTransactionTimeoutHandler jdbcTransactionTimeoutHandler) {
        this.jdbcTransactionTimeoutHandler = jdbcTransactionTimeoutHandler;
    }

    /**
     * DBアクセス時の実行時のコンテキストを設定する。
     *
     * @param context DBアクセス時の実行時のコンテキスト
     */
    public void setContext(DbExecutionContext context) {
        this.context =  context;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AppDbConnection getConnection() {
        return context.getConnection();
    }

    /**
     * SQL文を実行するためのテンプレートクラス。
     *
     * @param <T> SQL実行結果の型
     */
    private abstract class SqlExecutor<T> {

        /**
         * SQL文を実行する。
         * 本メソッドでは、以下の順に処理を行う。
         * <p/>
         * <ol>
         * <li>事前処理。必要に応じて実装クラスにて事前処理を行う。デフォルトでは、何もしない。</li>
         * <li>SQL文実行の開始ログの出力。実装クラスにて出力を行う。</li>
         * <li>トランザクションタイムアウトチェック（トランザクションタイムアウト設定がある場合のみ）</li>
         * <li>SQL文の実行。実行処理は実装クラスで処理を行う。</li>
         * <li>トランザクションタイムアウトの実行後チェック（トランザクションタイムアウト設定がある場合のみ）</li>
         * <li>SQL文実行の終了ログの出力。実装クラスにて出力を行う。</li>
         * <li>例外処理。
         * <ol>
         * <li>トランザクションタイムアウトのチェック。（トランザクションタイムアウト設定がある場合のみ）</li>
         * <li>例外の再送出（例外をSqlStatementExceptionにつけかえて再送出)</li>
         * </ol>
         * </li>
         * </ol>
         *
         * @return SQL文実行結果
         */
        T doSql() {
            try {
                // 事前処理
                preprocess();

                // 開始ログ
                if (SQL_LOGGER.isDebugEnabled()) {
                    writeStartLog();
                }

                if (jdbcTransactionTimeoutHandler != null) {
                    // トランザクションタイムアウトが有効な場合
                    int expiryTime = jdbcTransactionTimeoutHandler.getExpiryTimeSec();
                    jdbcTransactionTimeoutHandler.checkTransactionTimeout();
                    if (getQueryTimeout() <= 0 || expiryTime < getQueryTimeout()) {
                        setQueryTimeout(expiryTime);
                    }
                }

                /// SQL実行
                long executeStart = System.currentTimeMillis();
                T result = execute();
                long executeEnd = System.currentTimeMillis();

                if (jdbcTransactionTimeoutHandler != null) {
                    // 事後チェック（ここでチェックすることにより、無駄な業務処理の実行を防止できる）
                    jdbcTransactionTimeoutHandler.checkTransactionTimeout();
                }

                // 終了ログ
                if (SQL_LOGGER.isDebugEnabled()) {
                    writeEndLog((executeEnd - executeStart), result);
                }

                return result;
            } catch (SQLException e) {
                if (jdbcTransactionTimeoutHandler != null) {
                    jdbcTransactionTimeoutHandler.checkTransactionTimeout(e, context.getDialect());
                }
                throw sqlStatementExceptionFactory.createSqlStatementException(
                        "failed to " + getSqlType() + ". SQL = [" + sql + ']', e, context);
            }
        }

        /**
         * SQL実行前の準備処理。
         * <p/>
         * デフォルト実装では何も行わない。必要に応じて実装すること。
         */
        void preprocess() {
        }

        /**
         * SQL文を実行する。
         *
         * @return 実行結果
         * @throws SQLException SQL文の実行に失敗した場合
         */
        abstract T execute() throws SQLException;

        /** SQL文実行の開始時のログ出力を行う。 */
        abstract void writeStartLog();

        /**
         * SQL文の実行の終了ログの出力を行う。
         *
         * @param executeTime 実行時間
         * @param result SQLの実行結果
         */
        abstract void writeEndLog(long executeTime, T result);

        /**
         * SQLタイプ文字列を取得する。
         * <p/>
         * SQLタイプ文字列は、SQL実行時の例外メッセージとして使用する。
         *
         * @return SQLタイプ文字列。
         */
        abstract String getSqlType();
    }

    /**
     * 未処理の範囲指定があるかどうか。
     *
     * @return 検索オプションが設定されており、SQLにlimit, offsetが設定できない場合true
     */
    private boolean needsClientSidePagination() {
        return !supportsOffsetInSql() && hasSelectOption();
    }

    /**
     * Sql内にOffsetを付与する機能がサポートされているかどうか。
     *
     * @return Offsetが付与できる場合はtrue.
     */
    private boolean supportsOffsetInSql() {
        return context.getDialect().supportsOffset();
    }

    /**
     * 検索オプションの指定がされていないことを検証する。
     */
    private void verifyNoSelectOption() {
        if(hasSelectOption()){
            throw new IllegalOperationException(
                    "method call is not allowed.\n"
                  + "retrieve(int, int) method is only allowed when this object created by \n"
                  + "AppDbConnection#prepareStatement(java.lang.String) or #prepareParameterizedSqlStatement(java.lang.String).\n");
        }
    }

    /**
     * 検索オプションがあるかどうか。
     *
     * @return 検索オプションがあればtrue.
     */
    private boolean hasSelectOption() {
        return selectOption != null;
    }

    /**
     * 検索処理条件を設定する。
     *
     * @param selectOption 検索処理オプション
     */
    public void setSelectOption(SelectOption selectOption) {
       this.selectOption = selectOption;
    }

    /**
     * {@link nablarch.core.db.dialect.Dialect}に設定された{@link ResultSetConvertor}を取得する。
     *
     * @return {@link ResultSetConvertor}
     */
    private ResultSetConvertor getResultSetConvertor() {
        return context.getDialect()
                .getResultSetConvertor();
    }

    /**
     * パラメータ名を保持するクラス。
     */
    private static class NamedParameterHolder {

        /** 配列パターン */
        private static final Pattern array_pattern = Pattern.compile("^(.+)\\[([0-9]*)\\]$");

        /** パラメータ名 */
        private final String parameterName;

        /** 前方一致か否か */
        private final boolean forwardMatch;

        /** 後方一致か否か */
        private final boolean backWardMatch;

        /** 配列要素か否か */
        private final boolean array;

        /** 配列ポジション */
        private final Integer arrayPosition;

        /**
         * パラメータ名をもとに構築する。
         *
         * @param parameterName パラメータ名
         */
        public NamedParameterHolder(final String parameterName) {
            forwardMatch = isForwardMatchCondition(parameterName);
            backWardMatch = isBackWardMatchCondition(parameterName);

            // like検索の処理
            String tmpParameterName = parameterName;
            if (backWardMatch) {
                tmpParameterName = tmpParameterName.substring(1);
            }
            if (forwardMatch) {
                tmpParameterName = tmpParameterName.substring(0, tmpParameterName.length() - 1);
            }

            // 配列パラメータの処理
            final Matcher matcher = array_pattern.matcher(tmpParameterName);
            if (matcher.matches()) {
                // 配列を表すパラメータの場/**/合
                array = true;
                tmpParameterName = matcher.group(1);
                arrayPosition = toIntPosition(tmpParameterName, matcher.group(2));
            } else {
                array = false;
                arrayPosition = null;
            }
            this.parameterName = tmpParameterName;
        }

        /**
         * パラメータ名の添字を数値に変換する。
         * <p>
         * 数値に変換できない場合は、 {@link IllegalArgumentException}。ただし、添字が空文字列の場合はnullをかえす。
         *
         * @param parameterName プロパティ名
         * @param positionString 添字文字列
         * @return 数値に変換した添字(添字文字列が空文字列の場合はnull)
         */
        private static Integer toIntPosition(final String parameterName, final String positionString) {
            if (StringUtil.isNullOrEmpty(positionString)) {
                return null;
            }
            try {
                return Integer.valueOf(positionString);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        String.format("additional character of Array parameter is not numeric. parameter = [%s]",
                                parameterName), e);
            }
        }

        /**
         * 後方一致の条件か否か。
         *
         * @param parameterName 条件のプロパティ名
         * @return 後方一致の場合{@code true}
         */
        private static boolean isBackWardMatchCondition(final String parameterName) {
            return parameterName.startsWith("%");
        }

        /**
         * 前方一致の条件か否か。
         *
         * @param parameterName 条件のプロパティ名
         * @return 前方一致の場合{@code true}
         */
        private static boolean isForwardMatchCondition(final String parameterName) {
            return parameterName.endsWith("%");
        }

        /**
         * like検索か否か
         *
         * @return like検索の場合{@code true}
         */
        public boolean isLikeParameter() {
            return forwardMatch || backWardMatch;
        }

        /**
         * 前方一致か否か
         *
         * @return 前方一致の場合{@code true}
         */
        public boolean isForwardMatch() {
            return forwardMatch;
        }

        /**
         * 後方一致か否か
         *
         * @return 後方一致の場合{@code true}
         */
        public boolean isBackWardMatch() {
            return backWardMatch;
        }

        /**
         * パラメータ名を取得する。
         *
         * @return パラメータ名
         */
        public String getParameterName() {
            return parameterName;
        }

        /**
         * 配列を示すパラメータか否か
         *
         * @return 配列パラメータの場合{@code true}
         */
        public boolean isArray() {
            return array;
        }

        /**
         * 配列要素の添字を取得する。
         *
         * @return 配列要素の添字
         */
        public Integer getArrayPosition() {
            return arrayPosition;
        }
    }
}

