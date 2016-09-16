package nablarch.core.db.connection.exception;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import nablarch.core.db.DbAccessException;
import nablarch.core.db.DbExecutionContext;
import nablarch.core.db.connection.BasicDbConnection;
import nablarch.core.db.dialect.DefaultDialect;
import nablarch.core.db.dialect.Dialect;
import nablarch.core.db.dialect.OracleDialect;
import nablarch.core.db.statement.BasicSqlParameterParserFactory;
import nablarch.core.db.statement.BasicStatementFactory;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.exception.BasicSqlStatementExceptionFactory;
import nablarch.core.db.statement.exception.SqlStatementException;
import nablarch.core.transaction.TransactionContext;

import org.junit.Test;

import mockit.Expectations;
import mockit.Mocked;

/**
 * {@link BasicDbAccessExceptionFactory}のテスト。
 * @author Kiyohito Itoh
 */
public class BasicDbAccessExceptionFactoryTest {

    @Mocked
    public Connection connection;

    /**
     * connection引数がnullの場合に{@link DbConnectionException}が生成されること。
     */
    @Test
    public void testNullConnection() {
        BasicDbAccessExceptionFactory factory = new BasicDbAccessExceptionFactory();
        DbAccessException e = factory.createDbAccessException("test_null_con", new SQLException("reason_null_con"), null);
        assertTrue(e instanceof DbConnectionException);
        assertThat(e.getMessage(), is("test_null_con"));
        assertThat(e.getCause().getMessage(), is("reason_null_con"));
    }

    /**
     * {@link SqlStatementException}を捕捉した場合、
     * かつ接続エラーの場合に{@link DbConnectionException}が生成されること。
     */
    @Test
    public void testCatchSqlStatementExceptionWithConnectionError() throws Exception {
        BasicStatementFactory statementFactory = new BasicStatementFactory();
        statementFactory.setSqlStatementExceptionFactory(new BasicSqlStatementExceptionFactory());
        statementFactory.setSqlParameterParserFactory(new BasicSqlParameterParserFactory());
        BasicDbConnection con = new BasicDbConnection(connection);
        final Dialect dialect = new DefaultDialect() {
            @Override
            public String getPingSql() {
                return "select 1";
            }
        };
        con.setContext(new DbExecutionContext(con, dialect, TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
        con.setFactory(statementFactory);

        new Expectations() {{
            PreparedStatement ps = connection.prepareStatement(dialect.getPingSql());
            ps.execute();
            result = new SQLException("failed to connect with mock.");
        }};

        BasicDbAccessExceptionFactory factory = new BasicDbAccessExceptionFactory();

        DbAccessException e = factory.createDbAccessException("test_sql_error", new SQLException("reason_sql_error"), con);
        assertThat(e, is(instanceOf(DbConnectionException.class)));
        assertThat(e.getMessage(), is("test_sql_error"));
        assertThat(e.getCause().getMessage(), is("reason_sql_error"));
    }

    /**
     * {@link SqlStatementException}を捕捉した場合、
     * かつ接続エラーでない場合は{@link DbAccessException}が生成されること。
     */
    @Test
    public void testCatchSqlStatementExceptionWithNoConnectionError() throws Exception {
        BasicStatementFactory statementFactory = new BasicStatementFactory();
        statementFactory.setSqlStatementExceptionFactory(new BasicSqlStatementExceptionFactory());
        statementFactory.setSqlParameterParserFactory(new BasicSqlParameterParserFactory());
        BasicDbConnection con = new BasicDbConnection(connection);
        con.setFactory(statementFactory);
        final Dialect dialect = new DefaultDialect() {
            @Override
            public String getPingSql() {
                return "select 'ping'";
            }
        };
        con.setContext(new DbExecutionContext(con, dialect, TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));

        new Expectations(){{
            connection.prepareStatement(dialect.getPingSql()).execute();
        }};

        BasicDbAccessExceptionFactory factory = new BasicDbAccessExceptionFactory();
        DbAccessException e = factory.createDbAccessException("test_sql_error", new SQLException("reason_sql_error"), con);
        assertThat(e, is(instanceOf(DbAccessException.class)));
        assertThat(e.getMessage(), is("test_sql_error"));
        assertThat(e.getCause().getMessage(), is("reason_sql_error"));
    }

    /**
     * {@link SqlStatementException}以外の例外を捕捉した場合に{@link DbAccessException}が生成されること。
     */
    @Test
    public void testCatchNonSqlStatementException() {
        BasicDbConnection con = new BasicDbConnection(null) {
            @Override
            public SqlPStatement prepareStatement(String sql) {
                throw new IllegalArgumentException("rt_ex_test");
            }
        };
        con.setContext(new DbExecutionContext(con, new OracleDialect(), "con"));
        BasicDbAccessExceptionFactory factory = new BasicDbAccessExceptionFactory();
        DbAccessException e = factory.createDbAccessException("test_rt_ex", new SQLException("reason_rt_ex"), con);
        assertThat(e, is(instanceOf(DbAccessException.class)));
        assertThat(e.getMessage(), is("test_rt_ex"));
        assertThat(e.getCause().getMessage(), is("reason_rt_ex"));
    }
}
