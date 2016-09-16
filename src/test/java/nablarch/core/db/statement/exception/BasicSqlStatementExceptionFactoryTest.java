package nablarch.core.db.statement.exception;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.SQLException;

import nablarch.core.db.DbExecutionContext;
import nablarch.core.db.dialect.Dialect;

import org.junit.Test;

import mockit.Expectations;
import mockit.Mocked;

/**
 * {@link BasicSqlStatementExceptionFactory}のテストクラス。<br>
 *
 * @author Hisaaki Sioiri
 */
public class BasicSqlStatementExceptionFactoryTest {

    @Mocked
    private DbExecutionContext context;
    private BasicSqlStatementExceptionFactory factory = new BasicSqlStatementExceptionFactory();

    /**
     * {@link nablarch.core.db.statement.exception.BasicSqlStatementExceptionFactory#createSqlStatementException(String, java.sql.SQLException)} のテスト。
     * Dialectが一意制約違反と判定した場合
     *
     * @throws Exception
     */
    @Test
    public void testCreateSqlStatementExceptionIsDuplicateException() {

        final SQLException sqle = new SQLException("message", "E001", 1);
        new Expectations() {{
            Dialect dialect = context.getDialect();
            dialect.isDuplicateException(sqle);
            result = true;
        }};
        SqlStatementException sqlStatementException = factory.createSqlStatementException("一意制約違反と判定するDialect", sqle, context);
        assertThat("一意制約違反のExceptionである。", sqlStatementException, instanceOf(DuplicateStatementException.class));
        assertThat("メッセージ比較", sqlStatementException.getMessage(), is("一意制約違反と判定するDialect"));
    }

    /**
     * {@link nablarch.core.db.statement.exception.BasicSqlStatementExceptionFactory#createSqlStatementException(String, java.sql.SQLException)} のテスト。
     *  Dialectが一意制約違反でないと判定した場合
     * @throws Exception
     */
    @Test
    public void testCreateSqlStatementExceptionIsNotDuplicateException() {
        final SQLException sqle2 = new SQLException("message", "E001", 1);
        new Expectations() {{
            Dialect dialect = context.getDialect();
            dialect.isDuplicateException(sqle2);
            result = false;
        }};
        SqlStatementException sqlStatementException2 = factory.createSqlStatementException("一意制約違反と判定しないDialect", sqle2, context);
        assertThat("一意制約違反のExceptionでない。", sqlStatementException2, instanceOf(SqlStatementException.class));
        assertThat("メッセージ比較", sqlStatementException2.getMessage(), is("一意制約違反と判定しないDialect"));
    }
}
