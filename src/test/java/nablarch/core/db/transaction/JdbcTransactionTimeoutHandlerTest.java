package nablarch.core.db.transaction;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.containsString;

import java.sql.SQLException;

import nablarch.core.db.dialect.DefaultDialect;
import nablarch.core.transaction.TransactionTimeoutException;

import org.junit.Test;

/**
 * {@link JdbcTransactionTimeoutHandler}のテストクラス。
 *
 * @author hisaaki sioiri
 */
public class JdbcTransactionTimeoutHandlerTest {

    /**
     * トランザクション有効期限が正しく計算されている事のテスト。
     *
     * @throws Exception
     */
    @Test
    public void testExpirationTimeSec() throws Exception {
        JdbcTransactionTimeoutHandler jdbcTransactionTimeoutHandler = new JdbcTransactionTimeoutHandler(2);
        jdbcTransactionTimeoutHandler.begin();
        assertThat("トランザクション開始直後は、有効秒数が残っている。", jdbcTransactionTimeoutHandler.getExpiryTimeSec() > 0, is(true));
        Thread.sleep(3000);
        assertThat(jdbcTransactionTimeoutHandler.getExpiryTimeSec() <= 0, is(true));
    }

    /** トランザクションタイムアウト時間に0を設定した場合、異常終了すること。 */
    @Test
    public void testTimeoutEqualsZero() {
        try {
            JdbcTransactionTimeoutHandler helper = new JdbcTransactionTimeoutHandler(0);
            fail("とおらないはず");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is(containsString(
                    "must be greater than 0.")));
        }
    }

    /** トランザクションタイムアウト時間に0未満の値を設定した場合、異常終了すること */
    @Test
    public void testTimeoutLessZero() {
        try {
            JdbcTransactionTimeoutHandler helper = new JdbcTransactionTimeoutHandler(-1);
            fail("とおらないはず");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is(containsString(
                    "must be greater than 0.")));
        }
    }

    /** トランザクションを開始せずに有効期限を取得した場合 */
    @Test
    public void testNotBegin() {
        JdbcTransactionTimeoutHandler helper = new JdbcTransactionTimeoutHandler(1);
        try {
            helper.getExpiryTimeSec();
            fail("とおらないはず");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is(containsString(
                    "transaction status is abnormal. must call #begin()")));
        }
    }

    /**
     * トランザクションタイムアウトチェック
     * <p/>
     * トランザクションタイムアウト時間に到達していない場合は、エラーが発生しないこと。
     */
    @Test
    public void testCheckTransactionTimeoutNormal() throws Exception {
        JdbcTransactionTimeoutHandler helper = new JdbcTransactionTimeoutHandler(2);
        helper.begin();
        Thread.sleep(1000);
        helper.checkTransactionTimeout();
        assertThat(true, is(true));
    }

    /**
     * トランザクションタイムアウトチェック
     * <p/>
     * 有効期限を過ぎている場合は、タイムアウトエラーが発生すること。
     */
    @Test(expected = TransactionTimeoutException.class)
    public void testCheckTransactionTimeoutError() throws Exception {
        JdbcTransactionTimeoutHandler helper = new JdbcTransactionTimeoutHandler(1);
        helper.begin();
        Thread.sleep(2000);
        helper.checkTransactionTimeout();
    }

    /**
     * トランザクションタイムアウトチェック。
     * <p/>
     * 有効期限は過ぎているが、SQLExceptionの例外がタイムアウト対象の例外コードではないため、
     * タイムアウトエラーは発生しない。
     */
    @Test
    public void testSQLExceptionCheckNormal() throws Exception {
        SQLException sqle = new SQLException("error", "state", 1000);
        JdbcTransactionTimeoutHandler helper = new JdbcTransactionTimeoutHandler(1);
        helper.begin();
        Thread.sleep(2000);

        // タイムアウトはしているはずだが、SQLExceptionの例外が、
        // タイムアウト対象のエラーコードではないためタイムアウトにはならない。
        helper.checkTransactionTimeout(sqle, new DefaultDialect() {
            @Override
            public boolean isTransactionTimeoutError(SQLException sqlException) {
                return sqlException.getErrorCode() == 1001;
            }
        });
    }

    /**
     * トランザクションタイムアウトチェック。
     * <p/>
     * 指定したエラーコードに一致するエラーコードを持つSQLExceptionが発生したが、
     * 有効期限を経過していない場合。
     */
    @Test
    public void testSQLExceptionCheckNormal2() throws Exception {
        SQLException sqle = new SQLException("error", "state", 1001);
        JdbcTransactionTimeoutHandler helper = new JdbcTransactionTimeoutHandler(1);
        helper.begin();
        Thread.sleep(1);
        helper.checkTransactionTimeout(sqle, new DefaultDialect() {
            @Override
            public boolean isTransactionTimeoutError(SQLException sqlException) {
                return true;
            }
        });
    }

    /**
     * トランザクションタイムアウトチェック。
     * <p/>
     * 指定したエラーコードに一致するエラーコードを持つSQLExceptionが発生し有効期限を経過している場合
     */
    @Test
    public void testSQLExceptionCheckTimeoutError() throws Exception {
        SQLException sqle = new SQLException("error", "state", 2001);
        JdbcTransactionTimeoutHandler helper = new JdbcTransactionTimeoutHandler(1);
        helper.begin();
        Thread.sleep(2000);
        try {
            helper.checkTransactionTimeout(sqle, new DefaultDialect() {
                @Override
                public boolean isTransactionTimeoutError(SQLException sqlException) {
                    return true;
                }
            });
            fail("ここはとおらない。");
        } catch (TransactionTimeoutException e) {
            e.printStackTrace();
            assertThat(e.getMessage(), is(containsString("transaction was timeout")));
        }
    }

    /**
     * トランザクションタイムアウトチェック。
     * <p/>
     * 1度目のトランザクションで例外が発生したが、再度beginを呼出て有効期限内にチェック処理を行った場合は例外が発生しない。
     */
    @Test
    public void testBegin() throws Exception {
        JdbcTransactionTimeoutHandler helper = new JdbcTransactionTimeoutHandler(1);
        helper.begin();
        Thread.sleep(2000);
        try {
            helper.checkTransactionTimeout();
            fail("ここはとおらない。");
        } catch (TransactionTimeoutException e) {
            assertThat(e.getMessage(), is(containsString("transaction was timeout")));
        }

        // 再度beginを呼び出して直後にチェックを行なってもエラーは発生しない。
        helper.begin();
        helper.checkTransactionTimeout();

        // タイムアウトが発生するまで再度待機した場合は、例外が発生する。
        helper.begin();
        Thread.sleep(2000);
        try {
            helper.checkTransactionTimeout();
            fail("ここはとおらない。");
        } catch (TransactionTimeoutException e) {
            assertThat(e.getMessage(), is(containsString("transaction was timeout")));
        }
    }
}

