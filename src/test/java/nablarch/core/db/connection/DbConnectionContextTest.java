package nablarch.core.db.connection;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import nablarch.core.transaction.TransactionContext;

import org.junit.After;
import org.junit.Test;

import mockit.Mocked;


/**
 * {@link DbConnectionContext}のテストクラス。
 *
 * @author hisaaki sioiri
 */
public class DbConnectionContextTest {

    @Mocked
    private AppDbConnection mockCon1;

    @Mocked
    private AppDbConnection mockCon2;

    @After
    public void cleanUpContext() {
        DbConnectionContext.removeConnection();
        DbConnectionContext.removeConnection("connectionName");
    }

    /**
     * {@link DbConnectionContext#getConnection()}のTest。
     *
     * @throws Exception
     */
    @Test
    public void testSetConnection() throws Exception {

        DbConnectionContext.setConnection(mockCon1);

        AppDbConnection connection = DbConnectionContext.getConnection();
        assertThat("設定したConnectionと同一のConnectionが取得できること。", connection, is(sameInstance(mockCon1)));

        try {
            DbConnectionContext.setConnection(mockCon2);
            fail("does not run.");
        } catch (Exception e) {
            assertEquals("メッセージの比較", String.format(
                    "specified database connection name was duplication in thread local. connection name = [%s]",
                    TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY), e.getMessage());
        }

        // コネクションの２重登録エラーが発生した場合にコネクションを上書きしていないことを確認。
        assertThat("初回に登録したものと同一のConnectionが取得できること。", DbConnectionContext.getConnection(), is(sameInstance(mockCon1)));

        // 指定された名前が登録されていない場合は、エラーとなること。
        DbConnectionContext.removeConnection();
        try {
            DbConnectionContext.getConnection();
            fail("does not run.");
        } catch (Exception e) {
            assertEquals(String.format(
                    "specified database connection name is not register in thread local. connection name = [%s]",
                    TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY), e.getMessage());
        }

        // 指定された名前が登録されていない場合に、コネクションを削除してもエラーにならないこと。
        DbConnectionContext.removeConnection();
    }

    /**
     * {@link DbConnectionContext#getConnection(String)}のTest。
     *
     * @throws Exception
     */
    @Test
    public void testSetConnection2() throws Exception {

        String connectionName = "connectionName";
        DbConnectionContext.setConnection(connectionName, mockCon1);

        AppDbConnection connection = DbConnectionContext.getConnection(connectionName);
        assertThat("設定したConnectionと同一のConnectionが取得できること。", connection, is(mockCon1));

        // コネクションを再び設定した場合は、エラーとなること。
        try {
            DbConnectionContext.setConnection(connectionName, mockCon2);
            fail("does not run.");
        } catch (Exception e) {
            assertEquals("メッセージの比較", String.format(
                    "specified database connection name was duplication in thread local. connection name = [%s]",
                    connectionName), e.getMessage());
        }

        // コネクションの２重登録エラーが発生した場合にコネクションを上書きしていないことを確認。
        assertThat("初回に登録したものと同一のConnectionが取得できること。",
                DbConnectionContext.getConnection(connectionName), is(sameInstance(mockCon1)));

        // 指定された名前が登録されていない場合は、エラーとなること。
        DbConnectionContext.removeConnection(connectionName);
        try {
            DbConnectionContext.getConnection(connectionName);
            fail("does not run.");
        } catch (Exception e) {
            assertEquals(String.format(
                    "specified database connection name is not register in thread local. connection name = [%s]",
                    connectionName), e.getMessage());
        }

        // 指定された名前が登録されていない場合に、コネクションを削除してもエラーにならないこと。
        DbConnectionContext.removeConnection(connectionName);
    }

    /**
     * {@link DbConnectionContext#containConnection(String)}のテスト。
     * @throws Exception
     */
    @Test
    public void testContainConnection() throws Exception {

        assertFalse("登録していないのでfalse", DbConnectionContext
                .containConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));

        DbConnectionContext.setConnection(mockCon1);
        assertTrue("登録したのでtrue", DbConnectionContext
                .containConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));

        DbConnectionContext.removeConnection();
        assertFalse("削除したのでfalse", DbConnectionContext
                .containConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
    }
    @Test
    public void testGetTransactionConnection(@Mocked TransactionManagerConnection tranConnection) {
        /* デフォルトのConnectionName */
        DbConnectionContext.setConnection(tranConnection);
        assertThat("設定したTransactionManagerConnectionが取得できる。", DbConnectionContext.getTransactionManagerConnection(), sameInstance(tranConnection));
        DbConnectionContext.removeConnection();

        String connectionName = "connectionName";
        DbConnectionContext.setConnection(connectionName, tranConnection);
        assertThat("設定したTransactionManagerConnectionが取得できる。", DbConnectionContext.getTransactionManagerConnection(connectionName),
                                                                   sameInstance(tranConnection));
        DbConnectionContext.removeConnection(connectionName);

    }

    /**
     * {@link DbConnectionContext#getTransactionManagerConnection()}で設定されたConnectionがTransactionConnectionを実装しない場合の確認。
     * 発生する例外を確認する。
     */
    @Test(expected = ClassCastException.class)
    public void testGetDefaultTransactionConnectionFail() {
        // AppDbConnectionしか実装していないMock。
        DbConnectionContext.setConnection("connectionName", mockCon1);
        // これはClassCast例外は発生しない。
        DbConnectionContext.getConnection("connectionName");
        DbConnectionContext.getTransactionManagerConnection("connectionName");
    }

    /**
     * 接続の削除後に再度新しい接続をスレッドに割り当てられること。
     * @throws Exception
     */
    @Test
    public void testCloseAndReConnect() throws Exception {
        DbConnectionContext.setConnection(mockCon1);
        DbConnectionContext.removeConnection();
        try {
            DbConnectionContext.getConnection();
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        
        DbConnectionContext.setConnection(mockCon2);
        assertThat(DbConnectionContext.getConnection(), is(sameInstance(mockCon2)));
        DbConnectionContext.removeConnection();
    }
}
