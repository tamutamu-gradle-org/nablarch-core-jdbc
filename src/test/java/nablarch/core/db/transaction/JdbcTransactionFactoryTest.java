package nablarch.core.db.transaction;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hamcrest.CoreMatchers;

import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.transaction.Transaction;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.transaction.TransactionTimeoutException;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * DbTransactionのテストクラス。<br>
 *
 * @author Hisaaki Sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class JdbcTransactionFactoryTest {

    @ClassRule
    public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    /** timeoutのためにlockするスレッド */
    private static ExecutorService executorService = Executors.newSingleThreadExecutor();

    private TransactionManagerConnection dbConnection = null;

    private static final String UPDATE_QUERY = "UPDATE JDBC_TRAN_FACTORY_TEST SET COL1 = COL1 WHERE COL1 = '1'";

    @BeforeClass
    public static void setUp() throws Exception {
        VariousDbTestHelper.createTable(TestEntity.class);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        executorService.shutdownNow();
    }

    @Before
    public void setUpDbConnection() throws Exception {
        dbConnection = createDbConnection();
        DbConnectionContext.setConnection(dbConnection);
        VariousDbTestHelper.delete(TestEntity.class);
    }

    @After
    public void removeConnection() throws Exception {
        if (dbConnection != null) {
            dbConnection.terminate();
        }
        DbConnectionContext.removeConnection();
    }

    /**
     * setIsolationLevelのテストケース。
     *
     * @throws Exception
     */
    @Test
    public void testSetIsolationLevel() throws NoSuchFieldException, IllegalAccessException {

        JdbcTransactionFactory factory = new JdbcTransactionFactory();
        factory.setIsolationLevel("READ_COMMITTED");
        Field field = factory.getClass().getDeclaredField("isolationLevel");
        field.setAccessible(true);
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, field.get(factory));

        factory.setIsolationLevel("READ_UNCOMMITTED");
        assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, field.get(factory));

        factory.setIsolationLevel("REPEATABLE_READ");
        assertEquals(Connection.TRANSACTION_REPEATABLE_READ, field.get(factory));

        factory.setIsolationLevel("SERIALIZABLE");
        assertEquals(Connection.TRANSACTION_SERIALIZABLE, field.get(factory));

        // 不正なレベルを設定
        try {
            factory.setIsolationLevel("TEST");
            fail("");
        } catch (Exception e) {
            assertEquals("invalid isolation level. isolation level:TEST", e.getMessage());
        }
    }

    /** {@link nablarch.core.db.transaction.JdbcTransactionFactory#setInitSqlList(java.util.List)}のテスト。 */
    @Test
    public void setInitSqlList() throws SQLException {

        JdbcTransactionFactory factory = new JdbcTransactionFactory();

        List<String> sqlList = new ArrayList<String>();
        sqlList.add("insert into jdbc_tran_factory_test values('1')");
        sqlList.add("insert into jdbc_tran_factory_test values('2')");
        sqlList.add("insert into jdbc_tran_factory_test values('3')");
        factory.setInitSqlList(sqlList);
        Transaction transaction = factory.getTransaction(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        transaction.begin();
        // init sqlの内容はcommitされているので、別トランザクションからも参照できる。
        List<TestEntity> entities = VariousDbTestHelper.findAll(TestEntity.class, "col1");
        assertThat(entities.size(), is(3));
        assertThat(entities.get(0).col1, is("1"));
        assertThat(entities.get(1).col1, is("2"));
        assertThat(entities.get(2).col1, is("3"));
    }

    /** トランザクションタイムアウト設定に関連したテスト。 */
    @Test
    public void testTransactionTimeout() throws Exception {

        VariousDbTestHelper.setUpTable(new TestEntity("1"));

        JdbcTransactionFactory transactionFactory = new JdbcTransactionFactory();
        transactionFactory.setTransactionTimeoutSec(3);
        Transaction transaction = transactionFactory.getTransaction(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);

        transaction.begin();
        try {
            new TransactionTimeoutCaseExecutor() {
                @Override
                void execute(TransactionManagerConnection connection) {
                    SqlPStatement statement = connection.prepareStatement(UPDATE_QUERY);
                    statement.execute();
                };
            }.executeTestCase(dbConnection);
            fail("タイムアウトエラーが発生するからここは通らない。");
        } catch (TransactionTimeoutException e) {
            e.printStackTrace();
            assertThat(e.getMessage(), is(CoreMatchers.allOf(
                containsString("transaction was timeout."),
                containsString("transaction execution time = ["),
                endsWith("]")
            )));
        }
        transaction.commit();
    }

    private TransactionManagerConnection createDbConnection() throws SQLException {
        final ConnectionFactory connectionFactory = repositoryResource.getComponentByType(ConnectionFactory.class);

        return connectionFactory.getConnection(
                TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
    }

    /**
     * create table jdbc_tran_factory_test (col1 char(1))
     */
    @Table(name="jdbc_tran_factory_test")
    @Entity
    public static class TestEntity {

        public TestEntity() {
        }

        public TestEntity(String col1) {
            this.col1 = col1;
        }

        @Id
        @Column(name="col1", length=1)
        public String col1;
    }

    private abstract static class TransactionTimeoutCaseExecutor {
        abstract void execute(TransactionManagerConnection connection);
        /**
         * jdbc_tran_factory_test.col1 = 1 のレコードをロックして、{@link #execute(TransactionManagerConnection)}を実行する。
         *
         * @param connection {@link #execute(TransactionManagerConnection)}の引数。
         */
        void executeTestCase(final TransactionManagerConnection connection) throws Exception {
            final Connection lockConnection = VariousDbTestHelper.getNativeConnection();
            lockConnection.setAutoCommit(false);
            try {
                // lockする。
                final PreparedStatement lock = lockConnection.prepareStatement(UPDATE_QUERY);
                lock.executeUpdate();

                final Future<Boolean> future = executorService.submit(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        execute(connection);
                        return true;
                    }
                });
                try {
                    future.get();
                } catch (InterruptedException e) {
                    throw e;
                } catch (ExecutionException e) {
                    final Throwable cause = e.getCause();
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    }
                    throw new RuntimeException(cause);
                }
            } finally {
                try {
                    lockConnection.rollback();
                } finally {
                    lockConnection.close();
                }
            }
        }
    }
}
