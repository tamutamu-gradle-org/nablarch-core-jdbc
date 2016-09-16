package nablarch.core.db.statement;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
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

import nablarch.core.db.connection.BasicDbConnection;
import nablarch.core.db.connection.BasicDbConnectionFactoryForDataSource;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.statement.exception.SqlStatementException;
import nablarch.core.db.transaction.JdbcTransactionTimeoutHandler;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.transaction.TransactionTimeoutException;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link BasicSqlPStatement}のトランザクションタイムアウトに着目したテスト。
 *
 * @author hisaaki sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class BasicSqlPStatementOnTransactionTimeoutTest {

    @ClassRule
    public static final SystemRepositoryResource systemRepositoryResource = new SystemRepositoryResource(
            "db-default.xml");

    /** タイムアウトを実現するためのレコードをロックするスレッド */
    private static ExecutorService executorService = Executors.newSingleThreadExecutor();

    /** テストコネクション */
    private BasicDbConnection dbConnection;

    /** SQLExceptionのトランザクションタイムアウト対象のエラー一覧 */
    private static final List<String> TRANSACTION_TIMEOUT_ERROR_LIST = new ArrayList<String>() {{
        add("1013");
        add("1234");
    }};

    @BeforeClass
    public static void classSetup() throws Exception {
        VariousDbTestHelper.createTable(TimeoutTestEntity.class);
    }

    @AfterClass
    public static void classTearDown() throws Exception {
        executorService.shutdownNow();
    }

    /** テストの準備 */
    @Before
    public void setup() throws Exception {
        dbConnection = createConnection();
        VariousDbTestHelper.setUpTable(new TimeoutTestEntity("1"));
    }

    @After
    public void tearDown() throws Exception {
        if (dbConnection != null) {
            dbConnection.terminate();
        }
    }

    /**
     * テストで使用するデータベース接続を生成する。
     * <p/>
     * 生成したデータベース接続はトランザクションが開始状態で、
     * トランザクションタイムアウトのイベントハンドラが設定された状態となっている。
     *
     * @return データベース接続
     */
    private BasicDbConnection createConnection() {
        final BasicDbConnectionFactoryForDataSource connectionFactory = systemRepositoryResource.getComponentByType(
                BasicDbConnectionFactoryForDataSource.class);
        final TransactionManagerConnection connection = connectionFactory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);

        if (!(connection instanceof BasicDbConnection)) {
            throw new IllegalStateException("コネクション実装クラスが基本実装ではありません。");
        }
        JdbcTransactionTimeoutHandler jdbcTransactionTimeoutHandler = new JdbcTransactionTimeoutHandler(2);
        connection.setJdbcTransactionTimeoutHandler(jdbcTransactionTimeoutHandler);
        jdbcTransactionTimeoutHandler.begin();
        return (BasicDbConnection) connection;
    }

    //******************************************************************************************************************
    // retrieveのテスト
    //******************************************************************************************************************

    /** トランザクションタイムアウトが発生しない場合 */
    @Test
    public void testRetrieveNormal() throws Exception {
        SqlPStatement statement = dbConnection.prepareStatement("SELECT COL1 FROM TIMEOUT_TEST");
        SqlResultSet result = statement.retrieve();
        Thread.sleep(500);
        assertThat(result.size(), is(1));
    }

    /** SQL実行前にタイムアウトしていた場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testRetrievePreTimeout() throws Exception {
        SqlPStatement statement = dbConnection.prepareStatement("SELECT COL1 FROM TIMEOUT_TEST");
        Thread.sleep(3000);
        statement.retrieve();
    }

    /** SQL実行中にタイムアウトした場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testRetrieveAfterTimeout() throws Throwable {
        new TransactionTimeoutCaseExecutor() {
            @Override
            void execute(BasicDbConnection connection) {
                SqlPStatement statement = connection.prepareStatement(
                        "UPDATE TIMEOUT_TEST SET COL1 = COL1 WHERE COL1 = '1'");
                statement.execute();
            }
        }.executeTestCase(dbConnection);
    }

    //******************************************************************************************************************
    // オブジェクトを条件にするretrieveのテスト
    //******************************************************************************************************************

    /** トランザクションタイムアウトが発生しない場合 */
    @Test
    public void testRetrieveObjectNormal() throws Exception {
        ParameterizedSqlPStatement statement = dbConnection.prepareParameterizedSqlStatement(
                "SELECT * FROM TIMEOUT_TEST WHERE COL1 = :col1");
        Thread.sleep(500);
        SqlResultSet result = statement.retrieve(new TimeoutTestEntity("1"));
        assertThat(result.size(), is(1));
    }

    /** SQL実行前にタイムアウトしていた場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testRetrieveObjectPreTimeout() throws Exception {
        ParameterizedSqlPStatement statement = dbConnection.prepareParameterizedSqlStatement(
                "SELECT * FROM TIMEOUT_TEST WHERE COL1 = :col1");
        Thread.sleep(3000);
        statement.retrieve(new TimeoutTestEntity("1"));
    }

    /** SQL実行中にタイムアウトした場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testRetrieveObjectAfterTimeout() throws Exception {
        new TransactionTimeoutCaseExecutor() {
            @Override
            void execute(BasicDbConnection connection) {
                ParameterizedSqlPStatement statement = connection.prepareParameterizedSqlStatement(
                        "UPDATE TIMEOUT_TEST SET COL1 = COL1 WHERE COL1 = :col1");
                statement.executeUpdateByObject(new TimeoutTestEntity("1"));

            }
        }.executeTestCase(dbConnection);
    }

    //******************************************************************************************************************
    // Mapを条件にするretrieveのテスト
    //******************************************************************************************************************

    /** トランザクションタイムアウトが発生しない場合 */
    @Test
    public void testRetrieveMapNormal() throws Exception {
        ParameterizedSqlPStatement statement = dbConnection.prepareParameterizedSqlStatement(
                "SELECT * FROM TIMEOUT_TEST WHERE COL1 = :col1");
        Thread.sleep(500);
        SqlResultSet result = statement.retrieve(1, 1, new HashMap<String, Object>() {{
            put("col1", "1");
        }});
        assertThat(result.size(), is(1));
    }

    /** SQL実行前にタイムアウトしていた場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testRetrieveMapPreTimeout() throws Exception {
        ParameterizedSqlPStatement statement = dbConnection.prepareParameterizedSqlStatement(
                "SELECT * FROM TIMEOUT_TEST WHERE COL1 = :col1");
        Thread.sleep(3000);
        statement.retrieve(new HashMap<String, Object>() {{
            put("col1", "1");
        }});
    }

    /** SQL実行中にタイムアウトした場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testRetrieveMapAfterTimeout() throws Exception {

        new TransactionTimeoutCaseExecutor() {
            @Override
            void execute(BasicDbConnection connection) {
                ParameterizedSqlPStatement statement = connection.prepareParameterizedSqlStatement(
                        "UPDATE TIMEOUT_TEST SET COL1 = COL1 WHERE COL1 = :col1");
                statement.executeUpdateByMap(new HashMap<String, Object>() {{
                    put("col1", "1");
                }});
            }
        }.executeTestCase(dbConnection);
    }

    //******************************************************************************************************************
    // executeQueryのテスト
    //******************************************************************************************************************
    /** トランザクションタイムアウトが発生しない場合 */
    @Test
    public void testExecuteQueryNormal() throws Exception {
        SqlPStatement statement = dbConnection.prepareStatement("SELECT * FROM TIMEOUT_TEST");
        Thread.sleep(500);
        ResultSetIterator result = statement.executeQuery();
        assertThat(result.next(), is(true));
        assertThat(result.next(), is(false));
    }

    /** SQL実行前にタイムアウトしていた場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testExecuteQueryPreTimeout() throws Exception {
        SqlPStatement statement = dbConnection.prepareStatement("SELECT * FROM TIMEOUT_TEST");
        Thread.sleep(3000);
        statement.executeQuery();
    }

    /** SQL実行中にタイムアウトした場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testExecuteQueryAfterTimeout() throws Exception {
        new TransactionTimeoutCaseExecutor() {
            @Override
            void execute(BasicDbConnection connection) {
                SqlPStatement statement = connection.prepareStatement(
                        "UPDATE TIMEOUT_TEST SET COL1 = COL1 WHERE COL1 = '1'");
                statement.execute();
                statement.close();
            }
        }.executeTestCase(dbConnection);
    }

    //******************************************************************************************************************
    // Objectを条件に取るexecuteQueryのテスト
    //******************************************************************************************************************
    /** トランザクションタイムアウトが発生しない場合 */
    @Test
    public void testExecuteQueryObjectNormal() throws Exception {
        ParameterizedSqlPStatement statement = dbConnection.prepareParameterizedSqlStatement(
                "SELECT * FROM TIMEOUT_TEST WHERE COL1 = :col1");
        Thread.sleep(500);
        ResultSetIterator result = statement.executeQueryByObject(new TimeoutTestEntity("1"));
        assertThat(result.next(), is(true));
        assertThat(result.next(), is(false));
    }

    /** SQL実行前にタイムアウトしていた場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testExecuteQueryObjectPreTimeout() throws Exception {
        ParameterizedSqlPStatement statement = dbConnection.prepareParameterizedSqlStatement(
                "SELECT * FROM TIMEOUT_TEST WHERE COL1 = :col1");
        Thread.sleep(3000);
        statement.executeQueryByObject(new TimeoutTestEntity("1"));
    }

    /** SQL実行中にタイムアウトした場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testExecuteQueryObjectAfterTimeout() throws Exception {
        new TransactionTimeoutCaseExecutor() {
            @Override
            void execute(BasicDbConnection connection) {
                ParameterizedSqlPStatement statement = connection.prepareParameterizedSqlStatement(
                        "UPDATE TIMEOUT_TEST SET COL1 = COL1 WHERE COL1 = :col1");
                statement.executeUpdateByObject(new TimeoutTestEntity("1"));
            }
        }.executeTestCase(dbConnection);
    }

    //******************************************************************************************************************
    // Objectを条件に取るexecuteQueryのテスト
    //******************************************************************************************************************
    /** トランザクションタイムアウトが発生しない場合 */
    @Test
    public void testExecuteQueryMapNormal() throws Exception {
        ParameterizedSqlPStatement statement = dbConnection.prepareParameterizedSqlStatement(
                "SELECT * FROM TIMEOUT_TEST WHERE COL1 = :key1");
        Thread.sleep(500);
        ResultSetIterator result = statement.executeQueryByMap(new HashMap<String, Object>() {
            {
                put("key1", "1");
            }
        });
        assertThat(result.next(), is(true));
        assertThat(result.next(), is(false));
    }

    /** SQL実行前にタイムアウトしていた場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testExecuteQueryMapPreTimeout() throws Exception {
        ParameterizedSqlPStatement statement = dbConnection.prepareParameterizedSqlStatement(
                "SELECT * FROM TIMEOUT_TEST WHERE COL1 = :key1");
        Thread.sleep(3000);
        statement.executeQueryByMap(new HashMap<String, Object>() {{
            put("key1", "1");
        }});
    }

    /** SQL実行中にタイムアウトした場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testExecuteQueryMapAfterTimeout() throws Exception {
        new TransactionTimeoutCaseExecutor() {
            @Override
            void execute(BasicDbConnection connection) {
                ParameterizedSqlPStatement statement = connection.prepareParameterizedSqlStatement(
                        "UPDATE TIMEOUT_TEST SET COL1 = COL1 WHERE COL1 = :col1");
                statement.executeUpdateByMap(new HashMap<String, Object>() {
                    {
                        put("col1", "1");
                    }
                });
            }
        }.executeTestCase(dbConnection);
    }

    //******************************************************************************************************************
    // executeUpdateのテスト
    //******************************************************************************************************************
    /** トランザクションタイムアウトが発生しない場合 */
    @Test
    public void testExecuteUpdateNormal() throws Exception {
        SqlPStatement statement = dbConnection.prepareStatement("UPDATE TIMEOUT_TEST SET COL1 = '1'");
        Thread.sleep(500);
        int result =  statement.executeUpdate();
        assertThat(result, is(1));
    }

    /** SQL実行前にタイムアウトしていた場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testExecuteUpdatePreTimeout() throws Exception {
        SqlPStatement statement = dbConnection.prepareStatement("UPDATE TIMEOUT_TEST SET COL1 = '1'");
        Thread.sleep(3000);
        statement.executeUpdate();
    }

    /** SQL実行中にタイムアウトした場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testExecuteUpdateAfterTimeout() throws Exception {
        new TransactionTimeoutCaseExecutor() {
            @Override
            void execute(BasicDbConnection connection) {
                SqlPStatement statement = connection.prepareStatement(
                        "UPDATE TIMEOUT_TEST SET COL1 = COL1 WHERE COL1 = '1'");
                statement.executeUpdate();

            }
        }.executeTestCase(dbConnection);
    }

    //******************************************************************************************************************
    // Objectを条件にとるexecuteUpdateのテスト
    //******************************************************************************************************************
    /** トランザクションタイムアウトが発生しない場合 */
    @Test
    public void testExecuteUpdateObjectNormal() throws Exception {
        ParameterizedSqlPStatement statement = dbConnection.prepareParameterizedSqlStatement(
                "UPDATE TIMEOUT_TEST SET COL1 = '1' WHERE COL1 = :col1");
        Thread.sleep(500);
        int result = statement.executeUpdateByObject(new TimeoutTestEntity("1"));
        assertThat(result, is(1));
    }

    /** SQL実行前にタイムアウトしていた場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testExecuteUpdateObjectPreTimeout() throws Exception {
        ParameterizedSqlPStatement statement = dbConnection.prepareParameterizedSqlStatement(
                "UPDATE TIMEOUT_TEST SET COL1 = '1' WHERE COL1 = :col1");
        Thread.sleep(3000);
        statement.executeUpdateByObject(new TimeoutTestEntity("1"));
    }

    /** SQL実行中にタイムアウトした場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testExecuteUpdateObjectAfterTimeout() throws Exception {
        new TransactionTimeoutCaseExecutor() {
            @Override
            void execute(BasicDbConnection connection) {
                ParameterizedSqlPStatement statement = connection.prepareParameterizedSqlStatement(
                        "UPDATE TIMEOUT_TEST SET COL1 = '1' WHERE COL1 = :col1");
                statement.executeUpdateByObject(new TimeoutTestEntity("1"));
            }
        }.executeTestCase(dbConnection);
    }

    //******************************************************************************************************************
    // Mapを条件にとるexecuteUpdateのテスト
    //******************************************************************************************************************
    /** トランザクションタイムアウトが発生しない場合 */
    @Test
    public void testExecuteUpdateMapNormal() throws Exception {
        ParameterizedSqlPStatement statement = dbConnection.prepareParameterizedSqlStatement(
                "UPDATE TIMEOUT_TEST SET COL1 = '1' WHERE COL1 = :col1");
        Thread.sleep(500);
        int result = statement.executeUpdateByMap(new HashMap<String, Object>() {
            {
                put("col1", "1");
            }
        });
        assertThat(result, is(1));
    }

    /** SQL実行前にタイムアウトしていた場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testExecuteUpdateMapPreTimeout() throws Exception {
        ParameterizedSqlPStatement statement = dbConnection.prepareParameterizedSqlStatement(
                "UPDATE TIMEOUT_TEST SET COL1 = '1' WHERE COL1 = :col1");
        Thread.sleep(3000);
        statement.executeUpdateByMap(new HashMap<String, Object>() {
            {
                put("col1", "1");
            }
        });
    }

    /** SQL実行中にタイムアウトした場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testExecuteUpdateMapAfterTimeout() throws Exception {
        new TransactionTimeoutCaseExecutor() {
            @Override
            void execute(BasicDbConnection connection) {
                ParameterizedSqlPStatement statement = connection.prepareParameterizedSqlStatement(
                        "UPDATE TIMEOUT_TEST SET COL1 = '1' WHERE COL1 = :col1");
                statement.executeUpdateByMap(new HashMap<String, Object>() {
                    {
                        put("col1", "1");
                    }
                });
            }
        }.executeTestCase(dbConnection);
    }

    //******************************************************************************************************************
    // executeのテスト
    //******************************************************************************************************************
    /** トランザクションタイムアウトが発生しない場合 */
    @Test
    public void testExecuteNormal() throws Exception {
        SqlPStatement statement = dbConnection.prepareStatement("UPDATE TIMEOUT_TEST SET COL1 = '1'");
        Thread.sleep(500);
        boolean result =  statement.execute();
        assertThat(result, is(false));
        assertThat(statement.getUpdateCount(), is(1));
    }

    /** SQL実行前にタイムアウトしていた場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testExecutePreTimeout() throws Exception {
        SqlPStatement statement = dbConnection.prepareStatement("UPDATE TIMEOUT_TEST SET COL1 = '1'");
        Thread.sleep(3000);
        statement.execute();
    }

    /** SQL実行中にタイムアウトした場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testExecuteAfterTimeout() throws Exception {
        new TransactionTimeoutCaseExecutor() {
            @Override
            void execute(BasicDbConnection connection) {
                SqlPStatement statement = connection.prepareStatement(
                        "UPDATE TIMEOUT_TEST SET COL1 = '1' WHERE COL1 = '1'");
                statement.execute();
            }
        }.executeTestCase(dbConnection);
    }

    //******************************************************************************************************************
    // executeBatchのテスト
    //******************************************************************************************************************
    /** トランザクションタイムアウトが発生しない場合 */
    @Test
    public void testExecuteBatchNormal() throws Exception {
        SqlPStatement statement = dbConnection.prepareStatement("UPDATE TIMEOUT_TEST SET COL1 = '1'");
        statement.addBatch();
        statement.addBatch();
        Thread.sleep(500);
        int[] result =  statement.executeBatch();
        assertThat(result.length, is(2));
    }

    /** SQL実行前にタイムアウトしていた場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testExecuteBatchPreTimeout() throws Exception {
        SqlPStatement statement = dbConnection.prepareStatement("UPDATE TIMEOUT_TEST SET COL1 = '1'");
        statement.addBatch();
        statement.addBatch();
        Thread.sleep(3000);
        statement.executeBatch();
    }

    /** SQL実行中にタイムアウトした場合 */
    @Test(expected = TransactionTimeoutException.class)
    @TargetDb(exclude = TargetDb.Db.DB2)
    public void testExecuteBatchAfterTimeout() throws Exception {
        new TransactionTimeoutCaseExecutor() {
            @Override
            void execute(BasicDbConnection connection) {
                SqlPStatement statement = connection.prepareStatement(
                        "UPDATE TIMEOUT_TEST SET COL1 = '1' WHERE COL1 = '1'");
                statement.addBatch();
                statement.executeBatch();
            }
        }.executeTestCase(dbConnection);
    }
    
    /** SQL実行中にタイムアウトした場合 */
    @Test(expected = SqlStatementException.class)
    @TargetDb(include = TargetDb.Db.DB2)
    public void testExecuteBatchAfterTimeout_db2() throws Exception {
        new TransactionTimeoutCaseExecutor() {
            @Override
            void execute(BasicDbConnection connection) {
                SqlPStatement statement = connection.prepareStatement(
                        "UPDATE TIMEOUT_TEST SET COL1 = '1' WHERE COL1 = '1'");
                statement.addBatch();
                statement.executeBatch();
            }
        }.executeTestCase(dbConnection);
    }

    //******************************************************************************************************************
    // Objectを引数にとるexecuteBatchのテスト
    //******************************************************************************************************************
    /** トランザクションタイムアウトが発生しない場合 */
    @Test
    public void testExecuteBatchObjectNormal() throws Exception {
        ParameterizedSqlPStatement statement = dbConnection.prepareParameterizedSqlStatement(
                "UPDATE TIMEOUT_TEST SET COL1 = COL1 WHERE COL1 = :col1");
        statement.addBatchObject(new TimeoutTestEntity("1"));
        statement.addBatchObject(new TimeoutTestEntity("2"));
        Thread.sleep(500);
        int[] result =  statement.executeBatch();
        assertThat(result.length, is(2));
    }

    /** SQL実行前にタイムアウトしていた場合 */
    @Test(expected = TransactionTimeoutException.class)
    public void testExecuteBatchObjectPreTimeout() throws Exception {
        ParameterizedSqlPStatement statement = dbConnection.prepareParameterizedSqlStatement(
                "UPDATE TIMEOUT_TEST SET COL1 = COL1 WHERE COL1 = :col1");
        statement.addBatchObject(new TimeoutTestEntity("1"));
        statement.addBatchObject(new TimeoutTestEntity("2"));
        Thread.sleep(3000);
        statement.executeBatch();
    }

    /** SQL実行中にタイムアウトした場合 */
    @Test(expected = TransactionTimeoutException.class)
    @TargetDb(exclude = TargetDb.Db.DB2)
    public void testExecuteBatchObjectAfterTimeout() throws Exception {
    	new TransactionTimeoutCaseExecutor() {
           	@Override
           	void execute(BasicDbConnection connection) {
           		ParameterizedSqlPStatement statement = connection.prepareParameterizedSqlStatement(
               			"UPDATE TIMEOUT_TEST SET COL1 = COL1 WHERE COL1 = :col1");
               	statement.addBatchObject(new TimeoutTestEntity("1"));
               	statement.executeBatch();
           	}
        }.executeTestCase(dbConnection);
    }
    
    /**
     * SQL実行中にタイムアウトした場合(db2の場合)
     *
     * db2の場合、executeBatch時にqueryTimeout値はサポートされないので、
     * ロックタイムアウトの例外が発生する。
     */
    @Test(expected = SqlStatementException.class)
    @TargetDb(include = TargetDb.Db.DB2)
    public void testExecuteBatchObjectAfterTimeout_db2() throws Exception {
        new TransactionTimeoutCaseExecutor() {
            @Override
            void execute(BasicDbConnection connection) {
                ParameterizedSqlPStatement statement = connection.prepareParameterizedSqlStatement(
                        "UPDATE TIMEOUT_TEST SET COL1 = COL1 WHERE COL1 = :col1");
                statement.addBatchObject(new TimeoutTestEntity("1"));
                statement.executeBatch();
            }
        }.executeTestCase(dbConnection);
    }

    private abstract static class TransactionTimeoutCaseExecutor {

        abstract void execute(BasicDbConnection connection);

        void executeTestCase(final BasicDbConnection connection) throws Exception {
            final Connection lockConnection = VariousDbTestHelper.getNativeConnection();
            lockConnection.setAutoCommit(false);
            try {
                final PreparedStatement st = lockConnection.prepareStatement(
                        "UPDATE TIMEOUT_TEST SET COL1 = COL1 WHERE COL1 = '1'");
                st.executeUpdate();

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

    @Entity
    @Table(name = "TIMEOUT_TEST")
    public static class TimeoutTestEntity {

        @Id
        @Column(name = "col1")
        public String col1;

        public TimeoutTestEntity() {
        }

        public TimeoutTestEntity(String col1) {
            this.col1 = col1;
        }

        public String getCol1() {
            return col1;
        }
    }
}
