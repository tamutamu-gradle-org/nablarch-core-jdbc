package nablarch.core.db.statement;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.transaction.JdbcTransactionFactory;
import nablarch.core.repository.SystemRepository;
import nablarch.core.transaction.Transaction;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.transaction.TransactionTimeoutException;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.log.app.OnMemoryLogWriter;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import mockit.Deencapsulation;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;

/**
 * {@link BasicSqlCStatement}のトランザクションタイムアウトに関連したテスト。
 */
@RunWith(DatabaseTestRunner.class)
@TargetDb(include = TargetDb.Db.ORACLE)
public class BasicSqlCStatementTestWithTransactionTimeout {

    @ClassRule
    public static final SystemRepositoryResource SYSTEM_REPOSITORY_RESOURCE = new SystemRepositoryResource("db-default.xml");

    /** テストで使用するデータベース接続 */
    private TransactionManagerConnection testConnection;

    private SqlCStatement sut;

    @BeforeClass
    public static void setUpClass() throws Exception {
        DbConnectionContext.removeConnection();
    }

    @Before
    public void setUp() throws Exception {
        OnMemoryLogWriter.clear();

        ConnectionFactory connectionFactory = SystemRepository.get("connectionFactory");
        testConnection = connectionFactory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        DbConnectionContext.setConnection(testConnection);

        JdbcTransactionFactory transactionFactory = SystemRepository.get("jdbcTransactionFactory");
        transactionFactory.setTransactionTimeoutSec(1);
        final Transaction transaction = transactionFactory.getTransaction(
                TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        TransactionContext.setTransaction(
                TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY,
                transaction);

        transaction.begin();
    }

    @After
    public void tearDown() throws Exception {
        TransactionContext.removeTransaction();
        DbConnectionContext.removeConnection();
        testConnection.terminate();
        System.out.println("BasicSqlCStatementTestWithTransactionTimeout.tearDown");
    }

    /**
     * トランザクションタイムアウトしないケース
     *
     * @throws Exception
     */
    @Test
    public void noTimeout() throws Exception {
        sut = DbConnectionContext.getConnection()
                .prepareCall("BEGIN ? := 'ok'; END;");
        sut.registerOutParameter(1, Types.VARCHAR);

        Thread.sleep(900);
        sut.execute();
        assertThat("結果が取れること", sut.getString(1), is("ok"));
    }

    /**
     * ストアド実行前にタイムアウト時間に達していた場合、タイムアウトエラーとなること。
     *
     * @throws Exception
     */
    @Test(expected = TransactionTimeoutException.class)
    public void timeoutPreExec() throws Exception {
        sut = DbConnectionContext.getConnection()
                .prepareCall("BEGIN NULL; END;");

        Thread.sleep(2000);
        sut.execute();
    }

    /**
     * タイムアウトを示す任意のエラーが発生して、トランザクションタイムアウト時間を超過していた場合
     * タイムアウトエラーとなること。
     *
     * @throws Exception
     */
    @Test(expected = TransactionTimeoutException.class)
    public void timeoutTargetError(@Mocked final Connection mockConnection) throws Exception {
        new Expectations() {{
            final CallableStatement statement = mockConnection.prepareCall(anyString);
            statement.execute();
            result = new Delegate<CallableStatement>() {
                boolean execute() throws SQLException {
                    System.out.println("execute sleep...");
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    throw new SQLException("timeoutとなる例外", null, 1013);
                }
            };
        }};

        final Connection originalConnection = Deencapsulation.getField(testConnection, Connection.class);
        try {
            Deencapsulation.setField(testConnection, mockConnection);
            sut = testConnection.prepareCall("BEGIN NULL; END;");
            sut.execute();
        } finally {
            Deencapsulation.setField(testConnection, originalConnection);
        }
    }
}
