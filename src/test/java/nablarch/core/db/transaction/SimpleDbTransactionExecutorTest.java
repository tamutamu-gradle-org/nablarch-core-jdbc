package nablarch.core.db.transaction;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.transaction.TransactionFactory;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import nablarch.test.support.log.app.OnMemoryLogWriter;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link SimpleDbTransactionExecutor}のテスト。
 *
 * @author hisaaki sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class SimpleDbTransactionExecutorTest {

    /** テストターゲット */
    private SimpleDbTransactionManager transaction;

    @ClassRule
    public static SystemRepositoryResource repository = new SystemRepositoryResource("nablarch/core/db/transaction/SimpleDbTransactionExecutorTest.xml");

    private static final String ID = "00001";

    private static final String INIT_VALUE = "初期値";

    private static final String UPDATED_VALUE = "更新済み";

    @BeforeClass
    public static void beforeClass() {
        VariousDbTestHelper.createTable(TestEntity.class);
    }

    /** データベースのセットアップとLogのクリアを行う */
    @Before
    public void setUp() {
        VariousDbTestHelper.setUpTable(TestEntity.create(ID, INIT_VALUE));
        OnMemoryLogWriter.clear();
        transaction = repository.getComponent("transactionManager");
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul><li>更新した内容が正常にコミットされていること</li></ul>
     */
    @Test
    public void testDoTransactionSuccess() {
        // ターゲットの呼び出し
        Integer updateCnt = new SimpleDbTransactionExecutorSub(transaction)
                .doTransaction();
        assertThat(updateCnt, is(1));

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されること", entity.col2, is(UPDATED_VALUE));
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にRuntimeExceptionが発生する。例外発生前に更新された内容はロールバックされること。</li>
     * <li>呼び出し元には、発生した例外が送出されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ExceptionAtExecute() {
        // ターゲットの呼び出し
        try {
            new SimpleDbTransactionExecutorRuntimeException(transaction)
                    .doTransaction();
            fail("does not run.");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("runtime exception."));
        }
        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にErrorが発生する。例外発生前に更新された内容はロールバックされること。</li>
     * <li>呼び出し元には、発生した例外が送出されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ErrorAtExecute() {
        // ターゲットの呼び出し
        try {
            new SimpleDbTransactionExecutorError(transaction)
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is("Error."));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にRuntimeExceptionが発生し、ロールバック時に再度RuntimeExceptionが発生する場合</li>
     * <li>呼び出し元には、ロールバック時に発生した例外が送出されること。</li>
     * <li>SQL実行時に発生したRuntimeExceptionは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ExceptionAtExecuteAndRollback() {
        // ターゲットの呼び出し
        try {
            new SimpleDbTransactionExecutorRuntimeException(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_RUNTIME))
                    .doTransaction();
            fail("does not run.");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("rollback runtime error."));
        }

        // 更新されていないことをアサート
        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLog("java.lang.RuntimeException.*runtime exception");
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にRuntimeExceptionが発生し、ロールバック時にはErrorが発生する場合</li>
     * <li>呼び出し元には、ロールバック時に発生した例外が送出されること。</li>
     * <li>SQL実行時に発生したRuntimeExceptionは、ワーニングレベルでログ出y力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ExceptionAtExecute_ErrorAtRollback() {
        // ターゲットの呼び出し
        try {
            new SimpleDbTransactionExecutorRuntimeException(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_ERROR))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is("rollback Error."));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLog("java.lang.RuntimeException.*runtime exception");
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にErrorが発生し、ロールバック時にはRuntimeExceptionが発生する場合</li>
     * <li>呼び出し元には、ロールバック時に発生した例外が送出されること。</li>
     * <li>SQL実行時のErrorは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ErrorAtExecute_ExceptionAtRollback() {
        // ターゲットの呼び出し
        try {
            new SimpleDbTransactionExecutorError(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_RUNTIME))
                    .doTransaction();
            fail("does not run.");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("rollback runtime error."));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLog("java.lang.Error.*Error");
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にErrorが発生し、ロールバック時にはErrorが発生する場合</li>
     * <li>呼び出し元には、ロールバック時に発生した例外が送出されること。</li>
     * <li>SQL実行時のErrorは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ErrorAtExecuteAndRollback() {
        try {
            new SimpleDbTransactionExecutorError(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_ERROR))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is("rollback Error."));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLog("java.lang.Error.*Error");
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にRuntimeExceptionが発生し、トランザクションの終了時にはRuntimeExceptionが発生する場合</li>
     * <li>呼び出し元には、トランザクション終了時に発生した例外が送出されること。</li>
     * <li>SQL実行時のRuntimeExceptionは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ExceptionAtExecuteAndEnd() {
        // ターゲットの呼び出し
        try {
            new SimpleDbTransactionExecutorRuntimeException(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_RUNTIME))
                    .doTransaction();
            fail("does not run.");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("end runtime error."));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLog("java.lang.RuntimeException.*runtime exception");
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にRuntimeExceptionが発生し、トランザクションの終了時にはErrorが発生する場合</li>
     * <li>呼び出し元には、トランザクション終了時に発生した例外が送出されること。</li>
     * <li>SQL実行時のRuntimeExceptionは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ExceptionAtExecute_ErrorAtEnd() {
        try {
            new SimpleDbTransactionExecutorRuntimeException(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_ERROR))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is("end Error."));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLog("java.lang.RuntimeException.*runtime exception");
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にErrorが発生し、トランザクションの終了時にはRuntimeExceptionが発生する場合</li>
     * <li>呼び出し元には、トランザクション終了時に発生した例外が送出されること。</li>
     * <li>SQL実行時のErrorは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ErrorAtExecute_ExceptionAtEnd() {
        // ターゲットの呼び出し
        try {
            new SimpleDbTransactionExecutorError(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_RUNTIME))
                    .doTransaction();
            fail("does not run.");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("end runtime error."));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLog("java.lang.Error.*Error");
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にErrorが発生し、トランザクションの終了時にはErrorが発生する場合</li>
     * <li>呼び出し元には、トランザクション終了時に発生した例外が送出されること。</li>
     * <li>SQL実行時のErrorは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ErrorAtExecuteAndEnd() {
        try {
            new SimpleDbTransactionExecutorError(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_ERROR))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is("end Error."));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLog("java.lang.Error.*Error");
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にRuntimeExceptionが発生し、ロールバックとトランザクションの終了時にはRuntimeExceptionが発生する場合</li>
     * <li>呼び出し元には、トランザクション終了時に発生した例外が送出されること。</li>
     * <li>SQL実行時とロールバック時のRuntimeExceptionは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ExceptionAtExecuteAndRollbackAndEnd() {
        try {
            new SimpleDbTransactionExecutorRuntimeException(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_RUNTIME,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_RUNTIME))
                    .doTransaction();
            fail("does not run.");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("end runtime error."));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLog("java.lang.RuntimeException.*runtime exception");
        assertWarnLog("rollback runtime error");
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にErrorが発生し、ロールバックとトランザクションの終了時にはRuntimeExceptionが発生する場合</li>
     * <li>呼び出し元には、トランザクション終了時に発生した例外が送出されること。</li>
     * <li>SQL実行時とロールバック時のRuntimeExceptionは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ErrorAtExecute_ExceptionAtRollbackAndEnd() {
        try {
            new SimpleDbTransactionExecutorError(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_RUNTIME,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_RUNTIME))
                    .doTransaction();
            fail("does not run.");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("end runtime error."));
        }


        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLog("java.lang.Error.*Error");
        assertWarnLog("rollback runtime error");
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>SQL実行時にRuntimeExceptionが発生し、ロールバックとトランザクションの終了時にはErrorが発生する場合</li>
     * <li>呼び出し元には、トランザクション終了時に発生した例外が送出されること。</li>
     * <li>SQL実行時とロールバック時のRuntimeExceptionは、ワーニングレベルでログ出力されること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ExceptionAtExecute_ErrorAtRollbackAndEnd() {
        try {
            new SimpleDbTransactionExecutorRuntimeException(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.ROLLBACK_ERROR,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_ERROR))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is("end Error."));
        }


        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("更新されていないこと", entity.col2, is(INIT_VALUE));

        // ログのアサート
        assertWarnLog("java.lang.RuntimeException.*runtime exception");
        assertWarnLog("rollback Error");
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>トランザクションの終了時にはRuntimeExceptionが発生する場合</li>
     * <li>呼び出し元には、トランザクション終了時に発生した例外が送出されること。</li>
     * <li>コミットは成功しているので、更新処理が反映されていること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ExceptionAtEnd() {
        try {
            new SimpleDbTransactionExecutorSub(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_RUNTIME))
                    .doTransaction();
            fail("does not run.");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("end runtime error."));
        }


        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("コミットは成功しているので、更新処理が正しく行われていること", entity.col2, is(UPDATED_VALUE));

        // ログのアサート
        // ログは、出力されていないこと。
        final List<String> messages = OnMemoryLogWriter.getMessages("writer.memory");
        int warnCount = 0;
        for (String message : messages) {
            if (message.contains("WARN")) {
                warnCount++;
            }
        }
        assertThat(warnCount, is(0));
    }

    /**
     * {@link SimpleDbTransactionExecutor#doTransaction()}のテスト。
     * <br/>
     * <h2>テスト内容</h2>
     * <ul>
     * <li>トランザクションの終了時にはErrorが発生する場合</li>
     * <li>呼び出し元には、トランザクション終了時に発生した例外が送出されること。</li>
     * <li>コミットは成功しているので、更新処理が反映されていること。</li>
     * </ul>
     */
    @Test
    public void testDoTransaction_ErrorAtEnd() {
        try {
            new SimpleDbTransactionExecutorSub(
                    new SimpleDbTransactionManagerError(
                            transaction,
                            SimpleDbTransactionManagerError.ERROR_STATEMENT.END_ERROR))
                    .doTransaction();
            fail("does not run.");
        } catch (Error e) {
            assertThat(e.getMessage(), is("end Error."));
        }

        TestEntity entity = VariousDbTestHelper.findById(TestEntity.class, ID);
        assertThat("コミットは成功しているので、更新処理が正しく行われていること", entity.col2, is(UPDATED_VALUE));

        // ログのアサート
        // ログは、出力されていないこと。
        final List<String> messages = OnMemoryLogWriter.getMessages("writer.memory");
        int warnCount = 0;
        for (String message : messages) {
            if (message.contains("WARN")) {
                warnCount++;
            }
        }
        assertThat(warnCount, is(0));
    }

    /**
     * ワーニングログをアサートする。
     *
     * @param message ログのメッセージ
     */
    private static void assertWarnLog(String message) {
        List<String> log = OnMemoryLogWriter.getMessages("writer.memory");
        boolean writeLog = false;
        for (String logMessage : log) {
            String str = logMessage.replaceAll("\\r|\\n", "");
            if (str.matches(
                    "^.*WARN.*failed in the "
                            + "application process\\..*" + message + ".*$")) {
                writeLog = true;
            }
        }
        assertThat("元例外がWARNレベルでログに出力されていること", writeLog, is(true));
    }

    /**
     * create table sbt_test_table (
     *      col1 char(5),
     *      col2 varchar2(100)
     * )
     *
     */
    @Entity
    @Table(name="SBT_TEST_TABLE")
    public static class TestEntity {
        @Id
        @Column(name="col1", length=5, nullable = false)
        public String col1;

        @Column(name="col2", length=100)
        public String col2;

        private static TestEntity create(String col1, String col2) {
            TestEntity entity = new TestEntity();
            entity.col1 = col1;
            entity.col2 = col2;
            return entity;
        }
    }

    /**
     * エラーを発生させる{@link SimpleDbTransactionManager}のサブクラス。
     */
    private static class SimpleDbTransactionManagerError extends SimpleDbTransactionManager {

        enum ERROR_STATEMENT {
            ROLLBACK_RUNTIME,
            ROLLBACK_ERROR,
            END_RUNTIME,
            END_ERROR
        }

        private final SimpleDbTransactionManager transactionManager;

        private final ERROR_STATEMENT[] errorState;

        private SimpleDbTransactionManagerError(
                SimpleDbTransactionManager transactionManager,
                ERROR_STATEMENT... errorState) {
            this.transactionManager = transactionManager;
            this.errorState = errorState;
        }

        private boolean is(ERROR_STATEMENT[] errors, ERROR_STATEMENT error) {
            for (ERROR_STATEMENT statement : errors) {
                if (statement == error) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void beginTransaction() {
            transactionManager.beginTransaction();
        }

        @Override
        public void commitTransaction() {
            transactionManager.commitTransaction();
        }

        @Override
        public void rollbackTransaction() {
            transactionManager.rollbackTransaction();
            if (is(errorState, ERROR_STATEMENT.ROLLBACK_RUNTIME)) {
                throw new RuntimeException("rollback runtime error.");
            } else if (is(errorState, ERROR_STATEMENT.ROLLBACK_ERROR)) {
                throw new Error("rollback Error.");
            }
        }

        @Override
        public void endTransaction() {
            transactionManager.endTransaction();
            if (is(errorState, ERROR_STATEMENT.END_RUNTIME)) {
                throw new RuntimeException("end runtime error.");
            } else if (is(errorState, ERROR_STATEMENT.END_ERROR)) {
                throw new Error("end Error.");
            }
        }

        @Override
        public void setConnectionFactory(ConnectionFactory connectionFactory) {
            transactionManager.setConnectionFactory(connectionFactory);
        }

        @Override
        public void setTransactionFactory(
                TransactionFactory transactionFactory) {
            transactionManager.setTransactionFactory(transactionFactory);
        }

        @Override
        public void setDbTransactionName(String dbTransactionName) {
            transactionManager.setDbTransactionName(dbTransactionName);
        }

        @Override
        public String getDbTransactionName() {
            return transactionManager.getDbTransactionName();
        }
    }

    /**
     * テスト用の{@link SimpleDbTransactionExecutor}実装クラス。
     */
    private static class SimpleDbTransactionExecutorSub extends SimpleDbTransactionExecutor<Integer> {

        /**
         * コンストラクタ。
         *
         * @param transactionManager トランザクションマネージャ
         */
        public SimpleDbTransactionExecutorSub(
                SimpleDbTransactionManager transactionManager) {
            super(transactionManager);
        }

        @Override
        public Integer execute(AppDbConnection connection) {
            SqlPStatement statement = connection.prepareStatement(
                    "update sbt_test_table set col2 = ? where col1 = ?");
            statement.setString(1, UPDATED_VALUE);
            statement.setString(2, ID);
            return statement.executeUpdate();
        }
    }

    /**
     * テスト用の{@link SimpleDbTransactionExecutor}実装クラス。
     * <br/>
     * 本クラスは、更新用SQL実行後に、RuntimeExceptionを送出する。
     */
    private static class SimpleDbTransactionExecutorRuntimeException extends SimpleDbTransactionExecutorSub {

        /**
         * コンストラクタ。
         *
         * @param transactionManager トランザクションマネージャ
         */
        public SimpleDbTransactionExecutorRuntimeException(
                SimpleDbTransactionManager transactionManager) {
            super(transactionManager);
        }

        @Override
        public Integer execute(AppDbConnection connection) {
            super.execute(connection);
            // エラーを発生させる。
            throw new RuntimeException("runtime exception.");
        }
    }

    /**
     * テスト用の{@link SimpleDbTransactionExecutor}実装クラス。
     * <br/>
     * 本クラスは、更新用SQL実行後に、Errorを送出する。
     */
    private static class SimpleDbTransactionExecutorError extends SimpleDbTransactionExecutorSub {

        /**
         * コンストラクタ。
         *
         * @param transactionManager トランザクションマネージャ
         */
        public SimpleDbTransactionExecutorError(
                SimpleDbTransactionManager transactionManager) {
            super(transactionManager);
        }

        @Override
        public Integer execute(AppDbConnection connection) {
            super.execute(connection);
            // エラーを発生させる。
            throw new Error("Error.");
        }
    }
}
