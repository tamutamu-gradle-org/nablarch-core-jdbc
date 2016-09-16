package nablarch.core.db.transaction;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.containsString;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import mockit.NonStrictExpectations;
import mockit.Verifications;

/**
 * {@link SimpleDbTransactionManager}のテストクラス。
 *
 * @author Hisaaki Sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class SimpleDbTransactionManagerTest {

    // テスト開始前にトランザクションを開始しているTransactionManager
    private static final String START_TRANSACTION_BEFORE_TEST = "transactionManager";
    private SimpleDbTransactionManager startBeforeTest;

    @ClassRule
    public static SystemRepositoryResource container = new SystemRepositoryResource("nablarch/core/db/transaction/SimpleDbTransactionManagerTest.xml");

    @BeforeClass
    public static void setUpClass() throws Exception {
        VariousDbTestHelper.createTable(TestEntity.class);
    }

    @Before
    public void before() throws SQLException {
        startBeforeTest = container.getComponent(START_TRANSACTION_BEFORE_TEST);
        startBeforeTest.beginTransaction();
        VariousDbTestHelper.delete(TestEntity.class);
    }

    @After
    public void after() {
        startBeforeTest.endTransaction();
    }

    /**
     * {@link SimpleDbTransactionManager#beginTransaction()} の正常系テスト。
     *
     * @throws SQLException
     */
    @Test
    public void beginTransaction() throws SQLException {
        SimpleDbTransactionManager dbTransactionManager = new SimpleDbTransactionManager();
        ConnectionFactory connectionFactory = container.getComponent("connectionFactory");
        dbTransactionManager.setConnectionFactory(connectionFactory);
        dbTransactionManager.setTransactionFactory(new JdbcTransactionFactory());

        // トランザクションの開始
        dbTransactionManager.beginTransaction();
        try {
            // コンテキストからコネクションが取得で切ること。
            AppDbConnection dbConnection = DbConnectionContext.getConnection(
                    dbTransactionManager.getDbTransactionName());

            // コネクションが取得できる事を確認
            assertThat(dbConnection, instanceOf(AppDbConnection.class));

        } finally {
            dbTransactionManager.endTransaction();
        }
    }

    /**
     * {@link SimpleDbTransactionManager#beginTransaction()} の異常系テスト。
     *
     * @throws SQLException
     */
    @Test
    public void beginTransactionError() throws SQLException {

        //**********************************************************************
        // トランザクションファクトリを設定しなかった場合。
        //**********************************************************************
        SimpleDbTransactionManager dbTransactionManager = new SimpleDbTransactionManager();

        ConnectionFactory connectionFactory = container.getComponentByType(ConnectionFactory.class);
        dbTransactionManager.setConnectionFactory(connectionFactory);

        try {
            dbTransactionManager.beginTransaction();
            fail("ここは通らない");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("TransactionFactory is not set."));
        }
        assertFalse(DbConnectionContext.containConnection(dbTransactionManager.getDbTransactionName()));


        //**********************************************************************
        // コネクションファクトリを設定しなかった場合。
        //**********************************************************************
        dbTransactionManager = new SimpleDbTransactionManager();
        dbTransactionManager.setTransactionFactory(new JdbcTransactionFactory());

        try {
            dbTransactionManager.beginTransaction();
            fail("ここは通らない");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("ConnectionFactory is not set."));
        }
        assertFalse(DbConnectionContext.containConnection(dbTransactionManager.getDbTransactionName()));

        //**********************************************************************
        // トランザクションのinitSqlに不正なSQLを設定
        //**********************************************************************
        dbTransactionManager = new SimpleDbTransactionManager();
        dbTransactionManager.setConnectionFactory(connectionFactory);
        JdbcTransactionFactory transactionFactory = new JdbcTransactionFactory();
        List<String> initSql = new ArrayList<String>();
        initSql.add("invalid sql");
        transactionFactory.setInitSqlList(initSql);
        dbTransactionManager.setTransactionFactory(transactionFactory);

        try {
            dbTransactionManager.beginTransaction();
            fail("ここは通らない");
        } catch (Exception e) {
            assertThat(e.getMessage(), is(
                    "transaction begin failed. dbTransactionName = " + dbTransactionManager.getDbTransactionName()));
        }
        assertFalse(DbConnectionContext.containConnection(dbTransactionManager.getDbTransactionName()));
    }

    /**
     * {@link SimpleDbTransactionManager#commitTransaction()} の正常系テスト。
     *
     * @throws Exception
     */
    @Test
    public void commitTransaction() throws Exception {
        SimpleDbTransactionManager dbTransactionManager = container.getComponent("targetTransactionManager");

        // トランザクションを開始
        dbTransactionManager.beginTransaction();
        try {
            AppDbConnection connection = DbConnectionContext.getConnection(dbTransactionManager.getDbTransactionName());
            SqlPStatement statement = connection.prepareStatement("insert into sbm_test_table values(?, ?)");
            statement.setString(1, "1");
            statement.setString(2, "12345");
            assertThat(statement.executeUpdate(), is(1));

            // 別トランザクションでDBを検索しデータが存在しないことを確認
            assertThat(VariousDbTestHelper.findAll(TestEntity.class).size(), is(0));

            // コミット後は、別トランザクションで参照できる。
            dbTransactionManager.commitTransaction();
            assertThat(VariousDbTestHelper.findAll(TestEntity.class).size(), is(1));
        } finally {
            dbTransactionManager.endTransaction();
        }

    }

    /**
     * {@link SimpleDbTransactionManager#commitTransaction()} の異常系テスト。
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void commitTransactionError() throws Exception {
        SimpleDbTransactionManager dbTransactionManager = container.getComponent("targetTransactionManager");

        // Transactionを開始せずにコミットを呼び出し
        dbTransactionManager.commitTransaction();
    }

    /**
     * {@link nablarch.core.db.transaction.SimpleDbTransactionManager#rollbackTransaction()} の正常系テスト。
     *
     * @throws Exception
     */
    @Test
    public void rollbackTransaction() throws Exception {
        SimpleDbTransactionManager dbTransactionManager = container.getComponent("targetTransactionManager");

        // トランザクションを開始
        dbTransactionManager.beginTransaction();

        try {
            AppDbConnection connection = DbConnectionContext.getConnection(dbTransactionManager.getDbTransactionName());
            SqlPStatement statement = connection.prepareStatement("insert into sbm_test_table values(?, ?)");
            statement.setString(1, "1");
            statement.setString(2, "12345");
            assertThat("1件登録されていること。", statement.executeUpdate(), is(1));

            // 同一セッションで登録されたレコードが存在している事を確認
            SqlPStatement select = connection.prepareStatement("select * from sbm_test_table where pk_col = '1'");
            assertThat("1件存在すること。", select.retrieve().size(), is(1));

            assertThat("別のセッションからは参照できない。", VariousDbTestHelper.findAll(TestEntity.class).size(), is(0));
            // ロールバック
            dbTransactionManager.rollbackTransaction();

            assertThat("rollback後はデータが存在しないこと。", select.retrieve().size(), is(0));
            assertThat("rollback後はデータが存在しないこと。", VariousDbTestHelper.findAll(TestEntity.class).size(), is(0));
        } finally {
            dbTransactionManager.endTransaction();
        }

    }

    /**
     * {@link nablarch.core.db.transaction.SimpleDbTransactionManager#rollbackTransaction()} の異常系テスト。
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void rollbackTransactionError() throws Exception {
        SimpleDbTransactionManager dbTransactionManager = container.getComponent("targetTransactionManager");

        // Transactionを開始せずにロールバックを呼び出し
        dbTransactionManager.rollbackTransaction();
    }

    /**
     * {@link SimpleDbTransactionManager#endTransaction()}の正常系テスト。
     *
     * トランザクションを終了したら、スレッドローカルが解放され
     * その後のトランザクション制御ができないことを確認。
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void endTransaction() {
        SimpleDbTransactionManager dbTransactionManager = container.getComponent("targetTransactionManager");

        // トランザクションを終了したロールバックできないこと。
        dbTransactionManager.beginTransaction();
        dbTransactionManager.endTransaction();

        dbTransactionManager.rollbackTransaction();
    }

    /**
     * 既に使用されているコネクション名が指定された場合は、既に使用されています例外が発生すること
     * また、この場合には{@link ConnectionFactory#getConnection()}が呼び出されないことを検証する。
     */
    @Test
    public void connectionNameAlreadyUsed() throws Exception {
        final ConnectionFactory connectionFactory = container.getComponent("connectionFactory");

        new NonStrictExpectations(connectionFactory) {{
            connectionFactory.getConnection("test");
        }};

        try {
            final SimpleDbTransactionManager transactionManager = container.getComponent(START_TRANSACTION_BEFORE_TEST);
            transactionManager.beginTransaction();
            fail("ここはとおらない");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(),
                    containsString("The specified database connection name is already used. connection name=[test]"));
        }

        new Verifications() {{
            // コネクション名が既に使用済みなので、新たな接続を取得しないことを検証する。
            connectionFactory.getConnection(anyString);
            times = 0;
        }};
    }

    /**
     * create table sbm_test_table (
     *    pk_col char(1) not null,
     *    val_col varchar2(100),
     *    primary key(pk_col)
     * )
     */
    @Entity
    @Table(name="sbm_test_table")
    public static class TestEntity {
        @Id
        @Column(name="pk_col", length=1, nullable=false)
        public String pk;
        @Column(name="val_col", length=100)
        public String val;
    }
}
