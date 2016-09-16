package nablarch.core.db.statement;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import nablarch.core.db.DbExecutionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.dialect.DefaultDialect;
import nablarch.core.transaction.TransactionContext;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import mockit.Deencapsulation;
import mockit.Mocked;

/**
 * {@link BasicStatementFactory}のテストクラス。
 *
 * @author hisaaki sioiri
 */
@RunWith(DatabaseTestRunner.class)
public abstract class BasicStatementFactoryTestLogic {


    /** SQLファイルのパス */
    private static final String SQL_FILE_PATH = "nablarch.core.db.statement.factory.sql-file";

    /** テストで使用するコネクション */
    private Connection connection;

    @BeforeClass
    public static void setupClass() {
        VariousDbTestHelper.createTable(TestEntity.class);
    }

    @Before
    public void setUp() throws Exception {
        connection = VariousDbTestHelper.getNativeConnection();
    }

    @After
    public void tearDown() throws Exception {
        connection.close();
    }

    /**
     * {@link BasicStatementFactory#getSqlPStatement(String, Connection, DbExecutionContext)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void testGetSqlPStatement() throws Exception {
        BasicStatementFactory sut = createStatementFactory();
        sut.setFetchSize("1");
        sut.setQueryTimeout(5);

        SqlPStatement statement = sut.getSqlPStatement(
                "SELECT * FROM STATEMENT_FACTORY_TEST", connection, createContext());

        assertThat("ファクトリの値がステートメントに移送されていること",
                statement.getFetchSize(), is(1));
        assertThat("ファクトリの値がステートメントに移送されていること",
                statement.getQueryTimeout(), is(5));
    }

    /** SelectOptionが正しく設定されることの確認 */
    @Test
    public void testGetSqlPStatementWithSelectOption() throws Exception {
        BasicStatementFactory sut = createStatementFactory();
        SqlPStatement statement = sut.getSqlPStatement(
                "SELECT * FROM STATEMENT_FACTORY_TEST", connection, createContext(), new SelectOption(10, 20));

        SelectOption selectOption = (SelectOption) Deencapsulation.getField(statement, "selectOption");
        assertThat(selectOption.getStartPosition(), is(10));
        assertThat(selectOption.getLimit(), is(20));
    }

    /**
     * {@link BasicStatementFactory#getSqlPStatement(String, Connection, int, DbExecutionContext)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void testGetSqlPStatementSupportAutoGenKeys() throws Exception {
        BasicStatementFactory sut = createStatementFactory();
        sut.setFetchSize("2");
        sut.setQueryTimeout(50);

        SqlPStatement statement = sut.getSqlPStatement(
                "SELECT * FROM STATEMENT_FACTORY_TEST", connection, Statement.RETURN_GENERATED_KEYS, createContext());

        assertThat("ファクトリの値がステートメントに移送されていること",
                statement.getFetchSize(), is(2));
        assertThat("ファクトリの値がステートメントに移送されていること",
                statement.getQueryTimeout(), is(50));
    }

    /**
     * {@link BasicStatementFactory#getSqlPStatement(String, Connection, int[], DbExecutionContext)}のテスト
     */
    @Test
    // PostgreSQLはindex指定の自動生成キーの取得機能に対応していないのでテストから除外する。
    @TargetDb(exclude = TargetDb.Db.POSTGRE_SQL)
    public void testGetSqlPStatementSupportAutoGenKeysByColumnIndexes() throws Exception {
        BasicStatementFactory sut = createStatementFactory();
        sut.setFetchSize("3");
        sut.setQueryTimeout(100);

        SqlPStatement sqlPStatement = sut.getSqlPStatement(
                "SELECT * FROM STATEMENT_FACTORY_TEST", connection, new int[] {1}, createContext());
        assertThat(sqlPStatement.getFetchSize(), is(3));
        assertThat(sqlPStatement.getQueryTimeout(), is(100));
    }

    /**
     * {@link BasicStatementFactory#getSqlPStatement(String, Connection, String[], DbExecutionContext)}のテスト。
     */
    @Test
    public void testGetSqlPStatementSupportAutoGenKeysByColumnNames() throws Exception {
        BasicStatementFactory sut = createStatementFactory();
        sut.setFetchSize("4");
        sut.setQueryTimeout(150);

        SqlPStatement sqlPStatement = sut.getSqlPStatement(
                "SELECT * FROM STATEMENT_FACTORY_TEST", connection, new String[] {"col1"}, createContext());
        assertThat(sqlPStatement.getFetchSize(), is(4));
        assertThat(sqlPStatement.getQueryTimeout(), is(150));
    }

    /** {@link BasicStatementFactory#getSqlPStatementBySqlId(String, Connection, DbExecutionContext)} のテスト。 */
    @Test
    public void testGetSqlPStatementBySqlId() throws SQLException {
        BasicSqlLoader loader = new BasicSqlLoader();

        BasicStatementFactory sut = createStatementFactory();
        sut.setFetchSize("5");
        sut.setQueryTimeout(200);
        sut.setSqlLoader(loader);

        SqlPStatement statement = sut.getSqlPStatementBySqlId(
                SQL_FILE_PATH + "#SQL1", connection, createContext());

        assertThat(statement.getFetchSize(), is(5));
        assertThat(statement.getQueryTimeout(), is(200));
    }

    /**
     * {@link BasicStatementFactory#getParameterizedSqlPStatement(String, Connection, DbExecutionContext)}のテスト。
     */
    @Test
    public void testGetParameterizedSqlPStatement() throws Exception {
        BasicStatementFactory sut = createStatementFactory();
        sut.setSqlParameterParserFactory(new BasicSqlParameterParserFactory());
        sut.setFetchSize("6");
        sut.setQueryTimeout(250);


        ParameterizedSqlPStatement statement = sut.getParameterizedSqlPStatement(
                "SELECT '1' FROM STATEMENT_FACTORY_TEST", connection, createContext());

        assertThat(statement, instanceOf(ParameterizedSqlPStatement.class));
    }

    /** SelectOptionが正しく設定されることの確認 */
    @Test
    public void testGetParameterizedSqlPStatementWithSelectOption() throws Exception {
        BasicStatementFactory sut = createStatementFactory();
        sut.setSqlParameterParserFactory(new BasicSqlParameterParserFactory());


        ParameterizedSqlPStatement statement = sut.getParameterizedSqlPStatement(
                "SELECT '1' FROM STATEMENT_FACTORY_TEST", connection, createContext(), new SelectOption(10, 20));

        SelectOption selectOption = (SelectOption) Deencapsulation.getField(statement, "selectOption");
        assertThat(selectOption.getStartPosition(), is(10));
        assertThat(selectOption.getLimit(), is(20));
    }

    /**
     * {@link BasicStatementFactory#getParameterizedSqlPStatementBySqlId(String, String, Connection, DbExecutionContext)}のテスト。
     */
    @Test
    public void testGetParameterizedSqlPStatementBySqlId() throws Exception {
        BasicSqlLoader loader = new BasicSqlLoader();

        BasicStatementFactory sut = createStatementFactory();
        sut.setFetchSize("1234");
        sut.setSqlLoader(loader);
        sut.setSqlParameterParserFactory(new BasicSqlParameterParserFactory());

        ParameterizedSqlPStatement sqlPStatement = sut.getParameterizedSqlPStatementBySqlId(
                SQL_FILE_PATH + "#SQL2", connection, createContext());

        HashMap<String, Object> data = new HashMap<String, Object>();
        data.put("1", "1");
        sqlPStatement.retrieve(data);
        assertThat(sqlPStatement, instanceOf(ParameterizedSqlPStatement.class));
    }

    /**
     * {@link BasicStatementFactory#getParameterizedSqlPStatementBySqlId(String, Connection, DbExecutionContext)}で不正なSQLIDを指定した場合、
     * 例外が発生すること。
     */
    @Test
    public void testGetParameterizedSqlPStatementBySqlId_InvalidSqlId() throws Exception {
        BasicStatementFactory sut = createStatementFactory();
        sut.setSqlLoader(new BasicSqlLoader());

        try {
            sut.getParameterizedSqlPStatementBySqlId("sql_id", connection, createContext());
            fail("do not run.");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is(
                    "sql resource format'sql_id' is invalid. valid format is 'sql resource + # + SQL_ID'."));
        }
    }

    /**
     * {@link BasicStatementFactory#getParameterizedSqlPStatementBySqlId(String, Connection, DbExecutionContext)}
     * で存在しないSQLIDを指定した場合エラーとなること。
     */
    @Test
    public void testGetParameterizedSqlPStatementBySqlId_notFoundSqlId() throws Exception {
        BasicStatementFactory sut = createStatementFactory();
        sut.setSqlLoader(new BasicSqlLoader());

        try {
            // 存在しないSQL_IDを指定した場合
            sut.getParameterizedSqlPStatementBySqlId(
                    SQL_FILE_PATH + "#not_found", connection, createContext());
            fail("do not run");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is(String.format("sql is not found. sql resource = [%s#not_found]",
                    SQL_FILE_PATH)));
        }
    }

    /**
     * {@link BasicStatementFactory#getParameterizedSqlPStatementBySqlId(String, Connection, DbExecutionContext)}
     * でローダー未設定の場合エラーとなること。
     *
     * @throws Exception
     */
    @Test
    public void testGetParameterizedSqlPStatementBySqlId_loaderNotSetting() throws Exception {

        // ローダを設定せずにSQLID指定で実行
        BasicStatementFactory sut = createStatementFactory();
        try {
            ParameterizedSqlPStatement statement = sut.getParameterizedSqlPStatementBySqlId(
                    SQL_FILE_PATH + "#SQL1", connection, createContext());
            fail("do not run.");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is("SqlLoader was not specified."));
        }
    }

    /**
     * {@link BasicStatementFactory#getVariableConditionSql(String, Object)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void testGetVariableConditionSql() throws Exception {
        BasicStatementFactory sut = createStatementFactory();
        setupSqlParser(sut);

        String standardSql = "SELECT * FROM STATEMENT_FACTORY_TEST";
        assertThat("エラーとならずにSQLが取得できること",
                sut.getVariableConditionSql(standardSql, null), is(standardSql));


        String nablarchSql = "select * from test where $if(id) {entity_id = :id}";
        assertThat(sut.getVariableConditionSql(nablarchSql, new TestEntity()),
                is("select * from test where (0 = 0 or (entity_id = :id))"));

    }

    /**
     * {@link BasicStatementFactory#getVariableConditionSqlBySqlId(String, Object)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void testGetVariableConditionSqlBySqlId() throws Exception {

        BasicStatementFactory sut = createStatementFactory();
        setupSqlParser(sut);

        BasicSqlLoader loader = new BasicSqlLoader();
        sut.setSqlLoader(loader);

        // オブジェクトにnullを指定した場合
        assertThat(sut.getVariableConditionSqlBySqlId(SQL_FILE_PATH + "#SQL3", null),
                is("select '1' from statement_factory_test"));

        assertThat(sut.getVariableConditionSqlBySqlId(SQL_FILE_PATH + "#SQL4", new TestEntity()),
                is("select * from statement_factory_test where (0 = 0 or (entity_id = :id))"));

    }

    /**
     * SQLの解析クラスなどをセットアップする。
     *
     * @param factory セットアップ対象のファクトリ
     */
    private void setupSqlParser(BasicStatementFactory factory) {
        factory.setSqlParameterParserFactory(new BasicSqlParameterParserFactory());
    }

    /**
     * {@link BasicStatementFactory#setLikeEscapeTargetCharList(String)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void testSetLikeEscapeTargetCharList() throws Exception {

        // 正常系は、エラーにならないこと
        BasicStatementFactory sut = createStatementFactory();
        sut.setLikeEscapeTargetCharList("%,_");

        // 異常系はエラーになること
        try {
            sut.setLikeEscapeTargetCharList(",_");
            fail("Do not run.");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("invalid escape char. char = []"));
        }

        // 異常系はエラーになること
        try {
            sut.setLikeEscapeTargetCharList("_,%_");
            fail("Do not run.");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("invalid escape char. char = [%_]"));
        }
    }

    @Mocked
    private TransactionManagerConnection mockConnection;

    /**
     * DBアクセス時の実行時のコンテキストを生成する。
     *
     * @return DBアクセス時の実行時のコンテキスト
     */
    private DbExecutionContext createContext() {
        return new DbExecutionContext(mockConnection, new DefaultDialect(), TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
    }

    @Entity
    @Table(name = "statement_factory_test")
    public static class TestEntity {

        @Id
        @Column(name = "entity_id")
        public String id;

        public String getId() {
            return id;
        }
    }

    protected abstract BasicStatementFactory createStatementFactory();
}

