package nablarch.core.db.connection;

import static java.lang.Integer.MIN_VALUE;
import static java.lang.String.format;
import static java.sql.Statement.NO_GENERATED_KEYS;
import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.sql.DataSource;

import nablarch.core.db.DbAccessException;
import nablarch.core.db.DbExecutionContext;
import nablarch.core.db.connection.exception.BasicDbAccessExceptionFactory;
import nablarch.core.db.dialect.DefaultDialect;
import nablarch.core.db.dialect.Dialect;
import nablarch.core.db.statement.BasicSqlLoader;
import nablarch.core.db.statement.BasicSqlParameterParserFactory;
import nablarch.core.db.statement.BasicStatementFactory;
import nablarch.core.db.statement.ParameterizedSqlPStatement;
import nablarch.core.db.statement.SelectOption;
import nablarch.core.db.statement.SqlCStatement;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlStatement;
import nablarch.core.db.statement.exception.BasicSqlStatementExceptionFactory;
import nablarch.core.transaction.TransactionContext;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;


/**
 * {@link nablarch.core.db.connection.BasicDbConnection}のテストクラス。
 *
 * @author Hisaaki Sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class BasicDbConnectionTest {

    /** target instance */
    private BasicDbConnection sut;

    /** jdbc connection of target instance */
    private Connection jdbcConnection;

    private static DataSource dataSource;

    @ClassRule
    public static SystemRepositoryResource repository = new SystemRepositoryResource("db-default.xml");

    private static final String TEST_TABLE = UserTestEntity.class.getAnnotation(Table.class).name();

    private static final String INSERT_QUERY = "insert into " + TEST_TABLE + " values ('0001', 'Name', 'telnum')";

    private static final String SELECT_QUERY = "select * from " + TEST_TABLE;

    private static final String SQL_ID_1 = BasicDbConnectionTest.class.getName() + "#SQL1";
    private static final String SQL_ID_2 = BasicDbConnectionTest.class.getName() + "#SQL2";
    private static final String SQL_ID_3 = BasicDbConnectionTest.class.getName() + "#SQL3";
    private static final String SQL_ID_4 = BasicDbConnectionTest.class.getName() + "#SQL4";

    @BeforeClass
    public static void beforeClass() throws Exception {
        dataSource = repository.getComponentByType(DataSource.class);
        VariousDbTestHelper.createTable(UserTestEntity.class);
    }

    @Before
    public void before() throws SQLException {

        jdbcConnection = dataSource.getConnection();
        sut = new BasicDbConnection(jdbcConnection);
        sut.setContext(createContext(sut));
        // StatementFactoryの設定
        BasicStatementFactory statementFactory = createStatementFactory();
        statementFactory.setSqlStatementExceptionFactory(new BasicSqlStatementExceptionFactory());

        sut.setFactory(statementFactory);

    }

    @After
    public void after() throws SQLException {
        try {
            sut.terminate();
        } catch (Exception e) {
            // nop
        }
    }

    /** {@link BasicDbConnection#initialize()} のテスト。 */
    @Test
    public void initialize(@Mocked final Connection mockedConnection) throws Exception {
        new Expectations() {{
            mockedConnection.setAutoCommit(false);
        }};

        BasicDbConnection target = createTarget(mockedConnection);

        // 初期処理の実行
        target.initialize();
    }

    /**
     * 初期処理でエラーが発生するケース
     */
    @Test
    public void initialize_error(@Mocked final Connection mockedConnection) throws Exception {
        new Expectations() {{
            mockedConnection.setAutoCommit(anyBoolean);
            result = new SQLException("initialize error");
        }};
        BasicDbConnection target = createTarget(mockedConnection);
        try {
            target.initialize();
            fail("ここはとおらない。");
        } catch (DbAccessException e) {
            assertEquals("failed to initialize.", e.getMessage());
        }
    }

    /** {@link BasicDbConnection#commit()} のテスト。 */
    @Test
    public void commit() throws Exception {
        deleteTestTable();
        // initialize前は、更新したデータが即コミットされること。

        sut.initialize();
        sut.prepareStatement(INSERT_QUERY).executeUpdate();

        Assert.assertEquals("commit前は0件", 0, VariousDbTestHelper.findAll(UserTestEntity.class).size());
        sut.commit();
        Assert.assertEquals("commit後は1件", 1, VariousDbTestHelper.findAll(UserTestEntity.class).size());

        sut.terminate();
    }

    @Test(expected = DbAccessException.class)
    public void commitFail(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = new BasicDbConnection(mockedConnection);
        target.setDbAccessExceptionFactory(new BasicDbAccessExceptionFactory());
        new Expectations() {{
            mockedConnection.commit();
            result = new SQLException("commit error");
        }};
        target.commit();
    }

    /**
     * {@link BasicDbConnection#rollback()} のテスト。
     */
    @Test
    public void rollback() {
        deleteTestTable();
        sut.initialize();
        sut.prepareStatement(INSERT_QUERY).executeUpdate();

        SqlPStatement statement = sut.prepareStatement(SELECT_QUERY);
        assertEquals("同一セッションなので、登録データが取得できる。", 1, statement.retrieve().size());
        sut.rollback();
        assertEquals("ロールバックされるので取得できない。", 0, statement.retrieve().size());

        sut.terminate();
    }

    @Test(expected = DbAccessException.class)
    public void rollbackFail(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = new BasicDbConnection(mockedConnection);
        target.setDbAccessExceptionFactory(new BasicDbAccessExceptionFactory());
        target.setContext(new DbExecutionContext(target, new DefaultDialect(), "name"));
        new Expectations() {{
            mockedConnection.rollback();
            result = new SQLException("rollback error");
        }};
        target.rollback();
    }

    /**
     * terminate時のロールバックで例外が発生する
     * @throws Exception
     */
    @Test
    public void terminate_rollbackFail(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = new BasicDbConnection(mockedConnection);
        target.setDbAccessExceptionFactory(new BasicDbAccessExceptionFactory());
        new Expectations() {{
            mockedConnection.rollback();
            result = new SQLException("rollback error");
        }};
        target.setContext(new DbExecutionContext(target, new DefaultDialect(), "name"));
        try {
            target.terminate();
            fail("ここはとおらない。");
        } catch (DbAccessException e) {
            assertEquals("failed to rollback.", e.getMessage());
        }
    }

    /**
     * terminate時のclose処理で例外が発生する
     * @throws Exception
     */
    @Test
    public void terminate_closeError(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        new Expectations() {{
            mockedConnection.close();
            result = new SQLException("close error");
        }};

        try {
            target.terminate();
            fail("ここはとおならい。");
        } catch (DbAccessException e) {
            assertEquals("failed to terminate.", e.getMessage());
        }
    }

    /**
     * terminate時のStatement#closeでエラーが発生する
     * @throws Exception
     */
    @Test
    public void terminate_statementCloseError(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        new Expectations() {{
            PreparedStatement mockStatement = mockedConnection.prepareStatement(anyString);
            mockStatement.close();
            result = new SQLException("statement close error");
        }};

        try {
            target.prepareStatement("SELECT * FROM DUMMY_TABLE");
            target.terminate();
            fail("ここはとおならい。");
        } catch (RuntimeException e) {
            assertEquals("failed to closeStatements.", e.getMessage());
        }
    }

    /** {@link BasicDbConnection#terminate()} のテスト。 */
    @Test
    public void terminate() throws SQLException {
        VariousDbTestHelper.delete(UserTestEntity.class);
        sut.initialize();
        SqlPStatement insert = sut.prepareStatement(INSERT_QUERY);
        insert.executeUpdate();

        SqlPStatement select = sut.prepareStatement(SELECT_QUERY);
        assertEquals("同一セッションなので、登録データが取得できる。", 1, select.retrieve().size());

        assertFalse("close前は、statementが閉じられていない。[insert]", insert.isClosed());
        assertFalse("close前は、statementが閉じられていない。[select]", select.isClosed());

        sut.terminate();

        assertTrue("close後は、statementが閉じられている。[insert]", insert.isClosed());
        assertTrue("close後は、statementが閉じられている。[select]", select.isClosed());

        Assert.assertEquals("closeでロールバックされていること", 0, VariousDbTestHelper.findAll(UserTestEntity.class).size());

        // connection#closeは複数回呼び出しても問題ない。
        sut.terminate();
    }

    /** {@link nablarch.core.db.connection.BasicDbConnection#setIsolationLevel(int)} のテスト。 */
    @Test
    public void setIsolationLevel() throws IllegalAccessException, SQLException, NoSuchFieldException {
        final Connection con = Deencapsulation.getField(sut, Connection.class);
        con.rollback();

        // Oracleで有効なレベルは、下記２しゅるいのみ
        // Connection.TRANSACTION_READ_COMMITTED
        // Connection.TRANSACTION_SERIALIZABLE
        sut.setIsolationLevel(Connection.TRANSACTION_READ_COMMITTED);
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation());
        sut.setIsolationLevel(Connection.TRANSACTION_SERIALIZABLE);
        assertEquals(Connection.TRANSACTION_SERIALIZABLE, con.getTransactionIsolation());
    }

    @Test(expected = DbAccessException.class)
    public void setIsolationLevelFail() throws Exception {
        sut.terminate();
        sut.setIsolationLevel(Connection.TRANSACTION_SERIALIZABLE);
    }

    /** {@link nablarch.core.db.connection.BasicDbConnection#prepareStatement(String)} のテスト。 */
    @Test
    public void testPrepareStatementReuseOff(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(false);

        String sql = SELECT_QUERY;

        assertThat("キャッシュされないため異なるインスタンスが返却される。",
                target.prepareStatement(sql),
                not(sameInstance(target.prepareStatement(sql))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times = 2;
        }};
    }

    /** {@link nablarch.core.db.connection.BasicDbConnection#prepareStatement(String)} のテスト。 */
    @Test
    public void testPrepareStatementReuseOn(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);
        String sql = SELECT_QUERY;

        assertThat("キャッシュされるため同じインスタンスが返却される。",
                target.prepareStatement(sql),
                sameInstance(target.prepareStatement(sql)));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times = 1;
        }};
    }

    /** {@link nablarch.core.db.connection.BasicDbConnection#prepareStatement(String)} のテスト。 */
    @Test
    public void testPrepareStatementReuseOnAnotherSql(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);
        String sql = SELECT_QUERY;

        assertThat("異なるSQLは、異なるインスタンスが返却される。",
                target.prepareStatement(sql + " "),
                not(sameInstance(target.prepareStatement(sql))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times = 2;
        }};
    }

    /** {@link nablarch.core.db.connection.BasicDbConnection#prepareStatement(String)} のテスト。 */
    @Test
    public void testPrepareStatementReuseOnWithClosedStatement() {
        String sql = SELECT_QUERY;
        // close呼び出し後は、異なるインスタンスが返却されること
        SqlPStatement statement = sut.prepareStatement(sql);
        statement.close();
        assertFalse(sut.prepareStatement(sql) == statement);
    }

    @Test(expected = DbAccessException.class)
    public void prepareStatementFail(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        new Expectations() {{
            mockedConnection.prepareStatement(anyString);
            result = new SQLException();
        }};
        target.prepareStatement(SELECT_QUERY);
    }

    /**
     * {@link nablarch.core.db.connection.BasicDbConnection#prepareStatement(String, int)}のテスト。
     */
    @Test
    public void testPrepareStatementAutoGenKeyReuseOff(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(false);
        String sql = INSERT_QUERY;

        assertThat("キャッシュされないので異なるインスタンスが返却される。",
                target.prepareStatement(sql, RETURN_GENERATED_KEYS),
                not(sameInstance(target.prepareStatement(sql, RETURN_GENERATED_KEYS))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString, anyInt);
            times = 2;
        }};
    }

    /**
     * {@link nablarch.core.db.connection.BasicDbConnection#prepareStatement(String, int)}のテスト。
     */
    @Test
    public void testPrepareStatementAutoGenKeyReuseOnAnotherFlg(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);
        String sql = INSERT_QUERY;

        assertThat("自動生成キーのフラグが異なるので、異なるインスタンスが返却される",
                target.prepareStatement(sql, NO_GENERATED_KEYS),
                not(sameInstance(target.prepareStatement(sql, RETURN_GENERATED_KEYS))));

        new Verifications(){{
            mockedConnection.prepareStatement(anyString, anyInt);
            times = 2;
        }};
    }

    /**
     * {@link nablarch.core.db.connection.BasicDbConnection#prepareStatement(String, int)}のテスト。
     */
    @Test
    public void testPrepareStatementAutoGenKeyReuseOn(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);
        String sql = INSERT_QUERY;

        assertThat("自動生成キーのフラグが同じなので同一のインスタンスが返却される。",
                target.prepareStatement(sql, RETURN_GENERATED_KEYS),
                sameInstance(target.prepareStatement(sql, RETURN_GENERATED_KEYS)));

        new Verifications(){{
            mockedConnection.prepareStatement(anyString, anyInt);
            times = 1;
        }};
    }

    /**
     * {@link nablarch.core.db.connection.BasicDbConnection#prepareStatement(String, int)}のテスト。
     */
    @Test
    public void testPrepareStatementAutoGenKeyReuseOnNoGen(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);
        String sql = INSERT_QUERY;

        assertThat("自動生成キーのフラグが同じなので同一のインスタンスが返却される。",
                target.prepareStatement(sql, NO_GENERATED_KEYS),
                sameInstance(target.prepareStatement(sql, NO_GENERATED_KEYS)));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString, anyInt);
            times = 1;
        }};
    }

    @Test
    public void prepareStatementAutoGenKeyFail(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        String sql = INSERT_QUERY;
        final Throwable nativeExecption = new SQLException("statement作成時にエラー");
        String message = "";

        new Expectations() {{
            mockedConnection.prepareStatement(anyString, anyInt);
            result = nativeExecption;
        }};

        try {
            target.prepareStatement(sql, MIN_VALUE);
            fail("ここは通らない。");
        } catch (DbAccessException e) {
            message = e.getMessage();
            assertThat(e.getCause(), is(nativeExecption));
        }
        assertThat(message, is(MessageFormat.format(
                "failed to prepareStatement. SQL = [{0}], autoGeneratedKeys = [{1}]", sql, MIN_VALUE)));
    }


    /** {@link nablarch.core.db.connection.BasicDbConnection#prepareStatement(String, int[])}のテスト。 */
    @Test
    public void testPrepareStatementAutoGenKeyByColumnIndexesReuseOff(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(false);
        String sql = INSERT_QUERY;

        assertThat("キャッシュされないので異なるインスタンスが返却される。",
                target.prepareStatement(sql, new int[]{1}),
                not(sameInstance(target.prepareStatement(sql, new int[]{1}))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString, new int[]{1});
            times = 2;
        }};
    }

    /** {@link nablarch.core.db.connection.BasicDbConnection#prepareStatement(String, int[])}のテスト。 */
    @Test
    public void testPrepareStatementAutoGenKeyByColumnIndexesReuseOffMultiIndexes(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(false);
        String sql = INSERT_QUERY;

        assertThat("キャッシュされないので異なるインスタンスが返却される。",
                target.prepareStatement(sql, new int[]{1, 2}),
                not(sameInstance(target.prepareStatement(sql, new int[]{1, 2}))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString, new int[]{1, 2});
            times = 2;
        }};
    }

    /** {@link nablarch.core.db.connection.BasicDbConnection#prepareStatement(String, int[])}のテスト。 */
    @Test
    public void testPrepareStatementAutoGenKeyByColumnIndexesReuseOn(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);
        String sql = INSERT_QUERY;

        assertThat("キャッシュされるので同じインスタンスが返される",
                target.prepareStatement(sql, new int[]{1}),
                sameInstance(target.prepareStatement(sql, new int[]{1})));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString, new int[]{1});
            times = 1;
        }};
    }

    /** {@link nablarch.core.db.connection.BasicDbConnection#prepareStatement(String, int[])}のテスト。 */
    @Test
    public void testPrepareStatementAutoGenKeyByColumnIndexesReuseOnMultiIndexes(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);
        String sql = INSERT_QUERY;

        assertThat("自動生成カラムのインデックスが違うのでことなるインスタンスが返される",
                target.prepareStatement(sql, new int[]{2}),
                not(sameInstance(target.prepareStatement(sql, new int[]{1}))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString, new int[]{2});
            mockedConnection.prepareStatement(anyString, new int[]{1});
        }};
    }

    /** {@link nablarch.core.db.connection.BasicDbConnection#prepareStatement(String, int[])}のテスト。 */
    @Test
    public void testPrepareStatementAutoGenKeyByColumnIndexesReuseOnAnotherIndexes(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);
        String sql = INSERT_QUERY;

        assertThat("自動生成カラムのインデックスが複数の場合でもキャッシュが機能する",
                target.prepareStatement(sql, new int[]{1, 2}),
                sameInstance(target.prepareStatement(sql, new int[]{1, 2}))
        );

        new Verifications() {{
            mockedConnection.prepareStatement(anyString, new int[]{1, 2});
            times = 1;
        }};
    }

    @Test
    public void testPrepareStatementAutoGenKeyByColumnIndexesFail(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        String sql = INSERT_QUERY;
        String message = "";
        final Throwable nativeException = new SQLException("statementの生成に失敗");
        new Expectations() {{
            mockedConnection.prepareStatement(anyString, new int[0]);
            result = nativeException;
        }};

        try {
            target.prepareStatement(sql, new int[0]);
        } catch (DbAccessException e) {
            message = e.getMessage();
            assertThat(e.getCause(), is(nativeException));
        }

        assertThat(message,
                is(MessageFormat.format("failed to prepareStatement. SQL = [{0}], columnIndexes = [{1}]",
                        sql, Arrays.toString(new int[0]))));
    }

    @Test
    public void testPrepareStatementAutoGenKeyByColumnNamesReuseOff(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(false);
        String sql = INSERT_QUERY;
        final String col1 = "USER_ID";

        assertThat("キャッシュされないので異なるインスタンスが返される。",
                target.prepareStatement(sql, new String[]{col1}),
                not(sameInstance(target.prepareStatement(sql, new String[]{col1}))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString, new String[]{col1});
            times = 2;
        }};
    }

    @Test
    public void testPrepareStatementAutoGenKeyByColumnNamesReuseOffManyCols(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(false);

        String sql = INSERT_QUERY;
        final String col1 = "USER_ID";
        final String col2 = "USER_NAME";

        assertThat("複数カラムを指定した場合でも問題ない",
                target.prepareStatement(sql, new String[]{col1, col2}),
                not(sameInstance(target.prepareStatement(sql, new String[]{col1, col2}))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString, new String[]{col1, col2});
            times = 2;
        }};
    }

    @Test
    public void testPrepareStatementAutoGenKeyByColumnNamesReuseOn(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);

        String sql = INSERT_QUERY;
        final String col1 = "USER_ID";

        assertThat("キャッシュされるので同じインスタンスが取得できる",
                target.prepareStatement(sql, new String[]{col1}),
                sameInstance(target.prepareStatement(sql, new String[]{col1})));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString, new String[]{col1});
            times = 1;
        }};
    }

    @Test
    public void testPrepareStatementAutoGenKeyByColumnNamesReuseOnMultiCol(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);

        String sql = INSERT_QUERY;
        final String col1 = "USER_ID";
        final String col2 = "USER_NAME";

        assertThat("複数カラム指定した場合でもキャッシュが効く",
                target.prepareStatement(sql, new String[]{col1, col2}),
                sameInstance(target.prepareStatement(sql, new String[]{col1, col2})));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString, new String[]{col1, col2});
            times = 1;
        }};
    }

    @Test
    public void testPrepareStatementAutoGenKeyByColumnNamesReuseOnAnotherCol(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);

        String sql = INSERT_QUERY;
        final String col1 = "USER_ID";
        final String col2 = "USER_NAME";

        assertThat("カラム名が異なれば同じSQLでも違うインスタンス",
                target.prepareStatement(sql, new String[]{col1, col2}),
                not(sameInstance(target.prepareStatement(sql, new String[]{col1}))));

        new Verifications(){{
            mockedConnection.prepareStatement(anyString, new String[]{col1, col2});
            mockedConnection.prepareStatement(anyString, new String[]{col1});
        }};
    }

    @Test
    public void prepareStatementAutoGenKeyByColumnNamesFail(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        final Throwable nativeException = new SQLException("statement生成で失敗");
        new Expectations() {{
            mockedConnection.prepareStatement(anyString, new String[]{"col3"});
            result = nativeException;
        }};
        String sql = INSERT_QUERY;

        String message = "";
        try {
            target.prepareStatement(sql, new String[]{"col3"});
            fail("ここはとおらない。");
        } catch (DbAccessException e) {
            message = e.getMessage();
            assertThat(e.getCause(), is(nativeException));
        }
        assertThat(message, is(
                MessageFormat.format("failed to prepareStatement. SQL = [{0}], columnNames = [{1}]",
                        sql, Arrays.toString(new String[]{"col3"}))));
    }

    /** {@link BasicDbConnection#prepareStatementBySqlId(String)} のテスト。 */
    @Test
    public void testPrepareStatementBySqlIdReuseOff(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(false);

        String sqlId = SQL_ID_1;

        assertThat("キャッシュされないため異なるインスタンスが返却される。",
                target.prepareStatementBySqlId(sqlId),
                not(sameInstance(target.prepareStatementBySqlId(sqlId))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times = 2;
        }};
    }

    /** {@link BasicDbConnection#prepareStatementBySqlId(String)} のテスト。 */
    @Test
    public void testPrepareStatementBySqlIdReuseOn(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);

        String sqlId = SQL_ID_1;

        assertThat("キャッシュされるため同じインスタンスが返却される。",
                target.prepareStatementBySqlId(sqlId),
                sameInstance(target.prepareStatementBySqlId(sqlId)));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times = 1;
        }};
    }

    /** {@link BasicDbConnection#prepareStatementBySqlId(String)} のテスト。 */
    @Test
    public void testPrepareStatementBySqlIdReuseOnAnotherSql(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);

        assertThat("異なるSQLは、異なるインスタンスが返却される。",
                target.prepareStatementBySqlId(SQL_ID_2),
                not(sameInstance(target.prepareStatementBySqlId(SQL_ID_1))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times = 2;
        }};
    }

    /** {@link BasicDbConnection#prepareStatementBySqlId(String)}のテスト。  */
    @Test
    public void testPrepareStatementBySqlIdAfterClosed() throws Exception {
        // close呼び出し後は、異なるインスタンスが返却されること
        SqlPStatement statement = sut.prepareStatementBySqlId(SQL_ID_1);
        statement.close();
        assertFalse(sut.prepareStatementBySqlId(SQL_ID_1) == statement);
    }

    @Test
    public void testPrepareStatementBGySqlIdFailedToCreateStatement(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        final Throwable nativeException = new SQLException("statement生成時にエラー");
        new Expectations() {{
            mockedConnection.prepareStatement(anyString);
            result = nativeException;
        }};

        try {
            target.prepareStatementBySqlId(SQL_ID_1);
            fail("do not run.");
        } catch (Exception e) {
            assertEquals(
                    "failed to prepareStatementBySqlId. SQL_ID = [nablarch.core.db.connection.BasicDbConnectionTest#SQL1]",
                    e.getMessage());
            assertThat(e.getCause(), is(nativeException));
        }
    }

    /** {@link nablarch.core.db.connection.BasicDbConnection#prepareParameterizedSqlStatement(String)} のテスト。 */
    @Test
    public void prepareParameterizedSqlStatementOff(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(false);

        String sql = SELECT_QUERY;
        assertThat("キャッシュされないため異なるインスタンスが返却される。",
                target.prepareParameterizedSqlStatement(sql),
                not(sameInstance(target.prepareParameterizedSqlStatement(sql))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times = 2;
        }};
    }

    /** {@link nablarch.core.db.connection.BasicDbConnection#prepareParameterizedSqlStatement(String)} のテスト。 */
    @Test
    public void prepareParameterizedSqlStatementOn(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);
        String sql = SELECT_QUERY;

        assertThat("キャッシュされるため同じインスタンスが返却される。",
                target.prepareParameterizedSqlStatement(sql),
                sameInstance(target.prepareParameterizedSqlStatement(sql)));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times = 1;
        }};
    }

    /** {@link nablarch.core.db.connection.BasicDbConnection#prepareParameterizedSqlStatement(String)} のテスト。 */
    @Test
    public void prepareParameterizedSqlStatementOnAnotherSql(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);
        String sql = SELECT_QUERY;

        assertThat("異なるSQLは、異なるインスタンスが返却される。",
                target.prepareParameterizedSqlStatement(sql + " "),
                not(sameInstance(target.prepareParameterizedSqlStatement(sql))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times = 2;
        }};
    }

    /** {@link nablarch.core.db.connection.BasicDbConnection#prepareParameterizedSqlStatement(String)} のテスト。 */
    @Test
    public void prepareParameterizedSqlStatementFailedToCreateStatement(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        final Throwable nativeException = new SQLException("statement作成時に失敗");
        new Expectations() {{
            mockedConnection.prepareStatement(anyString);
            result = nativeException;
        }};

        // 構文エラーのSQLを指定した場合は例外が発生
        try {
            target.prepareParameterizedSqlStatement("");
            fail("ここはとおらない。");
        } catch (DbAccessException e) {
            assertEquals("failed to prepareParameterizedSqlStatement. SQL = []", e.getMessage());
            assertThat(e.getCause(), is(nativeException));
        }
    }

    /**
     * {@link nablarch.core.db.connection.BasicDbConnection#prepareParameterizedSqlStatementBySqlId(String)}のテスト。
     * @throws Exception テスト実行時の例外
     */
    @Test
    public void prepareParameterizedSqlStatementBySqlIdReuseOff(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(false);
        String sqlId = SQL_ID_1;

        assertThat("キャッシュされないため異なるインスタンスが返却される。",
                target.prepareParameterizedSqlStatementBySqlId(sqlId),
                not(sameInstance(target.prepareParameterizedSqlStatementBySqlId(sqlId))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times=2;
        }};
    }

    /**
     * {@link nablarch.core.db.connection.BasicDbConnection#prepareParameterizedSqlStatementBySqlId(String)}のテスト。
     * @throws Exception テスト実行時の例外
     */
    @Test
    public void testPrepareParameterizedSqlStatementBySqlIdReuseOn(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);
        String sqlId = SQL_ID_1;

        assertThat("キャッシュされるため同じインスタンスが返却される。",
                target.prepareParameterizedSqlStatementBySqlId(sqlId),
                sameInstance(target.prepareParameterizedSqlStatementBySqlId(sqlId)));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times = 1;
        }};
    }

    @Test
    public void testPrepareParameterizedSqlStatementBySqlIdReuseOnAnother(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);

        assertThat("異なるSQLは、異なるインスタンスが返却される。",
                target.prepareParameterizedSqlStatementBySqlId(SQL_ID_2),
                not(sameInstance(target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times = 2;
        }};
    }

    /**
     * {@link nablarch.core.db.connection.BasicDbConnection#prepareParameterizedSqlStatementBySqlId(String)}のテスト。
     * <p />
     * terminate()後に呼び出した場合の確認。
     *
     * @throws Exception テスト実行時の例外
     */
    public void testPrepareParameterizedSqlStatementBySqlIdAfterTerminate() throws Exception {
        sut.terminate();
        try {
            sut.prepareParameterizedSqlStatementBySqlId(SQL_ID_1);
            fail("do not run.");
        } catch (DbAccessException e) {
            assertEquals(format("failed to prepareParameterizedSqlStatementBySqlId. SQL_ID = [%s]", SQL_ID_1),
                    e.getMessage());
        }
    }

    /**
     *  {@link nablarch.core.db.connection.BasicDbConnection#prepareParameterizedSqlStatement(String, Object)} のテスト。
     * @throws Exception テスト実行時に例外が発生した場合
     */
    @Test
    public void prepareParameterizedSqlStatementReuseOff(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(false);
        String sql = "select * from mock where $if(userId){user_id = :userId} and user_name = :userName";
        UserTestEntity entity = new UserTestEntity();
        entity.userId = "userId";

        assertThat("キャッシュされないため異なるインスタンスが返却される。",
                target.prepareParameterizedSqlStatement(sql, entity),
                not(sameInstance(target.prepareParameterizedSqlStatement(sql, entity))));

        // キャッシュ無効なので、同一のクエリでも、複数回statementを作成する。
        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times=2;
        }};
    }

    /**
     *  {@link nablarch.core.db.connection.BasicDbConnection#prepareParameterizedSqlStatement(String, Object)} のテスト。
     *  <p />
     *  ステートメントを再利用する場合の動作を確認。
     * @throws Exception
     */
    @Test
    public void testPrepareParameterizedSqlStatementReuseOn(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);
        String sql = "select * from mock where $if(userId){user_id = :userId} and user_name = :userName";
        UserTestEntity entity = new UserTestEntity();
        entity.userId = "userId";

        assertThat("キャッシュされるため同じインスタンスが返却される。",
                target.prepareParameterizedSqlStatement(sql, entity),
                sameInstance(target.prepareParameterizedSqlStatement(sql, entity)));

        // 再利用を行うのでConnectionへのアクセスは1回
        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times=1;
        }};
    }

    /**
     *  {@link nablarch.core.db.connection.BasicDbConnection#prepareParameterizedSqlStatement(String, Object)} のテスト。
     *  <p />
     *  ステートメントを再利用する場合の動作を確認。
     * @throws Exception
     */
    @Test
    public void testPrepareParameterizedSqlStatementReuseOnAnother(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);
        String sql = "select * from mock where $if(userId){user_id = :userId} and user_name = :userName";
        UserTestEntity entity = new UserTestEntity();
        entity.userId = "userId";


        assertThat("異なるSQLは、異なるインスタンスが返却される。",
                target.prepareParameterizedSqlStatement(sql + " ", entity),
                not(sameInstance(target.prepareParameterizedSqlStatement(sql, entity))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times=2;
        }};
    }

    /**
     *  {@link nablarch.core.db.connection.BasicDbConnection#prepareParameterizedSqlStatement(String, Object)} のテスト。
     * <p />
     *
     * @throws Exception テスト実行時に例外が発生した場合
     */
    @Test
    public void testPrepareParameterizedSqlStatementFailedCreateStatement(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        final Throwable nativeException = new SQLException("statement作成に失敗。");
        UserTestEntity entity = new UserTestEntity();
        entity.userId = "userId";

        new Expectations() {{
            mockedConnection.prepareStatement(anyString);
            result = nativeException;
        }};

        try {
            target.prepareParameterizedSqlStatement("", entity);
            fail("ここはとおらない。");
        } catch (DbAccessException e) {
            assertThat(e.getMessage(), is("failed to prepareParameterizedSqlStatement. SQL = []"));
            assertThat(e.getCause(), is(nativeException));
        }
    }

    /** {@link BasicDbConnection#prepareParameterizedSqlStatementBySqlId(String, Object)}のテスト。 */
    @Test
    public void testPrepareParameterizedSqlStatementBySqlIdReuseOff(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(false);
        UserTestEntity entity = new UserTestEntity();
        entity.userId = "userId";
        String sql = SQL_ID_3;

        assertThat("キャッシュされないため異なるインスタンスが返却される。",
                target.prepareParameterizedSqlStatementBySqlId(sql, entity),
                not(sameInstance(target.prepareParameterizedSqlStatementBySqlId(sql, entity))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times=2;
        }};
    }

    /** {@link BasicDbConnection#prepareParameterizedSqlStatementBySqlId(String, Object)}のテスト。 */
    @Test
    public void testPrepareParameterizedSqlStatementBySqlIdWithObjectReuseOn(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);
        UserTestEntity entity = new UserTestEntity();
        entity.userId = "userId";

        assertThat("キャッシュされるため同じインスタンスが返却される。",
                target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, entity),
                sameInstance(target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, entity)));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times=1;
        }};
    }

    /** {@link BasicDbConnection#prepareParameterizedSqlStatementBySqlId(String, Object)}のテスト。 */
    @Test
    public void testPrepareParameterizedSqlStatementBySqlIdWithObjectReuseOnAnotherSql(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        UserTestEntity entity = new UserTestEntity();
        entity.userId = "userId";
        target.setStatementReuse(true);

        assertThat("異なるSQLは、異なるインスタンスが返却される。",
                target.prepareParameterizedSqlStatementBySqlId(SQL_ID_4, entity),
                not(sameInstance(target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, entity))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times=2;
        }};
    }

    /**
     * {@link BasicDbConnection#prepareParameterizedSqlStatementBySqlId(String, Object)}のテスト。
     * <p />
     *
     * @throws Exception テスト実行時に例外が発生した場合。
     */
    @Test
    public void testPrepareParameterizedSqlStatementBySqlIdWithObjectFailedToCreateStatement(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        String sqlId = SQL_ID_1;
        final Throwable nativeException = new SQLException("statement生成時のエラー");
        new Expectations() {{
            mockedConnection.prepareStatement(anyString);
            result = nativeException;
        }};

        try {
            target.prepareParameterizedSqlStatementBySqlId(sqlId, new UserTestEntity());
            fail("ここはとおらない。");
        } catch (DbAccessException e) {
            assertThat(e.getMessage(),
                    is(format("failed to prepareParameterizedSqlStatementBySqlId. SQL_ID = [%s]", sqlId)));
            assertThat(e.getCause(), is(nativeException));
        }
    }

    /** {@link BasicDbConnection#prepareParameterizedCountSqlStatementBySqlId(String, Object)}のテスト。 */
    @Test
    public void testPrepareParameterizedCountSqlStatementBySqlIdReuseOff(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        UserTestEntity entity = new UserTestEntity();
        entity.userId = "userId";
        String sqlId = SQL_ID_3;

        // Statementの再利用を行わない場合
        target.setStatementReuse(false);

        assertThat("キャッシュされないため異なるインスタンスが返却される。",
                target.prepareParameterizedCountSqlStatementBySqlId(sqlId, entity),
                not(sameInstance(target.prepareParameterizedCountSqlStatementBySqlId(sqlId, entity))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times=2;
        }};
    }

    /** {@link BasicDbConnection#prepareParameterizedCountSqlStatementBySqlId(String, Object)}のテスト。 */
    @Test
    public void testPrepareParameterizedCountSqlStatementBySqlIdReuseOn(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);

        UserTestEntity entity = new UserTestEntity();
        entity.userId = "userId";
        String sqlId = SQL_ID_3;

        assertThat("キャッシュされるため同じインスタンスが返却される。",
                target.prepareParameterizedCountSqlStatementBySqlId(sqlId, entity),
                sameInstance(target.prepareParameterizedCountSqlStatementBySqlId(sqlId, entity)));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times = 1;
        }};
    }

    /** {@link BasicDbConnection#prepareParameterizedCountSqlStatementBySqlId(String, Object)}のテスト。 */
    @Test
    public void testPrepareParameterizedCountSqlStatementBySqlIdReuseOnAnother(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        target.setStatementReuse(true);
        UserTestEntity entity = new UserTestEntity();
        entity.userId = "userId";

        assertThat("異なるSQLは、異なるインスタンスが返却される。",
                target.prepareParameterizedCountSqlStatementBySqlId(SQL_ID_4, entity),
                not(sameInstance(target.prepareParameterizedCountSqlStatementBySqlId(SQL_ID_3, entity))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times = 2;
        }};
    }

    /** {@link BasicDbConnection#prepareParameterizedCountSqlStatementBySqlId(String, Object)}のテスト。 */
    @Test
    public void testPrepareParameterizedCountSqlStatementBySqlIdFailedToCreateStatement(
            @Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        final Throwable nativeException = new SQLException("statementの生成に失敗。");
        new Expectations() {{
            mockedConnection.prepareStatement(anyString);
            result = nativeException;
        }};
        String sqlId = SQL_ID_1;
        try {
            target.prepareParameterizedCountSqlStatementBySqlId(sqlId, new UserTestEntity());
            fail("ここはとおらない。");
        } catch (DbAccessException e) {
            assertThat(e.getMessage(),
                    is(format("failed to prepareParameterizedCountSqlStatementBySqlId. SQL_ID = [%s]", sqlId)));
            assertThat(e.getCause(), is(nativeException));
        }
    }

    /**
     * {@link BasicDbConnection#prepareCountStatementBySqlId(String)}のテスト。
     * @throws Exception
     */
    @Test
    public void testPrepareCountStatementBySqlIdReuseOff(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        String sqlId = SQL_ID_3;

        // Statementの再利用を行わない場合
        target.setStatementReuse(false);

        assertThat("キャッシュされないため異なるインスタンスが返却される。",
                target.prepareCountStatementBySqlId(sqlId),
                not(sameInstance(target.prepareCountStatementBySqlId(sqlId))));

        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times=2;
        }};
    }

    @Test
    public void testPrepareCountStatementBySqlIdReuseOn(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        String sqlId = SQL_ID_3;

        target.setStatementReuse(true);
        assertThat("キャッシュされるため同じインスタンスが返却される。",
                target.prepareCountStatementBySqlId(sqlId),
                sameInstance(target.prepareCountStatementBySqlId(sqlId)));
        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times=1;
        }};

        assertThat("異なるSQLは、異なるインスタンスが返却される。",
                target.prepareCountStatementBySqlId(SQL_ID_4),
                not(sameInstance(target.prepareCountStatementBySqlId(sqlId))));
        new Verifications() {{
            mockedConnection.prepareStatement(anyString);
            times = 2 ;
        }};
    }

    @Test
    public void testPrepareCountStatementBySqlIdFailedToCreateStatement(@Mocked final Connection mockedConnection) throws Exception {
        BasicDbConnection target = createTarget(mockedConnection);
        final Throwable nativeException = new SQLException("statement作成時にエラー");
        new Expectations() {{
            mockedConnection.prepareStatement(anyString);
            result = nativeException;
        }};

        String sqlId = SQL_ID_1;
        try {
            target.prepareCountStatementBySqlId(sqlId);
            fail("ここはとおらない。");
        } catch (DbAccessException e) {
            assertThat(e.getMessage(),
                    is(format("failed to prepareCountStatementBySqlId. SQL_ID = [%s]", sqlId)));
            assertThat(e.getCause(), is(nativeException));
        }
    }

    /** {@link BasicDbConnection#getConnection()} */
    @Test
    public void getConnection() {
        assertThat("コンストラクタで設定されたJDBCコネクションが取得できる。",
                sut.getConnection(),
                sameInstance(jdbcConnection));
    }

    /** terminateでコネクションクローズできなかった場合、例外が発生すること。 */
    @Test
    public void testCloseConnectionFail() {
        try {
            new BasicDbConnection(jdbcConnection) {
                @Override
                protected void closeConnection() throws SQLException {
                    throw new SQLException("for test");
                }
            }.terminate();
            fail();
        } catch (DbAccessException e) {
            assertThat(e.getMessage(), is("failed to terminate."));
            assertThat(e.getCause(), instanceOf(SQLException.class));
        }
    }

    /**
     *{@link nablarch.core.db.connection.BasicDbConnection#prepareCall(String)}のテストケース
     */
    @Test
    @TargetDb(include = TargetDb.Db.ORACLE)
    public void testPrepareCall() throws Exception {
        // ----------------------------------------- cache off
        sut.setStatementReuse(false);

        final String sql = "BEGIN ?:= NULL; END;";

        assertThat("キャッシュされないので複数回呼び出した場合は、異なるインスタンスが返される",
                sut.prepareCall(sql),
                is(not(sameInstance(sut.prepareCall(sql)))));

        // ----------------------------------------- cache on
        sut.setStatementReuse(true);

        assertThat("キャッシュされるので同じインスタンスが返されるはず",
                sut.prepareCall(sql), is(sameInstance(sut.prepareCall(sql))));

        assertThat("SQLが一部異なる場合は、異なるインスタンスが返される",
                sut.prepareCall(sql + ' '),
                is(not(sameInstance(sut.prepareCall(sql)))));

        final SqlCStatement statement = sut.prepareCall(sql);
        statement.close();
        assertThat("ステートメントをクローズしているので、次回の呼び出しでは異なるインスタンスが返される。",
                statement,
                is(not(sameInstance(sut.prepareCall(sql)))));

        try {
            sut.terminate();
            sut.prepareCall(sql);
            fail("ここはとおらない。");
        } catch (DbAccessException e) {
            assertEquals(format(
                        "failed to prepareCall. SQL = [%s]", sql),
                    e.getMessage());
        }
    }

    @Test
    public void testPrepareCallBySqlId() throws Exception {
        // ----------------------------------------- cache off
        sut.setStatementReuse(false);

        final String sqlId = BasicDbConnectionTest.class.getName() + "#procedure";
        final String otherSqlId = BasicDbConnectionTest.class.getName() + "#procedure2";

        assertThat("キャッシュされないので複数回呼び出した場合は異なるインスタンスが返される。",
                sut.prepareCallBySqlId(sqlId),
                not(sameInstance(sut.prepareCallBySqlId(sqlId))));

        // ----------------------------------------- cache on
        sut.setStatementReuse(true);

        assertThat("キャッシュされるので同じインスタンスがかえされるはず",
                sut.prepareCallBySqlId(sqlId),
                is(sameInstance(sut.prepareCallBySqlId(sqlId))));
        assertThat("SQLが異なる場合は、異なるインスタンスが返される",
                sut.prepareCallBySqlId(sqlId),
                is(not(sameInstance(sut.prepareCallBySqlId(otherSqlId)))));

        final SqlCStatement statement = sut.prepareCall(sqlId);
        statement.close();
        assertThat("ステートメントをクローズしているので、次回の呼び出しでは異なるインスタンスが返される。",
                statement,
                is(not(sameInstance(sut.prepareCallBySqlId(sqlId)))));

        try {
            sut.terminate();
            sut.prepareCallBySqlId(sqlId);
            fail("ここはとおらない。");
        } catch (DbAccessException e) {
            assertEquals(
                    format("failed to prepareCallBySqlId. SQL_ID = [%s]", sqlId),
                    e.getMessage());
        }
    }

    /**
     * {@link BasicDbConnection#getDialect()}のテスト。
     *
     * @param connection {@link BasicDbConnection target}のインスタンス生成用のMock
     */
    @Test
    public void testGetDialect(@Mocked final Connection connection) {
        BasicDbConnection target = new BasicDbConnection(connection);
        Dialect dialect = new DefaultDialect();
        target.setContext(new DbExecutionContext(target, dialect, TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
        assertThat("コンテキストに設定されたダイアレクトが取得できること。", target.getDialect(), sameInstance(dialect));
    }

     private void deleteTestTable() {
          VariousDbTestHelper.delete(UserTestEntity.class);
     }

    /**
     * ステートメントに正しくSelectOptionが渡されるか確認する。
     */
    @Test
    public void testPrepareStatementWithOption(@Mocked final Connection connection) throws Exception {
        BasicDbConnection target = createTarget(connection);
        target.setContext(new DbExecutionContext(target, new DefaultDialect(), TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
        SqlPStatement ps = target.prepareStatement("sql", new SelectOption(2, 2));
        SelectOption selectOption = (SelectOption) Deencapsulation.getField(ps, "selectOption");
        assertThat(selectOption.getOffset(), is(1));
        assertThat(selectOption.getLimit(), is(2));
    }

    /**
     * Optionを指定する場合の再利用のテスト。
     */
    @Test
    public void testPrepareStatementWithOptionReuseOn(@Mocked final Connection connection) throws Exception {
        BasicDbConnection target = createTarget(connection);
        target.setStatementReuse(true);
        target.setContext(new DbExecutionContext(target, new DefaultDialect(), TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
        assertThat("同じオプションなので、同一のインスタンスが返却される",
                  target.prepareStatement("sql", new SelectOption(2, 2))
                , sameInstance(target.prepareStatement("sql", new SelectOption(2, 2))));
        assertThat("オプションが異なるので、別のインスタンスが返却される",
                target.prepareStatement("sql", new SelectOption(2, 2)),
                not(sameInstance(target.prepareStatement("sql", new SelectOption(2, 3)))));
    }

    /**
     * Optionを指定する場合の再利用のテスト。
     */
    @Test
    public void testPrepareStatementWithOptionReuseOff(@Mocked final Connection connection) throws Exception {
        BasicDbConnection target = createTarget(connection);
        target.setStatementReuse(false);
        target.setContext(new DbExecutionContext(target, new DefaultDialect(), TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
        assertThat("キャッシュしないので別のインスタンスが返却される",
                  target.prepareStatement("sql", new SelectOption(2, 2))
                , not(sameInstance(target.prepareStatement("sql", new SelectOption(2, 2)))));
    }

    /**
     * ステートメントの生成に失敗したときの確認。
     */
    @Test
    public void testPrepareStatementWithOptionFail(@Mocked final Connection connection) throws Exception {
            BasicDbConnection target = createTarget(connection);
            String sql = INSERT_QUERY;
            final Throwable nativeException = new SQLException("statement作成時にエラー");
            String message = "";
            SelectOption selectOption = new SelectOption(3, 5);
            new Expectations() {{
                connection.prepareStatement(anyString);
                result = nativeException;
            }};
            try {
                target.prepareStatement(sql, selectOption);
                fail("ここは通らない");
            } catch (DbAccessException e) {
                 message = e.getMessage();
                 assertThat(e.getCause(), is(nativeException));
            }
            assertThat(message, is("failed to prepareStatement. SQL = [" + sql + "], " + selectOption));
    }

    /**
     * ステートメントに正しくSelectOptionが渡されるか確認する。
     */
    @Test
    public void testPrepareStatementSqlIdWithOption(@Mocked final Connection connection) throws Exception {
        BasicDbConnection target = createTarget(connection);
        target.setContext(new DbExecutionContext(target, new DefaultDialect(), TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
        SqlPStatement ps = target.prepareStatementBySqlId(SQL_ID_1, new SelectOption(4, 5));
        SelectOption selectOption = (SelectOption) Deencapsulation.getField(ps, "selectOption");
        assertThat(selectOption.getOffset(), is(3));
        assertThat(selectOption.getLimit(), is(5));
    }

    /**
     * Optionを指定する場合の再利用のテスト。
     */
    @Test
    public void testPrepareStatementSqlIdWithOptionReuseOn(@Mocked final Connection connection) throws Exception {
        BasicDbConnection target = createTarget(connection);
        target.setStatementReuse(true);
        assertThat("同一のオプションなので、同じインスタンスが返却される。",
                target.prepareStatementBySqlId(SQL_ID_1, new SelectOption(4, 5)),
               sameInstance(target.prepareStatementBySqlId(SQL_ID_1, new SelectOption(4, 5))));
        assertThat("オプションが異なるので、別のインスタンスが返却される。",
                target.prepareStatementBySqlId(SQL_ID_1, new SelectOption(4, 5)),
                not(sameInstance(target.prepareStatementBySqlId(SQL_ID_1, new SelectOption(4, 6)))));
    }

    /**
     * Optionを指定する場合の再利用のテスト。
     */
    @Test
    public void testPrepareStatementSqlIdWithOptionReuseOff(@Mocked final Connection connection) throws Exception {
        BasicDbConnection target = createTarget(connection);
        target.setStatementReuse(false);
        assertThat("キャッシュしないので別のインスタンスが返却される",
                target.prepareStatementBySqlId(SQL_ID_1, new SelectOption(4, 5)),
               not(sameInstance(target.prepareStatementBySqlId(SQL_ID_1, new SelectOption(4, 5)))));
    }

    /**
     * ステートメントの生成に失敗したときの確認。
     */
    @Test
    public void testPrepareStatementSqlIdWithOptionFail(@Mocked final Connection connection) throws Exception {
            BasicDbConnection target = createTarget(connection);
            final Throwable nativeExecption = new SQLException("statement作成時にエラー");
            String message = "";
            SelectOption selectOption = new SelectOption(4, 5);
            new Expectations() {{
                connection.prepareStatement(anyString);
                result = nativeExecption;
            }};
            try {
                target.prepareStatementBySqlId(SQL_ID_1, selectOption);
                fail("ここは通らない");
            } catch (DbAccessException e) {
                 message = e.getMessage();
                 assertThat(e.getCause(), is(nativeExecption));
            }
            assertThat(message, is(format("failed to prepareStatementBySqlId. SQL_ID = [%s], %s", SQL_ID_1, selectOption)));
    }

    /**
     * ステートメントに正しくSelectOptionが渡されるか確認する。
     */
    @Test
    public void testParameterizedSqlPStatementWithOption(@Mocked final Connection connection) throws Exception {
        BasicDbConnection target = createTarget(connection);
        target.setContext(new DbExecutionContext(target, new DefaultDialect(), TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
        SelectOption selectOption = new SelectOption(3, 4);
        ParameterizedSqlPStatement ps = target.prepareParameterizedSqlStatement(SELECT_QUERY, selectOption);
        SelectOption actual = (SelectOption) Deencapsulation.getField(ps, "selectOption");
        assertThat(actual, is(selectOption));
    }

    /**
     * Optionを指定する場合の再利用のテスト。
     */
    @Test
    public void testParameterizedSqlPStatementWithOptionReuseOn(@Mocked final Connection connection) throws Exception {
        BasicDbConnection target = createTarget(connection);
        target.setContext(new DbExecutionContext(target, new DefaultDialect(), TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
        target.setStatementReuse(true);
        assertThat("同一のオプションなので、同じインスタンスが返却される。",
                target.prepareParameterizedSqlStatement(SELECT_QUERY, new SelectOption(3, 4)),
                sameInstance(target.prepareParameterizedSqlStatement(SELECT_QUERY, new SelectOption(3, 4))));
        assertThat("オプションが異なるので、別のインスタンスが返却される。",
                target.prepareParameterizedSqlStatement(SELECT_QUERY, new SelectOption(3, 4)),
                not(sameInstance(target.prepareParameterizedSqlStatement(SELECT_QUERY, new SelectOption(3, 5)))));
    }

    /**
     * Optionを指定する場合の再利用のテスト。
     */
    @Test
    public void testParameterizedSqlPStatementWithOptionReuseOff(@Mocked final Connection connection) throws Exception {
        BasicDbConnection target = createTarget(connection);
        target.setContext(new DbExecutionContext(target, new DefaultDialect(), TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
        target.setStatementReuse(false);
        assertThat("キャッシュしないので、異なるインスタンスが返却される",
                target.prepareParameterizedSqlStatement(SELECT_QUERY, new SelectOption(3, 4)),
                not(sameInstance(target.prepareParameterizedSqlStatement(SELECT_QUERY, new SelectOption(3, 4)))));
    }

    /**
     * ステートメントの生成に失敗した場合のテスト。
     */
    @Test
    public void testParameterizedSqlPStatementWithOptionFail(@Mocked final Connection connection) throws Exception {
            BasicDbConnection target = createTarget(connection);
            final Throwable nativeExecption = new SQLException("statement作成時にエラー");
            String message = "";
            SelectOption selectOption = new SelectOption(4, 5);
            new Expectations() {{
                connection.prepareStatement(anyString);
                result = nativeExecption;
            }};
            try {
                target.prepareParameterizedSqlStatement(SELECT_QUERY, selectOption);
                fail("ここは通らない");
            } catch (DbAccessException e) {
                 message = e.getMessage();
                 assertThat(e.getCause(), is(nativeExecption));
            }
            assertThat(message, is(format("failed to prepareParameterizedSqlStatement. SQL = [%s], %s" ,SELECT_QUERY, selectOption)));
    }

    /**
     * ステートメントに正しくSelectOptionが渡されるか確認する。
     */
    @Test
    public void testPrepareParameterizedSqlStatementBySqlId(@Mocked final Connection connection) throws Exception {
        BasicDbConnection target = createTarget(connection);
        target.setContext(new DbExecutionContext(target, new DefaultDialect(), TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
        ParameterizedSqlPStatement ps = target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, new SelectOption(2, 3));
        SelectOption selectOption = (SelectOption) Deencapsulation.getField(ps, "selectOption");
        assertThat(selectOption.getOffset(), is(1));
        assertThat(selectOption.getLimit(), is(3));
    }

    /**
     * Optionを指定する場合の再利用のテスト。
     */
    @Test
    public void testPrepareParameterizedSqlStatementBySqlIdWithOptionReuseOn(@Mocked final Connection connection) throws Exception {
        BasicDbConnection target = createTarget(connection);
        target.setStatementReuse(true);
        target.setContext(new DbExecutionContext(target, new DefaultDialect(), TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
        assertThat("同一のオプションなので、同じインスタンスが返却される。",
                target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, new SelectOption(2, 3)),
                sameInstance(target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, new SelectOption(2, 3))));
        assertThat("オプションが異なるので、異なるインスタンスが返却される。",
                target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, new SelectOption(2, 3)),
                not(sameInstance(target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, new SelectOption(2, 4)))));
    }

    /**
     * Optionを指定する場合の再利用のテスト。
     */
    @Test
    public void testPrepareParameterizedSqlStatementBySqlIdWithOptionReuseOff(@Mocked final Connection connection) throws Exception {
        BasicDbConnection target = createTarget(connection);
        target.setStatementReuse(false);
        target.setContext(new DbExecutionContext(target, new DefaultDialect(), TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
        assertThat("キャッシュしないので別のインスタンスが返却される",
                target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, new SelectOption(2, 3)),
                not(sameInstance(target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, new SelectOption(2, 3)))));
    }

    /**
     * ステートメントの生成に失敗した場合のテスト。
     */
    @Test
    public void testPrepareParameterizedSqlStatementBySqlIdWithOptionFail(@Mocked final Connection connection) throws Exception {
            BasicDbConnection target = createTarget(connection);
            final Throwable nativeExecption = new SQLException("statement作成時にエラー");
            String message = "";
            SelectOption selectOption = new SelectOption(4, 5);
            new Expectations() {{
                connection.prepareStatement(anyString);
                result = nativeExecption;
            }};
            try {
                target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, selectOption);
                fail("ここは通らない");
            } catch (DbAccessException e) {
                 message = e.getMessage();
                 assertThat(e.getCause(), is(nativeExecption));
            }
            assertThat(message, is(format("failed to prepareParameterizedSqlStatementBySqlId. SQL_ID = [%s], %s", SQL_ID_1, selectOption)));
    }

    /**
     * ステートメントに正しくSelectOptionが渡されるか確認する。
     */
    @Test
    public void testParameterizedSqlPStatementWithConditionWithOption(@Mocked final Connection connection) throws Exception {
        BasicDbConnection target = createTarget(connection);
        target.setContext(new DbExecutionContext(target, new DefaultDialect(), TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
        Object condition = new Object();
        SelectOption selectOption = new SelectOption(3, 4);
        ParameterizedSqlPStatement ps = target.prepareParameterizedSqlStatement(SELECT_QUERY, condition, selectOption);
        SelectOption actual = (SelectOption) Deencapsulation.getField(ps, "selectOption");
        assertThat(actual, is(selectOption));
    }

    /**
     * Optionを指定する場合の再利用のテスト。
     */
    @Test
    public void testParameterizedSqlPStatementWithConditionWithOptionReuseOn(@Mocked final Connection connection) throws Exception {
        BasicDbConnection target = createTarget(connection);
        target.setStatementReuse(true);
        target.setContext(new DbExecutionContext(target, new DefaultDialect(), TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
        Object condition = new Object();
        assertThat("同一のオプションなので、同じインスタンスが返却される。",
                target.prepareParameterizedSqlStatement(SELECT_QUERY, condition, new SelectOption(3, 4)),
                sameInstance(target.prepareParameterizedSqlStatement(SELECT_QUERY, condition, new SelectOption(3, 4))));
        assertThat("オプションが異なるので、別のインスタンスが返却される。",
                target.prepareParameterizedSqlStatement(SELECT_QUERY, condition, new SelectOption(3, 4)),
                not(sameInstance(target.prepareParameterizedSqlStatement(SELECT_QUERY, condition, new SelectOption(3, 5)))));
    }

    /**
     * Optionを指定する場合の再利用のテスト。
     */
    @Test
    public void testParameterizedSqlPStatementWithConditionWithOptionReuseOff(@Mocked final Connection connection) throws Exception {
        BasicDbConnection target = createTarget(connection);
        target.setStatementReuse(false);
        target.setContext(new DbExecutionContext(target, new DefaultDialect(), TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
        Object condition = new Object();
        assertThat("キャッシュしないので別のインスタンスが返却される",
                target.prepareParameterizedSqlStatement(SELECT_QUERY, condition, new SelectOption(3, 4)),
                not(sameInstance(target.prepareParameterizedSqlStatement(SELECT_QUERY, condition, new SelectOption(3, 4)))));
    }

    /**
     * ステートメントに正しくSelectOptionが渡されるか確認する。
     */
    @Test
    public void testParameterizedSqlPStatementWithConditionBySqlIdWithOption(@Mocked final Connection connection) throws Exception {
        BasicDbConnection target = createTarget(connection);
        target.setContext(new DbExecutionContext(target, new DefaultDialect(), TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
        Object condition = new Object();
        ParameterizedSqlPStatement ps = target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, condition, new SelectOption(3, 4));
        SelectOption selectOption = (SelectOption) Deencapsulation.getField(ps, "selectOption");
        assertThat(selectOption.getOffset(), is(2));
        assertThat(selectOption.getLimit(), is(4));
    }

    /**
     * Optionを指定する場合の再利用のテスト。
     */
    @Test
    public void testParameterizedSqlPStatementWithConditionBySqlIdWithOptionReuseOn(@Mocked final Connection connection) throws Exception {
        BasicDbConnection target = createTarget(connection);
        target.setStatementReuse(true);
        target.setContext(new DbExecutionContext(target, new DefaultDialect(), TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
        Object condition = new Object();
        assertThat("同一のオプションなので、同じインスタンスが返却される。",
                target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, condition, new SelectOption(3, 4)),
                sameInstance(target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, condition, new SelectOption(3, 4))));
        assertThat("オプションが異なるので、別のインスタンスが返却される。",
                target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, condition, new SelectOption(3, 4)),
                not(sameInstance(target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, condition, new SelectOption(3, 5)))));
    }

    /**
     * Optionを指定する場合の再利用のテスト。
     */
    @Test
    public void testParameterizedSqlPStatementWithConditionBySqlIdWithOptionReuseOff(@Mocked final Connection connection) throws Exception {
        BasicDbConnection target = createTarget(connection);
        target.setStatementReuse(false);
        target.setContext(new DbExecutionContext(target, new DefaultDialect(), TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
        Object condition = new Object();
        assertThat("キャッシュしないので別のインスタンスが返却される",
                target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, condition, new SelectOption(3, 4)),
                not(sameInstance(target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, condition, new SelectOption(3, 4)))));
    }

    /**
     * ステートメントの生成に失敗した場合のテスト。
     */
    @Test
    public void testParameterizedSqlPStatementWithConditionBySqlIdWithOptionFail(@Mocked final Connection connection) throws Exception {
            BasicDbConnection target = createTarget(connection);
            final Throwable nativeExecption = new SQLException("statement作成時にエラー");
            String message = "";
            SelectOption selectOption = new SelectOption(4, 5);
            Object condition = new Object();
            new Expectations() {{
                connection.prepareStatement(anyString);
                result = nativeExecption;
            }};
            try {
                target.prepareParameterizedSqlStatementBySqlId(SQL_ID_1, condition , selectOption);
                fail("ここは通らない");
            } catch (DbAccessException e) {
                 message = e.getMessage();
                 assertThat(e.getCause(), is(nativeExecption));
            }
            assertThat(message, is(format("failed to prepareParameterizedSqlStatementBySqlId. SQL_ID = [%s], %s", SQL_ID_1, selectOption)));
    }

    /**
     * {@link BasicDbConnection#removeStatement(SqlStatement)}のテスト。
     */
    @Test
    public void removeStatement() throws Exception {
        // リソース開放対象のステートメントリストを取得する。
        final List<SqlStatement> statements = Deencapsulation.getField(sut, "statements");

        assertThat("リソース開放対象のステートメントリストは空であること", statements.isEmpty(), is(true));

        final SqlPStatement st1 = sut.prepareStatement("SELECT * FROM USER_TEST");
        assertThat("ステートメントを生成したのでサイズが増えること", statements.size(), is(1));

        final SqlPStatement st2 = sut.prepareStatement("SELECT * FROM USER_TEST WHERE 1 = 1");
        assertThat("ステートメントを生成したのでサイズが増えること", statements.size(), is(2));

        final SqlPStatement st3 = sut.prepareStatement("SELECT * FROM USER_TEST WHERE 1 = 2");
        assertThat("ステートメントを生成したのでサイズが増えること", statements.size(), is(3));

        sut.removeStatement(st2);
        assertThat("ステートメントを削除したのでリストが減る", statements.size(), is(2));
        assertThat("ステートメントリストからst2が削除されていること", statements.contains(st2), is(false));

        sut.removeStatement(st1);
        assertThat("ステートメントを削除したのでリストが減る", statements.size(), is(1));
        assertThat("ステートメントリストからst1が削除されていること", statements.contains(st1), is(false));

        sut.removeStatement(st1);
        assertThat("削除済みのステートメントを指定した場合サイズは変わらないこと", statements.size(), is(1));

        sut.removeStatement(st3);
        assertThat("空になること", statements.isEmpty(), is(true));
        st1.close();
        st2.close();
        st3.close();
    }

    /**
     * モックのコネクションを使って{@link BasicDbConnection}を生成する。
     *
     * @param mockedConnection モックのコネクション
     * @return 生成したBasicConnection
     */
    private BasicDbConnection createTarget(final Connection mockedConnection) {
        BasicDbConnection target = new BasicDbConnection(mockedConnection);
        target.setFactory(createStatementFactory());
        target.setContext(createContext(target));
        target.setDbAccessExceptionFactory(new BasicDbAccessExceptionFactory());
        return target;
    }

    /**
     * テスト用のコンテキストを生成する。
     *
     * @return コンテキスト
     */
     private DbExecutionContext createContext(BasicDbConnection connection) {
        return new DbExecutionContext(connection, new DefaultDialect(), TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
     }

     private BasicStatementFactory createStatementFactory() {
         BasicStatementFactory factory = new BasicStatementFactory();
         factory.setSqlParameterParserFactory(new BasicSqlParameterParserFactory());
         BasicSqlLoader loader = new BasicSqlLoader();
         factory.setSqlLoader(loader);
         return factory;
     }

    /**
     * テストで利用するエンティティ。
     *
     * @author tani takanori
     */
    @Entity
    @Table(name="USER_TEST")
    public static class UserTestEntity {
        @Id
        @Column(name = "USER_ID", length = 4, nullable = false)
        public String userId;

        @Column(name = "USER_NAME", length = 10, nullable = false)
        public String userName;

        @Column(name = "TEL", length = 12, nullable = false)
        public String tel;

        public String getUserId() {
            return userId;
        }

        public String getUserName() {
            return userName;
        }

        public String getTel() {
            return tel;
        }
    }
}
