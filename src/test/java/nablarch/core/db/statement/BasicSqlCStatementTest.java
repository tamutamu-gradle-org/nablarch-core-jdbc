package nablarch.core.db.statement;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import nablarch.core.db.DbAccessException;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.util.DateUtil;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import nablarch.test.support.log.app.OnMemoryLogWriter;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import mockit.Expectations;
import mockit.Mocked;


/**
 * {@link BasicSqlCStatement}のテストクラス。
 *
 * @author hisaaki sioiri
 */
@RunWith(DatabaseTestRunner.class)
@TargetDb(include = TargetDb.Db.ORACLE)
public class BasicSqlCStatementTest {

    @ClassRule
    public static final SystemRepositoryResource SYSTEM_REPOSITORY_RESOURCE = new SystemRepositoryResource("db-default.xml");

    private static final String CALL_TEST_PROP = "{call TEST_PROP(?, ?, ?)}";

    private static final String CALL_TEST_FUNC = "{? = call test_func(?, ?)}";


    /** テストで使用するデータベース接続 */
    private static TransactionManagerConnection testConnection;

    private SqlCStatement sut;

    @BeforeClass
    public static void setUpClass() throws Exception {
        final ConnectionFactory connectionFactory = SYSTEM_REPOSITORY_RESOURCE.getComponentByType(
                ConnectionFactory.class);
        testConnection = connectionFactory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        setupDatabase(testConnection);
    }

    private static void setupDatabase(TransactionManagerConnection connection) {

        final SqlPStatement createProcedure = connection.prepareStatement(
                "CREATE OR REPLACE  PROCEDURE TEST_PROP(P1 IN VARCHAR2, O1 OUT NUMBER, O2 IN OUT NUMBER)"
                        + " AS "
                        + " BEGIN"
                        + "   O1 := to_number(P1) * 3; "
                        + "   IF O2 IS NULL THEN"
                        + "     O2 := 123.456;"
                        + "   END IF;"
                        + " END;");
        createProcedure.execute();
        createProcedure.close();

        final SqlPStatement createFunc = connection.prepareStatement(
                "CREATE OR REPLACE FUNCTION test_func(P1 IN VARCHAR2, P2 NUMBER DEFAULT 0) RETURN PLS_INTEGER"
                        + " AS"
                        + " BEGIN "
                        + " RETURN to_number(P1) + P2;"
                        + " END;");
        createFunc.execute();
        createFunc.close();

        VariousDbTestHelper.createTable(TestTable.class);

        final SqlPStatement insertProc = connection.prepareStatement(
                "CREATE OR  REPLACE PROCEDURE INSERT_PROC(P IN VARCHAR2, INSERT_COUNT OUT PLS_INTEGER)"
                        + " IS"
                        + " BEGIN"
                        + " INSERT INTO CSTATEMENT_TEST_TABLE(U_ID, TEXT) VALUES (1, P);"
                        + " INSERT_COUNT := SQL%ROWCOUNT;"
                        + " END;"
        );
        insertProc.execute();
        insertProc.close();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        testConnection.terminate();
    }

    @Before
    public void setUp() throws Exception {
        DbConnectionContext.removeConnection();
        OnMemoryLogWriter.clear();
        VariousDbTestHelper.delete(TestTable.class);
    }

    @After
    public void tearDown() throws Exception {
        DbConnectionContext.removeConnection();
        if (sut != null) {
            try {
                sut.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        testConnection.rollback();
    }

    /**
     * {@link BasicSqlCStatement#registerOutParameter(int, int)}のテスト。
     * <p/>
     * 設定した値がログに出力されることを確認する。
     * また、ストアド・プロシージャ実行後に値が取得できることを確認する。
     */
    @Test
    public void registerOutParameter_normal(@Mocked CallableStatement statement) {
        sut = new BasicSqlCStatement("sql", statement);
        sut.registerOutParameter(1, Types.CHAR);
        sut.registerOutParameter(2, Types.DECIMAL);
        sut.registerOutParameter(3, Types.SMALLINT);
        sut.registerOutParameter(4, Types.BLOB);
        sut.execute();
        assertLog("ログ：out param 1",
                Pattern.compile(String.format("1\\(out param\\) = \\[type:%d\\]", Types.CHAR)));
        assertLog("ログ：out param 2",
                Pattern.compile(String.format("2\\(out param\\) = \\[type:%d\\]", Types.DECIMAL)));
        assertLog("ログ：out param 3",
                Pattern.compile(String.format("3\\(out param\\) = \\[type:%d\\]", Types.SMALLINT)));
        assertLog("ログ：out param 4",
                Pattern.compile(String.format("4\\(out param\\) = \\[type:%d\\]", Types.BLOB)));

    }

    /**
     * {@link BasicSqlCStatement#registerOutParameter(int, int)}のテスト。
     * <p/>
     * registerOutParameter呼び出し時にSQLExceptionが発生するケース。
     */
    @Test
    public void registerOutParameter_exception(@Mocked final CallableStatement statement) throws Exception {
        new Expectations() {{
            statement.registerOutParameter(2, Types.INTEGER);
            result = new SQLException("registerOutParameter error.");
        }};

        DbAccessException exception = null;
        sut = new BasicSqlCStatement("sql", statement);
        try {
            sut.registerOutParameter(2, Types.INTEGER);
        } catch (DbAccessException e) {
            exception = e;
        }
        assertThat("例外のメッセージ", exception.getMessage(),
                is(MessageFormat.format("failed to registerOutParameter(2, {0})", Types.INTEGER)));
    }

    /**
     * {@link BasicSqlCStatement#registerOutParameter(int, int, int)}のテスト。
     * <p/>
     * 設定した値がログに出力されることを確認する。
     * また、ストアド・プロシージャ実行後に値が取得できることを確認する。
     */
    @Test
    public void registerOutParameterWithScale_normal(@Mocked CallableStatement statement) throws Exception {
        sut = new BasicSqlCStatement("sql", statement);
        sut.registerOutParameter(1, Types.DECIMAL, 10);
        sut.execute();
        assertLog("イン・アウトパラメータのOUT情報がログに出力されていること ",
                Pattern.compile(String.format("1\\(out param\\) = \\[type:%d, scale:%d", Types.DECIMAL, 10)));
    }

    /**
     * {@link BasicSqlCStatement#registerOutParameter(int, int, int)}のテスト。
     * <p/>
     * registerOutParameter呼び出し時にSQLExceptionが発生するケース。
     */
    @Test
    public void registerOutParameterWithScale_exception(@Mocked final CallableStatement statement) throws Exception {
        new Expectations() {{
            statement.registerOutParameter(1, Types.BIGINT, 2);
            result = new SQLException("registerOutParameter error.");
        }};

        sut = new BasicSqlCStatement("sql", statement);

        DbAccessException exception = null;
        try {
            sut.registerOutParameter(1, Types.BIGINT, 2);
        } catch (DbAccessException e) {
            exception = e;
        }
        assertThat("例外のメッセージ", exception.getMessage(),
                is(MessageFormat.format("failed to registerOutParameter(1, {0}, 2)", Types.BIGINT)));
    }

    /**
     * {@link BasicSqlCStatement#execute()}でのプロシージャ実行のテスト。
     * <p/>
     * ログにパラメータ情報が出力されること。
     * 実行結果のアウトパラメータが取得できること。
     */
    @Test
    public void executeProcedure() throws Exception {
        sut = testConnection.prepareCall(CALL_TEST_PROP);
        sut.clearParameters();
        sut.setString(1, "123");
        sut.setNull(3, Types.DECIMAL);
        sut.registerOutParameter(2, Types.BIGINT);
        sut.registerOutParameter(3, Types.DECIMAL);
        final boolean hasResultSet = sut.execute();

        assertThat("結果セットは返さないのでfalse", hasResultSet, is(false));

        assertThat("イン・アウトパラメータが取得できること", sut.getLong(2), is(369L));
        assertThat("アウトパラメータが取得できること", sut.getBigDecimal(3), is(new BigDecimal("123.456")));

        assertLog("SQL実行ログが出力されていること", Pattern.compile("DEBUG.+#execute.+SQL = \\Q[{call TEST_PROP(?, ?, ?)}\\E.+"));
        assertLog("パラメータ[1]のログが出力されていること", Pattern.compile("01 = \\[123\\]"));
        assertLog("パラメータ[2]のアウトパラメータがログ出力されていること",
                Pattern.compile(String.format("2\\(out param\\) = \\[type:%d\\]", Types.BIGINT)));
        assertLog("パラメータ[3]のインパラメータがログに出力されていること",
                Pattern.compile("03 = \\[null\\]"));
        assertLog("パラメータ[3]のアウトパラメータがログに出力されていること",
                Pattern.compile(String.format("3\\(out param\\) = \\[type:%d\\]", Types.DECIMAL)));
    }

    /**
     * {@link BasicSqlCStatement#execute()}のファンクション実行のテスト。
     * <p/>
     * ログにパラメータ情報が出力されること。
     * 実行結果の戻り値が取得できること。
     *
     * @throws Exception
     */
    @Test
    public void executeFunction() throws Exception {
        sut = testConnection.prepareCall(CALL_TEST_FUNC);
        sut.registerOutParameter(1, Types.NUMERIC);
        sut.setString(2, "100");
        sut.setInt(3, 50);
        sut.execute();

        assertThat("ファンクションの戻り値が取得できること。", sut.getInteger(1), is(150));

        assertLog("SQL実行ログが出力されていること", Pattern.compile("DEBUG.+#execute.+SQL = \\Q[" + CALL_TEST_FUNC + "\\E.+"));
        assertLog("戻り値のログが出力されていること", Pattern.compile(String.format("1\\(out param\\) = \\[type:%d\\]",
                Types.NUMERIC)));
        assertLog("パラメータ情報のログが出力されていること", Pattern.compile("02 = \\[100\\]"));
        assertLog("パラメータ情報のログが出力されていること", Pattern.compile("03 = \\[50\\]"));
    }

    /**
     * {@link BasicSqlCStatement#executeUpdate()}でプロシージャ実行のテスト。
     * <p/>
     * テスト内容的には、{@link #executeProcedure()}と同じ
     *
     * @throws Exception
     */
    @Test
    public void executeUpdateProcedure() throws Exception {
        sut = testConnection.prepareCall(CALL_TEST_PROP);
        sut.clearParameters();
        sut.setString(1, "123");
        sut.setNull(3, Types.DECIMAL);
        sut.registerOutParameter(2, Types.BIGINT);
        sut.registerOutParameter(3, Types.DECIMAL);
        sut.executeUpdate();

        assertThat("イン・アウトパラメータが取得できること", sut.getLong(2), is(369L));
        assertThat("アウトパラメータが取得できること", sut.getBigDecimal(3), is(new BigDecimal("123.456")));

        assertLog("SQL実行ログが出力されていること", Pattern.compile(
                "DEBUG.+#executeUpdate.+SQL = \\Q[{call TEST_PROP(?, ?, ?)}\\E.+"));
        assertLog("パラメータ[1]のログが出力されていること", Pattern.compile("01 = \\[123\\]"));
        assertLog("パラメータ[2]のアウトパラメータがログ出力されていること",
                Pattern.compile(String.format("2\\(out param\\) = \\[type:%d\\]", Types.BIGINT)));
        assertLog("パラメータ[3]のインパラメータがログに出力されていること",
                Pattern.compile("03 = \\[null\\]"));
        assertLog("パラメータ[3]のアウトパラメータがログに出力されていること",
                Pattern.compile(String.format("3\\(out param\\) = \\[type:%d\\]", Types.DECIMAL)));
    }

    /**
     * {@link BasicSqlCStatement#executeUpdate()}でファンクション実行のテスト。
     * <p/>
     * テストの内容的には、{@link #executeFunction()}と同じ
     *
     * @throws Exception
     */
    @Test
    public void executeUpdateFunction() throws Exception {
        sut = testConnection.prepareCall(CALL_TEST_FUNC);
        sut.registerOutParameter(1, Types.NUMERIC);
        sut.setString(2, "100");
        sut.setInt(3, 50);
        sut.executeUpdate();

        assertThat("ファンクションの戻り値が取得できること。", sut.getInteger(1), is(150));

        assertLog("SQL実行ログが出力されていること", Pattern.compile("DEBUG.+#executeUpdate.+SQL = \\Q[" + CALL_TEST_FUNC + "\\E.+"));
        assertLog("戻り値のログが出力されていること", Pattern.compile(String.format("1\\(out param\\) = \\[type:%d\\]",
                Types.NUMERIC)));
        assertLog("パラメータ情報のログが出力されていること", Pattern.compile("02 = \\[100\\]"));
        assertLog("パラメータ情報のログが出力されていること", Pattern.compile("03 = \\[50\\]"));
    }

    @Test
    public void executeFromSQLID() throws Exception {
        sut = testConnection.prepareCallBySqlId(this.getClass()
                .getName() + "#proc");
        sut.setString(1, "ほげ");
        sut.registerOutParameter(2, Types.INTEGER);
        sut.execute();

        assertThat("アウトパラメータの値が取得できる。", sut.getInteger(2), is(1));

        assertLog("SQLID情報がログに出力されていること", Pattern.compile(
                "additionalInfo = \\Q[SQL_ID = [nablarch.core.db.statement.BasicSqlCStatementTest#proc]]\\E"));
    }

    /**
     * トランザクションのコミットが有効にきいていることを確認する。
     *
     * @throws Exception
     */
    @Test
    public void executeWithTransactionCommit() throws Exception {
        // ------------------------------ execute
        sut = testConnection.prepareCall("{call INSERT_PROC(?, ?)}");
        sut.setString(1, "なまえ");
        sut.registerOutParameter(2, Types.INTEGER);
        sut.execute();

        testConnection.commit();

        // ------------------------------ assert
        assertThat("更新件数は1", sut.getInteger(2), is(1));

        final List<TestTable> result = VariousDbTestHelper.findAll(TestTable.class);
        assertThat("コミットされたデータが1件取得できる", result.size(), is(1));
        assertThat("パラメータで指定したデータが取得できる", result.get(0)
                .text, is("なまえ"));

        // ロールバックしても、データが取得できることを確認する。
        testConnection.rollback();
        final List<TestTable> afterRollback = VariousDbTestHelper.findAll(TestTable.class);
        assertThat("コミット済みなのでロールバックしてもデータは消えない", afterRollback.size(), is(1));
    }

    /**
     * トランザクションが有効にきいていることを確認する。
     * ロールバックしたらデータが消えることで確認。
     *
     * @throws Exception
     */
    @Test
    public void executeWithTransactionRollback() throws Exception {
        // ------------------------------ execute
        sut = testConnection.prepareCall("{call INSERT_PROC(?, ?)}");
        sut.setString(1, "データ");
        sut.registerOutParameter(2, Types.INTEGER);
        sut.execute();

        assertThat("更新件数は1", sut.getInteger(2), is(1));
        // ------------------------------ assert

        // ロールバックしてデータは取得できなくなる。
        testConnection.rollback();
        final List<TestTable> afterRollback = VariousDbTestHelper.findAll(TestTable.class);
        assertThat("ロールバックしたのでデータはなくなっている。", afterRollback.size(), is(0));
    }

    /**
     * {@link BasicSqlCStatement#getObject(int)}のテスト。
     * <p/>
     * アウトパラメータの値が取得できること。
     */
    @Test
    public void getObject() throws Exception {
        sut = testConnection.prepareCall("BEGIN"
                + " ? := '123';"
                + " ? := 123;"
                + " ? := to_blob('333231');"
                + " ? := to_clob('12345');"
                + " ? := to_timestamp('2015-01-29 11:22:33.444');"
                + " ? := to_nchar('1234512345');"
                + " ? := NULL;"
                + " END;");
        sut.registerOutParameter(1, Types.CHAR);
        sut.registerOutParameter(2, Types.SMALLINT);
        sut.registerOutParameter(3, Types.BINARY);
        sut.registerOutParameter(4, Types.VARCHAR);
        sut.registerOutParameter(5, Types.TIMESTAMP);
        sut.registerOutParameter(6, Types.NCHAR);
        sut.registerOutParameter(7, Types.VARCHAR);
        sut.execute();

        assertThat("index:1", sut.getObject(1), is((Object) "123"));
        assertThat("index:2", sut.getObject(2), is((Object) Short.valueOf("123")));
        assertThat("index:3", sut.getObject(3), is((Object) "321".getBytes()));
        assertThat("index:4", sut.getObject(4), is((Object) "12345"));
        assertThat("index:5", sut.getObject(5), is((Object) Timestamp.valueOf("2015-01-29 11:22:33.444")));
        assertThat("index:6", sut.getObject(6), is((Object) "1234512345"));
        assertThat("index:7", sut.getObject(7), is(nullValue()));
    }

    /**
     * {@link BasicSqlCStatement#getObject(int)}のテスト。
     * <p/>
     * {@link SQLException}が発生するケース。
     *
     * @throws Exception
     */
    @Test
    public void getObject_SQLException(@Mocked final CallableStatement statement) throws Exception {
        new Expectations() {{
            statement.getObject(anyInt);
            result = new SQLException("getObject error.");
        }};

        sut = new BasicSqlCStatement("sql", statement);
        DbAccessException exception = null;

        try {
            sut.getObject(3);
        } catch (DbAccessException e) {
            exception = e;
        }
        assertThat("例外が発生すること。", exception.getMessage(), is("failed to getObject(3)"));
    }

    /**
     * {@link BasicSqlCStatement#getString(int)}のテスト。
     * <p/>
     * アウトパラメータの値が文字列で取得できること。
     */
    @Test
    public void getString() throws Exception {
        sut = testConnection.prepareCall("BEGIN ? := 'abcdefg'; ? := '12345'; ? := NULL; END;");
        sut.registerOutParameter(1, Types.VARCHAR);
        sut.registerOutParameter(2, Types.NUMERIC);
        sut.registerOutParameter(3, Types.VARCHAR);
        sut.execute();
        assertThat("index:1", sut.getString(1), is("abcdefg"));
        assertThat("index:2", sut.getString(2), is("12345"));
        assertThat("index:3", sut.getString(3), is(nullValue()));
    }

    /**
     * {@link BasicSqlCStatement#getString(int)}のテスト。
     * <p/>
     * SQLExceptionが発生した場合のケース
     */
    @Test
    public void getString_error(@Mocked final CallableStatement statement) throws Exception {
        new Expectations() {{
            statement.getString(anyInt);
            result = new SQLException("getString error.");
        }};
        sut = new BasicSqlCStatement("sql", statement);

        DbAccessException exception = null;
        try {
            sut.getString(1);
        } catch (DbAccessException e) {
            exception = e;
        }
        assertThat("例外が発生していること", exception.getMessage(), is("failed to getString(1)"));
    }

    /**
     * {@link BasicSqlCStatement#getInteger(int)}のテスト。
     * <p/>
     * アウトパラメータの値がIntegerで取得できること。
     */
    @Test
    public void getInteger() throws Exception {
        sut = testConnection.prepareCall("BEGIN ? := 1; ? := '12345'; ?:= NULL; END;");
        sut.registerOutParameter(1, Types.INTEGER);
        sut.registerOutParameter(2, Types.VARCHAR);
        sut.registerOutParameter(3, Types.INTEGER);
        sut.execute();

        assertThat("index:1", sut.getInteger(1), is(1));
        assertThat("index:2", sut.getInteger(2), is(12345));
        assertThat("index:3", sut.getInteger(3), is(nullValue()));
    }

    /**
     * {@link BasicSqlCStatement#getInteger(int)}のテスト。
     * <p/>
     * 非数値型の値を読み取った場合、{@link java.lang.NumberFormatException}が発生すること。
     *
     * @throws Exception
     */
    @Test(expected = NumberFormatException.class)
    public void getInteger_notNumeric() throws Exception {
        sut = testConnection.prepareCall("BEGIN ? := 'abc'; END;");
        sut.registerOutParameter(1, Types.CHAR);
        sut.execute();
        sut.getInteger(1);
    }

    /**
     * {@link BasicSqlCStatement#getInteger(int)}のテスト。
     * <p/>
     * {@link SQLException}が発生するケース
     *
     * @throws Exception
     */
    @Test
    public void getInteger_SQLException(@Mocked final CallableStatement statement) throws Exception {
        new Expectations() {{
            statement.getObject(anyInt);
            result = new SQLException("getInteger error.");
        }};
        sut = new BasicSqlCStatement("sql", statement);

        DbAccessException exception = null;
        try {
            sut.getInteger(10);
        } catch (DbAccessException e) {
            exception = e;
        }
        assertThat("例外が発生すること", exception.getMessage(), is("failed to getInteger(10)"));
    }

    /**
     * {@link BasicSqlCStatement#getLong(int)}のテスト。
     * <p/>
     * アウトパラメータの値が取得できること。
     *
     * @throws Exception
     */
    @Test
    public void getLong() throws Exception {
        sut = testConnection.prepareCall("BEGIN ? := 100; ?:= '1234554321'; ?:= NULL; END;");
        sut.registerOutParameter(1, Types.INTEGER);
        sut.registerOutParameter(2, Types.VARCHAR);
        sut.registerOutParameter(3, Types.BIGINT);
        sut.execute();

        assertThat("index:1", sut.getLong(1), is(100L));
        assertThat("index:2", sut.getLong(2), is(1234554321L));
        assertThat("index:3", sut.getLong(3), is(nullValue()));
    }

    /**
     * {@link BasicSqlCStatement#getLong(int)}のテスト。
     * 非数値型の値を読み取った場合、{@link java.lang.NumberFormatException}が発生すること。
     *
     * @throws Exception
     */
    @Test(expected = NumberFormatException.class)
    public void getLong_notNumeric() throws Exception {
        sut = testConnection.prepareCall("BEGIN ? := 'aa'; END;");
        sut.registerOutParameter(1, Types.VARCHAR);
        sut.execute();
        sut.getLong(1);
    }

    /**
     * {@link BasicSqlCStatement#getLong(int)}のテスト。
     * <p/>
     * {@link SQLException}が発生するケース
     *
     * @throws Exception
     */
    @Test
    public void getLong_SQLException(@Mocked final CallableStatement statement) throws Exception {
        new Expectations() {{
            statement.getObject(anyInt);
            result = new SQLException("getLong error.");
        }};
        sut = new BasicSqlCStatement("sql", statement);

        DbAccessException exception = null;
        try {
            sut.getLong(2);
        } catch (DbAccessException e) {
            exception = e;
        }
        assertThat("例外が発生すること", exception.getMessage(), is("failed to getLong(2)"));
    }

    /**
     * {@link BasicSqlCStatement#getShort(int)}のテスト。
     * <p/>
     * アウトパラメータの値が取得できること
     *
     * @throws Exception
     */
    @Test
    public void getShort() throws Exception {
        sut = testConnection.prepareCall("BEGIN ?:= 12345; ?:= -1234; ?:=NULL; END;");
        sut.registerOutParameter(1, Types.SMALLINT);
        sut.registerOutParameter(2, Types.INTEGER);
        sut.registerOutParameter(3, Types.INTEGER);
        sut.execute();

        assertThat("index:1", sut.getShort(1), is(Short.valueOf("12345")));
        assertThat("index:2", sut.getShort(2), is(Short.valueOf("-1234")));
        assertThat("index:3", sut.getShort(3), is(nullValue()));
    }

    @Test
    public void getShort_SQLException(@Mocked final CallableStatement statement) throws Exception {
        new Expectations() {{
            statement.getObject(anyInt);
            result = new SQLException("getShort error.");
        }};

        DbAccessException exception = null;
        sut = new BasicSqlCStatement("sql", statement);

        try {
            sut.getShort(3);
        } catch (DbAccessException e) {
            exception = e;
        }

        assertThat("例外が発生していること", exception.getMessage(), is("failed to getShort(3)"));
    }

    /**
     * {@link BasicSqlCStatement#getBigDecimal(int)}のテスト。
     * <p/>
     * アウトパラメータの値が取得できること
     *
     * @throws Exception
     */
    @Test
    public void getBigDecimal() throws Exception {
        sut = testConnection.prepareCall("BEGIN ? := 123.45; ?:='12345.12345'; ?:= NULL; END;");
        sut.registerOutParameter(1, Types.DECIMAL, 2);
        sut.registerOutParameter(2, Types.DECIMAL, 3);
        sut.registerOutParameter(3, Types.NUMERIC, 4);
        sut.execute();

        assertThat("index:1", sut.getBigDecimal(1), is(new BigDecimal("123.45")));
        assertThat("index:2", sut.getBigDecimal(2), is(new BigDecimal("12345.12345")));
        assertThat("index:3", sut.getBigDecimal(3), is(nullValue()));
    }

    /**
     * {@link BasicSqlCStatement#getBigDecimal(int)}のテスト。
     * <p/>
     * {@link SQLException}が発生するケース
     *
     * @throws Exception
     */
    @Test
    public void getBigDecimal_SQLException(@Mocked final CallableStatement statement) throws Exception {
        new Expectations() {{
            statement.getBigDecimal(anyInt);
            result = new SQLException("getBigDecimal error.");
        }};

        sut = new BasicSqlCStatement("sql", statement);

        DbAccessException exception = null;
        try {
            sut.getBigDecimal(2);
        } catch (DbAccessException e) {
            exception = e;
        }
        assertThat("例外が発生すること", exception.getMessage(), is("failed to getBigDecimal(2)"));
    }

    /**
     * {@link BasicSqlCStatement#getDate(int)}のテスト。
     * <p/>
     * アウトパラメータの値が取得できること。
     *
     * @throws Exception
     */
    @Test
    public void getDate() throws Exception {
        sut = testConnection.prepareCall("BEGIN ? := to_date('2015-01-28');"
                + " ?:='2015-01-29';"
                + " ?:=NULL;"
                + " ?:=to_timestamp('2015-01-30 11:12:13');"
                + " END;");
        sut.registerOutParameter(1, Types.DATE);
        sut.registerOutParameter(2, Types.CHAR);
        sut.registerOutParameter(3, Types.DATE);
        sut.registerOutParameter(4, Types.TIMESTAMP);
        sut.execute();

        assertThat("index:1", sut.getDate(1), is(DateUtil.getDate("20150128")));
        final Date date = sut.getDate(2);
        assertThat("index:2", date, is(DateUtil.getDate("20150129")));
        assertThat("index:3", sut.getDate(3), is(nullValue()));
        assertThat("index:4", sut.getDate(4), is(DateUtil.getDate("20150130")));
    }

    /**
     * {@link BasicSqlCStatement#getDate(int)}のテスト。
     * <p/>
     * {@link SQLException}が発生するケース
     *
     * @throws Exception
     */
    @Test
    public void getDate_SQLException(@Mocked final CallableStatement statement) throws Exception {
        new Expectations() {{
            statement.getDate(anyInt);
            result = new SQLException("getDate error");
        }};

        sut = new BasicSqlCStatement("sql", statement);

        DbAccessException exception = null;
        try {
            sut.getDate(3);
        } catch (DbAccessException e) {
            exception = e;
        }
        assertThat("例外が発生していること", exception.getMessage(), is("failed to getDate(3)"));
    }

    /**
     * {@link BasicSqlCStatement#getTime(int)}のテスト。
     * <p/>
     * アウトパラメータの値が取得できること。
     *
     * @throws Exception
     */
    @Test
    public void getTime() throws Exception {
        sut = testConnection.prepareCall(
                "BEGIN ? := to_date('2015-01-28 16:01:02', 'YYYY/MM/DD HH24:MI:SS');"
                        + " ?:= '01:02:03';"
                        + " ?:= NULL;"
                        + " ?:= to_timestamp('2015-01-30 10:11:12.123');"
                        + "END;");
        sut.registerOutParameter(1, Types.TIME);
        sut.registerOutParameter(2, Types.CHAR);
        sut.registerOutParameter(3, Types.TIME);
        sut.registerOutParameter(4, Types.TIMESTAMP);
        sut.execute();

        assertThat("index:1", sut.getTime(1), is(Time.valueOf("16:01:02")));
        assertThat("index:2", sut.getTime(2), is(Time.valueOf("01:02:03")));
        assertThat("index:3", sut.getTime(3), is(nullValue()));
        assertThat("index:4", sut.getTime(4), is(Time.valueOf("10:11:12")));
    }

    /**
     * {@link BasicSqlCStatement#getTime(int)}のテスト。
     * <p/>
     * {@link SQLException}が発生するケース
     *
     * @throws Exception
     */
    @Test
    public void getTime_SQLException(@Mocked final CallableStatement statement) throws Exception {
        new Expectations() {{
            statement.getTime(anyInt);
            result = new SQLException("getTime error.");
        }};
        sut = new BasicSqlCStatement("sql", statement);

        DbAccessException exception = null;
        try {
            sut.getTime(10);
        } catch (DbAccessException e) {
            exception = e;
        }
        assertThat("例外が発生すること", exception.getMessage(), is("failed to getTime(10)"));
    }

    /**
     * {@link BasicSqlCStatement#getTimestamp(int)}のテスト。
     * <p/>
     * アウトパラメータの値が取得できること。
     *
     * @throws Exception
     */
    @Test
    public void getTimestamp() throws Exception {
        sut = testConnection.prepareCall(
                "BEGIN ? := to_timestamp('2015-01-28 01:02:03.123');"
                        + " ? := '2015-01-29 12:13:14';"
                        + " ? := NULL;"
                        + " ? := '2015-01-30';"
                        + " END;");
        sut.registerOutParameter(1, Types.TIMESTAMP);
        sut.registerOutParameter(2, Types.CHAR);
        sut.registerOutParameter(3, Types.TIMESTAMP);
        sut.registerOutParameter(4, Types.DATE);
        sut.execute();

        assertThat("index:1", sut.getTimestamp(1), is(Timestamp.valueOf("2015-01-28 01:02:03.123")));
        assertThat("index:2", sut.getTimestamp(2), is(Timestamp.valueOf("2015-01-29 12:13:14")));
        assertThat("index:3", sut.getTimestamp(3), is(nullValue()));
        assertThat("index:4", sut.getTimestamp(4), is(new Timestamp(java.sql.Date.valueOf("2015-01-30")
                .getTime())));
    }

    /**
     * {@link BasicSqlCStatement#getTimestamp(int)}のテスト。
     * <p/>
     * {@link SQLException}が発生するケース。
     */
    @Test
    public void getTimestamp_SQLException(@Mocked final CallableStatement statement) throws Exception {
        new Expectations() {{
            statement.getTimestamp(anyInt);
            result = new SQLException("getTimestamp error.");
        }};

        sut = new BasicSqlCStatement("sql", statement);
        DbAccessException exception = null;
        try {
            sut.getTimestamp(9);
        } catch (DbAccessException e) {
            exception = e;
        }
        assertThat("例外が発生すること", exception.getMessage(), is("failed to getTimestamp(9)"));
    }

    /**
     * {@link BasicSqlCStatement#getBoolean(int)}のテスト。
     * <p/>
     * アウトパラメータの値が取得できること。
     *
     * @throws Exception
     */
    @Test
    public void getBoolean() throws Exception {
        sut = testConnection.prepareCall("BEGIN ? := '1'; ? := 0; ?:= 2; ? := NULL; END;");
        sut.registerOutParameter(1, Types.CHAR);
        sut.registerOutParameter(2, Types.SMALLINT);
        sut.registerOutParameter(3, Types.SMALLINT);
        sut.registerOutParameter(4, Types.INTEGER);
        sut.execute();

        assertThat("index:1", sut.getBoolean(1), is(Boolean.TRUE));
        assertThat("index:2", sut.getBoolean(2), is(Boolean.FALSE));
        assertThat("index:3", sut.getBoolean(3), is(Boolean.TRUE));
        assertThat("index:4", sut.getBoolean(4), is(nullValue()));
    }

    /**
     * {@link BasicSqlCStatement#getBoolean(int)}のテスト。
     * <p/>
     * {@link SQLException}が発生するケース。
     *
     * @throws Exception
     */
    @Test
    public void getBoolean_SQLException(@Mocked final CallableStatement statement) throws Exception {
        new Expectations() {{
            statement.getBoolean(anyInt);
            result = new SQLException("getBoolean error.");
        }};

        DbAccessException exception = null;
        sut = new BasicSqlCStatement("sql", statement);
        try {
            sut.getBoolean(5);
        } catch (DbAccessException e) {
            exception = e;
        }
        assertThat("例外が発生すること", exception.getMessage(), is("failed to getBoolean(5)"));
    }

    /**
     * {@link BasicSqlCStatement#getBytes(int)}のテスト。
     * <p/>
     * アウトパラメータが取得できること。
     *
     * @throws Exception
     */
    @Test
    public void getBytes() throws Exception {
        sut = testConnection.prepareCall("BEGIN"
                + " ?:=to_blob('616263');"
                + " ?:='12345';"
                + " ?:=NULL;"
                + " END;");
        sut.registerOutParameter(1, Types.BINARY);
        sut.registerOutParameter(2, Types.VARCHAR);
        sut.registerOutParameter(3, Types.BINARY);
        sut.execute();

        assertThat("index:1", sut.getBytes(1), is("abc".getBytes()));
        assertThat("index:2", sut.getBytes(2), is("12345".getBytes()));
        assertThat("index:3", sut.getBytes(3), is(nullValue()));
    }

    /**
     * {@link BasicSqlCStatement#getBytes(int)}のテスト。
     * <p/>
     * {@link SQLException}が発生するケース
     *
     * @throws Exception
     */
    @Test
    public void getBytes_SQLException(@Mocked final CallableStatement statement) throws Exception {
        new Expectations() {{
            statement.getBytes(anyInt);
            result = new SQLException("getBytes error.");
        }};

        sut = new BasicSqlCStatement("sql", statement);
        DbAccessException exception = null;

        try {
            sut.getBytes(45);
        } catch (DbAccessException e) {
            exception = e;
        }
        assertThat("例外が発生すること", exception.getMessage(), is("failed to getBytes(45)"));
    }

    /**
     * {@link BasicSqlCStatement#getBlob(int)}のテスト。
     * <p/>
     * アウトパラメータが取得できること。
     *
     * @throws Exception
     */
    @Test
    public void getBlob() throws Exception {
        sut = testConnection.prepareCall("BEGIN"
                + " ?:= to_blob('e38182e38184e38186e38188e3818a');"
                + " ?:= NULL;"
                + "END;");
        sut.registerOutParameter(1, Types.BLOB);
        sut.registerOutParameter(2, Types.BLOB);
        sut.execute();

        Blob index1 = sut.getBlob(1);
        assertThat("index1", index1.getBytes(1, (int) index1.length()), is("あいうえお".getBytes("utf-8")));
        assertThat("index2", sut.getBlob(2), is(nullValue()));
    }

    /**
     * {@link BasicSqlCStatement#getBlob(int)}のテスト。
     * <p/>
     * {@link SQLException}が発生するケース。
     *
     * @throws Exception
     */
    @Test
    public void getBlob_SQLException(@Mocked final CallableStatement statement) throws Exception {
        new Expectations() {{
            statement.getBlob(anyInt);
            result = new SQLException("getBlob error.");
        }};

        sut = new BasicSqlCStatement("sql", statement);

        DbAccessException exception = null;

        try {
            sut.getBlob(10);
        } catch (DbAccessException e) {
            exception = e;
        }
        assertThat("例外が発生すること", exception.getMessage(), is("failed to getBlob(10)"));
    }

    /**
     * {@link BasicSqlCStatement#getClob(int)}のテスト。
     * <p/>
     * アウトパラメータが取得できること
     *
     * @throws Exception
     */
    @Test
    public void getClob() throws Exception {
        sut = testConnection.prepareCall("BEGIN ?:= to_clob('12345'); ? := NULL; END;");
        sut.registerOutParameter(1, Types.CLOB);
        sut.registerOutParameter(2, Types.CLOB);
        sut.execute();

        Clob clob = sut.getClob(1);
        assertThat("index:1", clob.getSubString(1, (int) clob.length()), is("12345"));
        assertThat("index:2", sut.getClob(2), is(nullValue()));
    }

    /**
     * {@link BasicSqlCStatement#getClob(int)}のテスト。
     * <p/>
     * {@link SQLException}が発生するケース
     *
     * @throws Exception
     */
    @Test
    public void getClob_SQLException(@Mocked final CallableStatement statement) throws Exception {
        new Expectations() {{
            statement.getClob(anyInt);
            result = new SQLException("getClob error.");
        }};

        DbAccessException exception = null;

        sut = new BasicSqlCStatement("sql", statement);
        try {
            sut.getClob(1);
        } catch (DbAccessException e) {
            exception = e;
        }
        assertThat("例外が発生すること", exception.getMessage(), is("failed to getClob(1)"));
    }

    /**
     * {@link BasicSqlCStatement#retrieve()}のテスト。
     * <p/>
     * 通常のSQL文は{@link BasicSqlCStatement}経由でも実行はできる。
     */
    @Test
    public void retrieve() throws Exception {
        sut = testConnection.prepareCall("SELECT '1' COL FROM DUAL");
        final SqlResultSet result = sut.retrieve();
        assertThat("SQL文も実行できること", result.size(), is(1));
    }

    /**
     * {@link BasicSqlCStatement#executeQuery()}のテスト。
     * <p/>
     * 通常のSQL文は{@link BasicSqlCStatement}経由でも実行はできる。
     *
     * @throws Exception
     */
    @Test
    public void executeQuery() throws Exception {
        sut = testConnection.prepareCall("SELECT '1' COL FROM DUAL UNION ALL SELECT '2' FROM DUAL ORDER BY 1");
        final ResultSetIterator rs = sut.executeQuery();
        assertThat("1レコード目が存在していること", rs.next(), is(true));
        assertThat("値が取得できること", rs.getRow()
                .getString("col"), is("1"));

        assertThat("2レコード目が存在していること", rs.next(), is(true));
        assertThat("値の取得ができること", rs.getRow()
                .getString("col"), is("2"));
    }

    private void assertLog(String msg, Pattern pattern) {

        List<String> logMessages = OnMemoryLogWriter.getMessages("writer.memory");
        for (String message : logMessages) {
            final Matcher matcher = pattern.matcher(message);
            if (matcher.find()) {
                return;
            }
        }
        throw new AssertionError(MessageFormat.format("expected log pattern not found. message = {0}\n"
                        + " expected pattern = [{1}]\n"
                        + " actual log messages = [\n{2}\n]",
                msg,
                pattern.pattern(),
                logMessages.toString()));
    }

    @Entity
    @Table(name = "CSTATEMENT_TEST_TABLE")
    public static class TestTable {
        @Id
        @Column(name = "u_id", length = 15)
        public Long id;

        @Column(name = "text", length = 1000)
        public String text;
    }
}

