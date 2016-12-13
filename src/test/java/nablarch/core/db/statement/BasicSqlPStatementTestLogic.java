package nablarch.core.db.statement;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import nablarch.core.db.DbAccessException;
import nablarch.core.db.DbExecutionContext;
import nablarch.core.db.connection.BasicDbConnection;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.dialect.DefaultDialect;
import nablarch.core.db.dialect.Dialect;
import nablarch.core.db.statement.exception.SqlStatementException;
import nablarch.core.exception.IllegalOperationException;
import nablarch.core.log.Logger;
import nablarch.core.transaction.TransactionContext;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import nablarch.test.support.log.app.OnMemoryLogWriter;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;

/**
 * {@link BasicSqlPStatement}のテストクラス。
 * サブクラスにて、{@link #createConnectionFactory}を実装し、
 * 生成するファクトリを切り替えることで、
 * 様々な組み合わせで、本クラスで用意されたテストメソッドを実行することができる。
 * これにより{@link BasicSqlPStatement}サブクラスの動作がスーパクラスと同等であることが
 * 確認できる。
 *
 * @author T.Kawasaki
 */
public abstract class BasicSqlPStatementTestLogic {

    @Entity
    @Table(name="statement_test_sqlserver")
    private static class SqlServerTestEntity {
        @Id
        @Column(name = "id", columnDefinition = "bigint identity")
        public Long id;

        @Column(name = "name")
        public String name;
    }

    @Entity
    @Table(name = "statement_test_table")
    public static class TestEntity {

        @Id
        @Column(name = "entity_id")
        public String id;

        @Column(name = "varchar_col", columnDefinition = "varchar(50)")
        public String varcharCol;

        @Column(name = "long_col", length = 20)
        public Long longCol;

        @Column(name = "date_col", columnDefinition = "date")
        @Temporal(TemporalType.DATE)
        public Date dateCol;

        @Column(name = "timestamp_col")
        public Timestamp timestampCol;

        @Column(name = "time_col")
        public Time timeCol;

        @Column(name = "decimal_col", length = 10, scale = 2)
        public BigDecimal decimalCol;

        @Column(name = "binary_col")
        public byte[] binaryCol;

        @Column(name = "integer_col", length = 9)
        public Integer integerCol;

        @Column(name = "float_col", precision = 2, scale = 1)
        public Float floatCol;

        @Column(name = "boolean_col")
        public Boolean booleanCol;

        @Transient
        public String[] varchars;

        public TestEntity() {
        }

        public TestEntity(String id, String varcharCol, Long longCol, Date dateCol, Integer integerCol, Float floatCol) {
            this.id = id;
            this.varcharCol = varcharCol;
            this.longCol = longCol;
            this.dateCol = dateCol;
            this.integerCol = integerCol;
            this.floatCol = floatCol;
        }

        public String getId() {
            return id;
        }

        public String getVarcharCol() {
            return varcharCol;
        }

        public Long getLongCol() {
            return longCol;
        }

        public Date getDateCol() {
            return dateCol;
        }

        public Timestamp getTimestampCol() {
            return timestampCol;
        }

        public Time getTimeCol() {
            return timeCol;
        }

        public BigDecimal getDecimalCol() {
            return decimalCol;
        }

        public byte[] getBinaryCol() {
            return binaryCol;
        }

        public Integer getIntegerCol() {
            return integerCol;
        }

        public Float getFloatCol() {
            return floatCol;
        }

        public Boolean getBooleanCol() {
            return booleanCol;
        }

        public String[] getVarchars() {
            return varchars;
        }
    }


    /**
     * テスト対象となる{@link nablarch.core.db.connection.ConnectionFactory}を生成する。
     * サブクラスにて、生成するファクトリを切り替えることで、
     * 様々な組み合わせで、本クラスで用意されたテストメソッドを実行することができる。
     *
     * @return テスト対象となるファクトリ
     */
    protected abstract ConnectionFactory createConnectionFactory();

    /** テスト用のコネクション */
    protected TransactionManagerConnection dbCon;

    @BeforeClass
    public static void dbSetup() {
        VariousDbTestHelper.createTable(TestEntity.class);
    }

    @After
    public void terminateDb() {
        dbCon.terminate();
    }

    @Before
    public void setUpTestData() throws Exception {
        dbCon = createConnectionFactory().getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        Calendar calendar = Calendar.getInstance();
        calendar.set(2010, 6, 10);
        VariousDbTestHelper.setUpTable(
                new TestEntity("10001", "a", 10000L, calendar.getTime(), 1, 1.1f),
                new TestEntity("10002", "b", 20000L, calendar.getTime(), 2, 2.2f),
                new TestEntity("10003", "c_\\(like検索用)", 30000L, calendar.getTime(), 3, 3.3f)
        );
    }

    @Before
    public void clearLog() {
        OnMemoryLogWriter.clear();
    }

    /**
     * {@link BasicSqlPStatement#executeQuery()}で条件パラメータ有りのSQLログが出力されること
     */
    @Test
    public void executeQuery_writeSqlLog() throws Exception {
        SqlPStatement statement = dbCon.prepareStatement(
                "SELECT ENTITY_ID FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        statement.setString(1, "10001");
        statement.executeQuery();

        assertLog("開始ログ", Pattern.compile(
                "\\Qnablarch.core.db.statement.BasicSqlPStatement#executeQuery "
                        + "SQL = [SELECT ENTITY_ID FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?]\\E"));
        assertLog("パラメータ", Pattern.compile("Parameters" + Logger.LS
                + "\t01 = \\Q[10001]\\E"));
        assertLog("終了ログ", Pattern.compile("\texecute time\\(ms\\) = \\[\\d+\\]"));
    }

    @Test
    public void executeQuery_writeSqlLog_withOption() throws Exception {
        setDialect(dbCon, new OffsetSupportDialcet("SELECT 'TEST_CODE' FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?"));
        SqlPStatement statement = dbCon.prepareStatement(
                "SELECT ENTITY_ID FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?", new SelectOption(2, 3));
        statement.setString(1, "10001");
        statement.executeQuery();
        assertLog("開始ログ", Pattern.compile(
                "\\Qnablarch.core.db.statement.BasicSqlPStatement#executeQuery "
                        + "SQL = [SELECT 'TEST_CODE' FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?]\\E"));
        assertLog("パラメータ", Pattern.compile("Parameters" + Logger.LS
                + "\t01 = \\Q[10001]\\E"));
        assertLog("終了ログ", Pattern.compile("\texecute time\\(ms\\) = \\[\\d+\\]"));
    }

    /**
     * {@link BasicSqlPStatement#executeQuery()}で条件指定した場合。
     */
    @Test
    public void executeQuery_withCondition() throws Exception {

        final SqlPStatement statement = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        statement.setString(1, "10002");
        final ResultSetIterator actual = statement.executeQuery();

        // ---------------------------- first row
        assertThat("1レコード目は存在している", actual.next(), is(true));
        final SqlRow row = actual.getRow();
        assertThat("条件で絞り込んだレコードが取得出来ていること", row.getString("entityId"), is("10002"));
        assertThat("属性も取得できる -> varchar", row.getString("varcharCol"), is("b"));

        // ---------------------------- next row
        assertThat("2レコード目は存在していない", actual.next(), is(false));
    }

    /**
     * 生成時に検索範囲を指定した場合に、指定した検索範囲で検索することのテスト。
     */
    @Test
    public void executeQuery_WithSelectOption() throws Exception {
        final SqlPStatement statement = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID >= ? ORDER BY ENTITY_ID", new SelectOption(2, 3));
        statement.setString(1, "1");
        final ResultSetIterator actual = statement.executeQuery();
        // -- first row (10002)
        assertThat("1件目が取得できる", actual.next(), is(true));
        assertThat(actual.getRow().getString("entityId"), is("10002"));
        // -- second result (10003)
        assertThat("2件目が取得できる", actual.next(), is(true));
        assertThat(actual.getRow().getString("entityId"), is("10003"));
        assertThat("3件目は取得できない", actual.next(), is(false));
    }

    /**
     * 生成時に検索範囲を指定した場合に、指定した検索範囲で検索することのテスト。
     * DialectがOffsetをサポートしていない場合のテスト。
     */
    @Test
    public void executeQuery_WithSelectOptionUnSupportOffset() throws Exception {
        setDialect(dbCon, new DefaultDialect());
        final SqlPStatement statement = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID >= ? ORDER BY ENTITY_ID", new SelectOption(2, 3));
        statement.setString(1, "1");
        final ResultSetIterator actual = statement.executeQuery();
        // -- first row (10002)
        assertThat(actual.next(), is(true));
        SqlRow row = actual.getRow();
        assertThat(row.getString("entityId"), is("10002"));
        // -- second result (10003)
        assertThat(actual.next(), is(true));
        row = actual.getRow();
        assertThat(row.getString("entityId"), is("10003"));
        assertThat(actual.next(), is(false));
    }

    /**
     * {@link BasicSqlPStatement#executeQuery()}で条件なしの場合。
     */
    @Test
    public void executeQuery_withoutCondition() throws Exception {
        final SqlPStatement statement = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE ORDER BY ENTITY_ID");
        final ResultSetIterator actual = statement.executeQuery();
        assertThat("1レコード目は存在する", actual.next(), is(true));
        assertThat("1レコード目:主キー", actual.getRow()
                .getString("entityId"), is("10001"));
        assertThat("2レコード目は存在する", actual.next(), is(true));
        assertThat("2レコード目:主キー", actual.getRow()
                .getString("entityId"), is("10002"));
        assertThat("3レコード目は存在する", actual.next(), is(true));
        assertThat("3レコード目:主キー", actual.getRow()
                .getString("entityId"), is("10003"));
        assertThat("4レコード目は存在しない", actual.next(), is(false));
    }

    /**
     * {@link BasicSqlPStatement#executeQuery()}で{@link BasicSqlPStatement#setMaxRows(int)}の設定値が有効になっていること
     * <p/>
     * ※3レコード存在しているテーブルの全レコード検索で、{@link BasicSqlPStatement#setMaxRows(int)}には2を指定
     */
    @Test
    public void executeQuery_withMaxRows() throws Exception {
        final SqlPStatement statement = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        statement.setMaxRows(2);
        final ResultSetIterator rows = statement.executeQuery();

        assertThat("1レコード目はあり", rows.next(), is(true));
        assertThat("2レコード目はあり", rows.next(), is(true));
        assertThat("3レコード目はなし", rows.next(), is(false));
    }

    /**
     * {@link ResultSetIterator}に正しく参照を渡していることの確認。
     */
    @Test
    public void testSetStatement() throws Exception {
        final SqlPStatement statement = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        ResultSetIterator iterator = statement.executeQuery();
        assertThat(iterator.getStatement(), sameInstance((SqlStatement) statement));
    }

    /**
     * {@link BasicSqlPStatement#executeQuery()}でSQLExceptionが発生するケース。
     */
    @Test(expected = SqlStatementException.class)
    public void executeQuery_SQLException(@Mocked final PreparedStatement mock) throws Exception {
        new Expectations() {
            {
                mock.executeQuery();
                result = new SQLException("executeQuery error.", "code", 100);
            }
        };
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        Deencapsulation.setField(sut, mock);
        sut.executeQuery();
    }

    /**
     * {@link BasicSqlPStatement#executeUpdate()}のログ出力のテスト。
     */
    @Test
    public void executeUpdate_writeSqlLog(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.executeUpdate();
            result = 5;
        }};
        final SqlPStatement sut = dbCon.prepareStatement(
                "UPDATE STATEMENT_TEST_TABLE SET VARCHAR_COL = ? WHERE ENTITY_ID = ?");
        Deencapsulation.setField(sut, mockStatement);
        sut.setString(1, "あいうえお");
        sut.setString(2, "10001");
        sut.executeUpdate();

        assertLog("開始ログ", Pattern.compile(
                "\\Qnablarch.core.db.statement.BasicSqlPStatement#executeUpdate SQL = ["
                        + "UPDATE STATEMENT_TEST_TABLE SET VARCHAR_COL = ? WHERE ENTITY_ID = ?]\\E"));
        assertLog("パラメータ", Pattern.compile(
                "Parameters" + Logger.LS
                        + "\t\\Q01 = [あいうえお]" + Logger.LS + "\t02 = [10001]\\E"));

        assertLog("終了ログ", Pattern.compile(
                "nablarch.core.db.statement.BasicSqlPStatement#executeUpdate"
                        + Logger.LS + "\texecute time\\(ms\\) = \\[[0-9]+\\] update count = \\[5\\]"));
    }

    /**
     * {@link BasicSqlPStatement#executeUpdate()}の条件がある場合の実行テスト。
     */
    @Test
    public void executeUpdate_withCondition() throws Exception {

        Calendar calendar = Calendar.getInstance();
        calendar.set(2015, 1, 16, 0, 0, 0);
        Date testDate = calendar.getTime();

        final SqlPStatement statement = dbCon.prepareStatement(
                "UPDATE STATEMENT_TEST_TABLE"
                        + " SET VARCHAR_COL = ?,"
                        + " LONG_COL = ?,"
                        + " DATE_COL = ? WHERE ENTITY_ID = ?");

        statement.setString(1, "あいうえお");
        statement.setLong(2, 12345L);
        statement.setDate(3, new java.sql.Date(testDate.getTime()));
        statement.setString(4, "10002");
        final int updated = statement.executeUpdate();
        dbCon.commit();

        assertThat("1レコード更新される", updated, is(1));
        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "10002");
        assertThat("更新されていること:varchar_col", actual.varcharCol, is("あいうえお"));
        assertThat("更新されていること:long_col", actual.longCol, is(12345L));
    }

    /**
     * {@link BasicSqlPStatement#executeUpdate()}の条件がない場合のテスト。
     */
    @Test
    public void executeUpdate_withoutCondition() throws Exception {

        final SqlPStatement statement = dbCon.prepareStatement(
                "UPDATE STATEMENT_TEST_TABLE SET LONG_COL = LONG_COL + 1");
        final int updated = statement.executeUpdate();

        assertThat("レコードが全て更新される", updated, is(3));

        dbCon.commit();

        final List<TestEntity> actual = VariousDbTestHelper.findAll(TestEntity.class, "id");
        long[] expected = {10001, 20001, 30001};
        int index = 0;
        for (TestEntity entity : actual) {
            assertThat("更新されている", entity.longCol, is(expected[index++]));
        }
    }

    /**
     * {@link BasicSqlPStatement#executeUpdate()}でSQLExceptionが発生するケース。
     */
    @Test(expected = SqlStatementException.class)
    public void executeUpdate_SQLException(@Mocked final PreparedStatement mock) throws Exception {
        new Expectations() {
            {
                mock.executeUpdate();
                result = new SQLException("executeUpdate error.", "code", 101);
            }
        };
        final SqlPStatement sut = dbCon.prepareStatement("UPDATE STATEMENT_TEST_TABLE SET LONG_COL = 1");
        Deencapsulation.setField(sut, mock);
        sut.executeUpdate();
    }

    /**
     * {@link BasicSqlPStatement#execute()}で更新処理を実行するケース
     */
    @Test
    public void execute() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (?)");
        sut.setString(1, "12345");
        final boolean result = sut.execute();
        assertThat(result, is(false));
        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "12345");
        assertThat(actual, is(notNullValue()));
    }

    /**
     * {@link BasicSqlPStatement#execute()}のSQLログのテスト。
     */
    @Test
    public void execute_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (?)");
        sut.setString(1, "12345");
        sut.execute();

        assertLog("開始ログ",
                Pattern.compile("\\Qnablarch.core.db.statement.BasicSqlPStatement#execute"
                        + " SQL = [INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (?)]\\E"));
        assertLog("パラメータログ",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [12345]\\E"));
        assertLog("実行ログ",
                Pattern.compile("\\Qnablarch.core.db.statement.BasicSqlPStatement#execute" + Logger.LS
                        + "\texecute time(ms)\\E = \\[[0-9]+\\]"));
    }

    /**
     * {@link BasicSqlPStatement#execute()}でselectを実行するケース
     */
    @Test
    public void execute_select() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "select * from STATEMENT_TEST_TABLE where ENTITY_ID = ?");
        sut.setString(1, "10001");
        final boolean result = sut.execute();
        assertThat("検索処理なのでtrue", result, is(true));

        final ResultSet rs = sut.getResultSet();
        try {
            assertThat(rs, is(notNullValue()));
        } finally {
            rs.close();
        }
    }

    /**
     * {@link BasicSqlPStatement#execute()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void execute_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.execute();
            result = new SQLException("execute error");
        }};
        final SqlPStatement sut = dbCon.prepareStatement(
                "select * from STATEMENT_TEST_TABLE where ENTITY_ID = ?");
        Deencapsulation.setField(sut, mockStatement);
        sut.setString(1, "10001");
        sut.execute();
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}のSQLログのテスト。
     *
     * @throws Exception
     */
    @Test
    public void retrieve_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        sut.setFetchSize(100);
        sut.setQueryTimeout(123);

        sut.retrieve();

        assertLog("開始ログ", Pattern.compile(
                "nablarch.core.db.statement.BasicSqlPStatement#retrieve SQL = \\Q[SELECT * FROM STATEMENT_TEST_TABLE]\\E"
                        + Logger.LS + "\tstart position = \\Q[1] size = [0] queryTimeout = [123] fetchSize = [100]\\E"
        ));
        assertLog("パラメータログ", Pattern.compile("Parameters$"));
        assertLog("終了ログ", Pattern.compile("nablarch.core.db.statement.BasicSqlPStatement#retrieve"
                        + Logger.LS
                        + "\texecute time\\(ms\\) = \\[[0-9]+\\] retrieve time\\(ms\\) = \\[[0-9]+\\] count = \\[3\\]"
        ));
    }

    /**
     * ステートメント生成時に設定された検索処理のオプションを指定し、Dialectが変換する場合の
     * {@link BasicSqlPStatement#retrieve()}のSQLログのテスト。
     *
     * @throws Exception
     */
    @Test
    public void retrieve_writeSqlLogWithOptionOffsetSupport() throws Exception {
        final String convertedSql = "SELECT 'TEST_CODE' FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID IN ('10002','10003')";
        setDialect(dbCon, new OffsetSupportDialcet(convertedSql));
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE", new SelectOption(2, 10));
        sut.setQueryTimeout(123);
        sut.setFetchSize(100);
        sut.retrieve();
        assertLog("開始ログ", Pattern.compile(
                "nablarch.core.db.statement.BasicSqlPStatement#retrieve SQL = \\Q["+ convertedSql +"]\\E"
                        + Logger.LS + "\tstart position = \\Q[1] size = [0] queryTimeout = [123] fetchSize = [100]\\E"
        ));
        assertLog("パラメータログ", Pattern.compile("Parameters$"));
        assertLog("終了ログ", Pattern.compile("nablarch.core.db.statement.BasicSqlPStatement#retrieve"
                        + Logger.LS + "\texecute time\\(ms\\) = \\[[0-9]+\\] retrieve time\\(ms\\) = \\[[0-9]+\\] count = \\[2\\]"
        ));
    }

    /**
     * ステートメント生成時に設定された検索処理のオプションを指定した場合のDialectが変換しない場合の
     * {@link BasicSqlPStatement#retrieve()}のSQLログのテスト。
     * @throws Exception
     */
    @Test
    public void retrieve_writeSqlLogWithOptionOffsetUnSupport() throws Exception {
        String sql = "SELECT * FROM STATEMENT_TEST_TABLE";
        setDialect(dbCon, new DefaultDialect());
        final SqlPStatement sut = dbCon.prepareStatement(sql, new SelectOption(2, 10));
        sut.setQueryTimeout(123);
        sut.setFetchSize(100); //実行時にページング機能が動作するのでfecthはmaxの10となり、無視されます。
        sut.retrieve();
        assertLog("開始ログ", Pattern.compile(
                "nablarch.core.db.statement.BasicSqlPStatement#retrieve SQL = \\Q["+ sql +"]\\E"
                        + Logger.LS + "\tstart position = \\Q[2] size = [10] queryTimeout = [123] fetchSize = [10]\\E"
        ));
        assertLog("パラメータログ", Pattern.compile("Parameters$"));
        assertLog("終了ログ", Pattern.compile("nablarch.core.db.statement.BasicSqlPStatement#retrieve"
                        + Logger.LS + "\texecute time\\(ms\\) = \\[[0-9]+\\] retrieve time\\(ms\\) = \\[[0-9]+\\] count = \\[2\\]"
        ));
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}で条件ありでデータの取得ができること
     *
     * @throws Exception
     */
    @Test
    public void retrieve_withCondition() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        sut.setString(1, "10001");
        final SqlResultSet actual = sut.retrieve();

        assertThat("1レコード取得できる", actual.size(), is(1));
        assertThat(actual.get(0)
                .getString("entityId"), is("10001"));
        assertThat(actual.get(0)
                .getString("varcharCol"), is("a"));
        assertThat(actual.get(0)
                .getLong("longCol"), is(10000L));
    }

    /**
     * ステートメント生成時に設定された検索処理のオプションが有効になることのテスト。<br />
     * {@link BasicSqlPStatement#retrieve()}で条件ありでデータの取得ができること
     *
     * @throws Exception
     */
    @Test
    public void retrieve_withStatementConditionWithSelectOption() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID >= ? order by ENTITY_ID", new SelectOption(2, 3));
        sut.setString(1, "10001");

        final SqlResultSet actual = sut.retrieve();
        assertThat("2レコード取得できる", actual.size(), is(2));
        assertThat(actual.get(0).getString("entityId"), is("10002"));
        assertThat(actual.get(1).getString("entityId"), is("10003"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}で条件なしでデータが取得できること
     * @throws Exception
     */
    @Test
    public void retrieve_withoutCondition() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        final SqlResultSet actual = sut.retrieve();

        assertThat("テーブルの全てのレコードが取得できる", actual.size(), is(3));
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}でリミット指定した検索処理ができること
     * @throws Exception
     */
    @Test
    public void retrieve_withLimit() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE LONG_COL < ? ORDER BY ENTITY_ID");
        sut.setLong(1, 50000);

        // この設定は無視される
        sut.setMaxRows(9999);

        final SqlResultSet actual = sut.retrieve(0, 2);
        assertThat("limitの2レコード取得できる", actual.size(), is(2));
        assertThat(actual.get(0)
                .getString("entityId"), is("10001"));
        assertThat(actual.get(1)
                .getString("entityId"), is("10002"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}でオフセットとリミットを指定した検索処理ができること
     * @throws Exception
     */
    @Test
    public void retrieve_withOffsetAndLimit() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE LONG_COL < ? ORDER BY ENTITY_ID");
        sut.setLong(1, 50000);
        final SqlResultSet actual = sut.retrieve(2, 2);
        assertThat("limitの2レコード取得できる", actual.size(), is(2));
        assertThat("2レコード目から取得できる", actual.get(0)
                .getString("entityId"), is("10002"));
        assertThat(actual.get(1)
                .getString("entityId"), is("10003"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}でオフセットが取得可能最大件数より大きい場合
     * レコードは取得されないこと。
     *
     * @throws Exception
     */
    @Test
    public void retrieve_offsetOverRecordCount() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE LONG_COL < ? ORDER BY ENTITY_ID");
        sut.setLong(1, 50000);
        final SqlResultSet actual = sut.retrieve(10, 10);
        assertThat("レコードは取得されない", actual.size(), is(0));
    }

    /**
     * ステートメント生成時に設定された検索処理のオプションが有効になることのテスト。<br />
     * ステートメント作成時の検索範囲通りに取得できること。
     *
     * @throws Exception テスト時の実行時例外
     */
    @Test
    public void retrieve_withLimitOffsetOption() throws Exception {
        // 2件目から2件を取得
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE ORDER BY ENTITY_ID", new SelectOption(2, 2));
        SqlResultSet rs = sut.retrieve();
        assertThat(rs.size(), is(2));
        assertThat(rs.get(0).getString("entity_id"), is("10002"));
        assertThat(rs.get(1).getString("entity_id"), is("10003"));
    }

    /**
     * ステートメント生成時に設定された検索処理オプションで、startポジションの値(検索開始位置だけ)有効な場合のテスト。<br />
     * limitに0以下を指定しても、検索できること。
     *
     * @throws Exception テスト時の実行時例外
     */
    @Test
    public void retrieve_withOffsetOption() throws Exception {
        // 2件目以降を取得
        assertThat(VariousDbTestHelper.findAll(TestEntity.class).size(), is(3));
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE ORDER BY ENTITY_ID", new SelectOption(2, 0));
        SqlResultSet rs = sut.retrieve();
        assertThat(rs.size(), is(2));
        assertThat(rs.get(0).getString("entity_id"), is("10002"));
        assertThat(rs.get(1).getString("entity_id"), is("10003"));
    }

    /**
     * ステートメント生成時に設定された検索処理オプションで、limitの値(検索開始位置だけ)有効な場合のテスト。<br />
     * startPos = 1以下にした場合でも、検索上限の設定が有効であること。
     *
     * @throws Exception テスト時の実行時例外
     */
    @Test
    public void retrieve_withLimitOption() throws Exception {
        // 1件目から2件取得
        assertThat(VariousDbTestHelper.findAll(TestEntity.class).size(), is(3));
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE ORDER BY ENTITY_ID", new SelectOption(1, 2));
        SqlResultSet rs = sut.retrieve();
        assertThat(rs.size(), is(2));
        assertThat(rs.get(0).getString("entity_id"), is("10001"));
        assertThat(rs.get(1).getString("entity_id"), is("10002"));
    }

    /**
     * ステートメント生成時に設定された検索処理オプションで、startポジション, limitの値が無効な場合でも、例外が発生すること。
     *
     * @throws Exception
     */
    @Test(expected = IllegalOperationException.class)
    public void retrieve_withIgnoredOption() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE ORDER BY ENTITY_ID", new SelectOption(1, 0));
        sut.retrieve(2, 2);
    }

    /**
     * ステートメント生成時に設定された検索処理オプションを指定して、実行時に範囲指定した場合に例外が発生することを確認する。
     *
     * @throws Exception
     */
    @Test(expected = IllegalOperationException.class)
    public void retrieve_withDuplicatePagenate() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE ORDER BY ENTITY_ID", new SelectOption(2, 2));
        sut.retrieve(2, 2);
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}でSQLExceptionが発生するケース
     * は、SqlStatementExceptionが送出されること。
     *
     * @throws Exception
     */
    @Test(expected = SqlStatementException.class)
    public void retrieve_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.executeQuery();
            result = new SQLException("retrieve error", "", 999);
        }};

        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE");
        Deencapsulation.setField(sut, mockStatement);
        sut.retrieve();
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}で非キャッチ例外が発生するケースは、
     * その例外がそのまま送出されること。
     *
     * @throws Exception
     */
    @Test(expected = IllegalStateException.class)
    public void retrieve_RuntimeException(
            @Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            ResultSet rs = mockStatement.executeQuery();
            rs.getMetaData();
            result = new IllegalStateException("error");
        }};

        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE");
        Deencapsulation.setField(sut, mockStatement);
        sut.retrieve();
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}でError系が発生するケースは、
     * その例外がそのまま送出されること。
     *
     * @throws Exception
     */
    @Test(expected = StackOverflowError.class)
    public void retrieve_Error(
            @Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            ResultSet rs = mockStatement.executeQuery();
            rs.getMetaData();
            result = new StackOverflowError("error");
        }};

        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE");
        Deencapsulation.setField(sut, mockStatement);
        sut.retrieve();
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}でfinallyのリソース開放で例外が発生する場合
     * その例外がそのまま送出されること。
     *
     * @throws Exception
     */
    @Test
    public void retrieve_finallyError(
            @Mocked final PreparedStatement mockStatement) throws Exception {

        new Expectations() {{
            ResultSet rs = mockStatement.executeQuery();
            final ResultSetMetaData rsm = rs.getMetaData();
            rsm.getColumnCount();
            result = 0;
            rs.close();
            result = new NullPointerException("null");
        }};

        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE");
        Deencapsulation.setField(sut, mockStatement);
        try {
            sut.retrieve();
            fail("");
        } catch (RuntimeException e) {
            assertThat("causeはNullPointerException", e.getCause(), is(instanceOf(NullPointerException.class)));
        }

        assertLog("ログが出力されれる。", Pattern.compile("failed to close result set."));
    }

    /**
     * {@link BasicSqlPStatement#retrieve()}でtryとfinallyの共に例外が発生する場合、
     * tryブロックの例外が送出されfinallyブロックの例外はログ出力されること。
     *
     * @throws Exception
     */
    @Test
    public void retrieve_tryAndFinallyError(
            @Mocked final PreparedStatement mockStatement) throws Exception {

        new Expectations() {{
            ResultSet rs = mockStatement.executeQuery();
            rs.close();
            result = new NullPointerException("null error");
            rs.getMetaData();
            result = new IllegalStateException("error");
        }};

        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE");
        Deencapsulation.setField(sut, mockStatement);
        try {
            sut.retrieve();
            fail("");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is("error"));
        }

        assertLog("ログが出力されれる。", Pattern.compile("failed to close result set."));
    }

    /**
     * {@link BasicSqlPStatement#addBatch()}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void addBatch() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (?)");

        assertThat("addBatch実行前のサイズは0", sut.getBatchSize(), is(0));

        sut.setString(1, "88888");
        sut.addBatch();
        assertThat("addBatchが実行されたのでサイズが増える", sut.getBatchSize(), is(1));

        sut.setString(1, "99999");
        sut.addBatch();
        assertThat("addBatchが実行されたのでサイズが増える", sut.getBatchSize(), is(2));
    }

    /**
     * {@link BasicSqlPStatement#addBatch()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが発生する。
     *
     * @throws Exception
     */
    @Test(expected = DbAccessException.class)
    public void addBatch_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.addBatch();
            result = new SQLException("addBatch error", "", 999);
        }};

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (?)");
        Deencapsulation.setField(sut, mockStatement);
        sut.setString(1, "1");
        sut.addBatch();
    }


    /**
     * {@link BasicSqlPStatement#executeBatch()}でSQLログが出力されること
     */
    @Test
    public void executeBatch_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (?)");
        sut.setString(1, "88888");
        sut.addBatch();
        sut.setString(1, "99999");
        sut.addBatch();
        sut.executeBatch();

        assertLog("開始ログ", Pattern.compile(
                "\\Qablarch.core.db.statement.BasicSqlPStatement#executeBatch SQL = ["
                        + "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (?)]\\E"));

        assertLog("パラメータログ", Pattern.compile(
                "Parameters" + Logger.LS
                        + "\t\\Qbatch count = [1]" + Logger.LS + "\t\t01 = [88888]" + Logger.LS
                        + "\tbatch count = [2]" + Logger.LS + "\t\t01 = [99999]\\E"
        ));
        assertLog("終了ログ", Pattern.compile(
                "nablarch.core.db.statement.BasicSqlPStatement#executeBatch"
                        + Logger.LS + "\texecute time\\(ms\\) = \\[[0-9]+\\] batch count = \\[2\\]"));
    }

    /**
     * {@link BasicSqlPStatement#executeBatch()}が実行できること。
     */
    @Test
    public void executeBatch() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (?)");
        sut.setString(1, "88888");
        sut.addBatch();
        sut.setString(1, "99999");
        sut.addBatch();
        final int[] result = sut.executeBatch();
        dbCon.commit();

        assertThat("戻り値のサイズは2", result.length, is(2));
        assertThat("executeBatch実行後はバッチサイズは0になる", sut.getBatchSize(), is(0));

        final List<TestEntity> actual = VariousDbTestHelper.findAll(TestEntity.class, "id");
        assertThat("2レコード増えていること", actual.size(), is(5));

        assertThat("登録されていること", actual.get(3).id, is("88888"));
        assertThat("登録されていること", actual.get(4).id, is("99999"));
    }

    /**
     * {@link BasicSqlPStatement#executeBatch()}でSQLExceptionが発生した場合、
     * SqlStatementExceptionが送出されること。
     */
    @Test(expected = SqlStatementException.class)
    public void executeBatch_SQLException(
            @Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.executeBatch();
            result = new SQLException("executeBatch error", "", 999);
        }};

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (?)");
        sut.setString(1, "99999");
        sut.addBatch();
        Deencapsulation.setField(sut, mockStatement);
        sut.executeBatch();
    }

    /**
     * {@link BasicSqlPStatement#clearBatch()} のテスト。
     */
    @Test
    public void clearBatch() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (?)");
        sut.setString(1, "99999");
        sut.addBatch();

        assertThat("追加されたのでバッチサイズは1", sut.getBatchSize(), is(1));

        sut.clearBatch();
        assertThat("クリアしたのでバッチサイズは0", sut.getBatchSize(), is(0));
    }

    public static class ParamObject {

        private String id = null;

        public String getId() {
            return id == null ? "12345" : id;
        }
    }

    /**
     * {@link BasicSqlPStatement#clearBatch()} のテスト。
     *
     * パラメータ化されたステートメントオブジェクトでも使用できることを確認する。
     */
    @Test
    public void clearBatch_Parametarized() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (:id)");

        final ParamObject param = new ParamObject();
        sut.addBatchObject(param);

        assertThat("追加されたのでバッチサイズは1", sut.getBatchSize(), is(1));

        sut.clearBatch();
        assertThat("クリアしたのでバッチサイズは0", sut.getBatchSize(), is(0));
    }


    /**
     * {@link BasicSqlPStatement#clearBatch()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void clearBatch_SQLException(
            @Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.clearBatch();
            result = new SQLException("clear batch error");
        }};

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES (?)");
        Deencapsulation.setField(sut, mockStatement);
        sut.clearBatch();
    }

    /**
     * {@link BasicSqlPStatement#setNull(int, int)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void setNull() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, VARCHAR_COL, LONG_COL, DATE_COL, TIMESTAMP_COL, DECIMAL_COL, BINARY_COL, INTEGER_COL) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
        sut.setString(1, "99999");
        sut.setNull(2, Types.VARCHAR);
        sut.setNull(3, Types.BIGINT);
        sut.setNull(4, Types.DATE);
        sut.setNull(5, Types.TIMESTAMP);
        sut.setNull(6, Types.DECIMAL);
        sut.setNull(7, Types.BINARY);
        sut.setNull(8, Types.INTEGER);
        final int updated = sut.executeUpdate();

        assertThat("1レコード登録出来ている", updated, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "99999");
        assertThat(actual.varcharCol, nullValue());
        assertThat(actual.longCol, is(nullValue()));
        assertThat(actual.dateCol, nullValue());
        assertThat(actual.timestampCol, nullValue());
        assertThat(actual.decimalCol, nullValue());
        assertThat(actual.binaryCol, nullValue());
        assertThat(actual.integerCol, nullValue());
    }

    /**
     * {@link BasicSqlPStatement#setNull(int, int)}のSQLログのテスト。
     */
    @Test
    public void setNull_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, VARCHAR_COL) "
                        + "VALUES (?, ?)");
        sut.setString(1, "99999");
        sut.setNull(2, Types.VARCHAR);
        sut.executeUpdate();

        assertLog("nullパラメータのログ", Pattern.compile("\\QParameters" + Logger.LS
                + "\t01 = [99999]" + Logger.LS
                + "\t02 = [null]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setNull(int, int)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void setNull_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setNull(anyInt, anyInt);
            result = new SQLException("setNull error");
        }};

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, VARCHAR_COL) "
                        + "VALUES (?, ?)");

        Deencapsulation.setField(sut, mockStatement);
        sut.setString(1, "99999");
        sut.setNull(2, Types.VARCHAR);
    }

    /**
     * {@link BasicSqlPStatement#setBoolean(int, boolean)}のテスト。
     *
     * @throws Exception
     */
    @Test
    public void setBoolean() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "UPDATE STATEMENT_TEST_TABLE SET BOOLEAN_COL = ? WHERE ENTITY_ID = ?");
        sut.setBoolean(1, true);
        sut.setString(2, "10001");

        final int updated = sut.executeUpdate();

        assertThat("1レコード更新される", updated, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "10001");
        assertThat("trueを設定したので1に更新される", actual.booleanCol, is(true));
    }

    /**
     * {@link BasicSqlPStatement#setBoolean(int, boolean)}のSQLログのテスト。
     */
    @Test
    public void setBoolean_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "UPDATE STATEMENT_TEST_TABLE SET BOOLEAN_COL = ?");
        sut.setBoolean(1, true);
        sut.executeUpdate();

        assertLog("Booleanパラメータが出力されていること", Pattern.compile(
                "\\QParameters" + Logger.LS
                        + "\t01 = [true]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setBoolean(int, boolean)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void setBoolean_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setBoolean(anyInt, anyBoolean);
            result = new SQLException("setBoolean error");
        }};
        final SqlPStatement sut = dbCon.prepareStatement(
                "UPDATE STATEMENT_TEST_TABLE SET VARCHAR_COL = ?, LONG_COL = ?");
        Deencapsulation.setField(sut, mockStatement);
        sut.setBoolean(2, false);
    }

    /**
     * {@link BasicSqlPStatement#setByte(int, byte)}のテスト。
     */
    @Test
    public void setByte() throws Exception {
        final SqlPStatement sut= dbCon.prepareStatement(
                "UPDATE STATEMENT_TEST_TABLE SET INTEGER_COL = ?, LONG_COL = ?, VARCHAR_COL = ? WHERE ENTITY_ID = ?");
        sut.setByte(1, (byte) 0x30);
        sut.setByte(2, (byte) 0x31);
        sut.setByte(3, (byte) 0x32);
        sut.setString(4, "10002");

        final int updated = sut.executeUpdate();
        assertThat("1レコード更新される", updated, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "10002");
        assertThat(actual.integerCol, is(0x30));
        assertThat(actual.longCol, is((long) 0x31));
        assertThat(actual.varcharCol, is("50"));        // 0x32 -> 50
    }

    /**
     * {@link BasicSqlPStatement#setByte(int, byte)}のSQLログのテスト。
     */
    @Test
    public void setByte_writeSqlLog() throws Exception {
        final SqlPStatement sut= dbCon.prepareStatement(
                "UPDATE STATEMENT_TEST_TABLE SET INTEGER_COL = ?, LONG_COL = ?");
        sut.setByte(1, (byte) 0x30);
        sut.setByte(2, (byte) 0x31);
        sut.executeUpdate();

        assertLog("パラメータ情報がログに出力されていること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [48]" + Logger.LS
                        + "\t02 = [49]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setByte(int, byte)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void setByte_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setByte(anyInt, anyByte);
            result = new SQLException("setByte error.");
        }};
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT '1' FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        Deencapsulation.setField(sut, mockStatement);
        sut.setByte(1, (byte) 0x00);
    }

    /**
     * {@link BasicSqlPStatement#setShort(int, short)}のテスト。
     */
    @Test
    public void setShort() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE LONG_COL = ?");
        sut.setShort(1, (short) 20000);
        final SqlResultSet actual = sut.retrieve();

        assertThat("データが取得できること", actual.size(), is(1));
        assertThat(actual.get(0)
                .getLong("longCol"), is(20000L));
    }

    /**
     * {@link BasicSqlPStatement#setShort(int, short)}のSQLログのテスト。
     */
    @Test
    public void setShort_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE LONG_COL = ?");
        sut.setShort(1, (short) 20000);
        sut.retrieve();

        assertLog("パラメータがログ出力されること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [20000]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setShort(int, short)}でSQLExceptinoが発生するケース。
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void setShort_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setShort(anyInt, anyShort);
            result = new SQLException("setShort error");
        }};
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE LONG_COL = ?");
        Deencapsulation.setField(sut, mockStatement);
        sut.setShort(1, (short) 1);
    }

    /**
     * {@link BasicSqlPStatement#setInt(int, int)}のテスト。
     */
    @Test
    public void setInt() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE INTEGER_COL = ?");
        sut.setInt(1, 2);
        final SqlResultSet actual = sut.retrieve();

        assertThat("1レコード取得できる", actual.size(), is(1));
        assertThat("条件の値が取得できる", actual.get(0)
                .getInteger("integerCol"), is(2));
    }

    /**
     * {@link BasicSqlPStatement#setInt(int, int)}のSQLログのテスト。
     */
    @Test
    public void setInt_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE INTEGER_COL IN (?, ?)");
        sut.setInt(1, 2);
        sut.setInt(2, 1);
        sut.retrieve();

        assertLog("パラメータが出力されていること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [2]" + Logger.LS
                        + "\t02 = [1]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setInt(int, int)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void setInt_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setInt(anyInt, anyInt);
            result = new SQLException("setInt error");
        }};
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE INTEGER_COL IN (?, ?)");
        Deencapsulation.setField(sut, mockStatement);
        sut.setInt(1, 2);
    }

    /**
     * {@link BasicSqlPStatement#setLong(int, long)}のテスト。
     */
    @Test
    public void setLong() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE LONG_COL = ?");
        sut.setLong(1, 30000L);
        final SqlResultSet actual = sut.retrieve();

        assertThat("1レコード取得できる", actual.size(), is(1));
        assertThat("条件の値が取得できる", actual.get(0)
                .getLong("longCol"), is(30000L));
    }

    /**
     * {@link BasicSqlPStatement#setLong(int, long)}のSQLログのテスト。
     */
    @Test
    public void setLong_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE LONG_COL IN (?, ?)");
        sut.setLong(1, 30000L);
        sut.setLong(2, 10000L);
        sut.retrieve();

        assertLog("パラメータが出力されていること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [30000]" + Logger.LS
                        + "\t02 = [10000]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setLong(int, long)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void setLong_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setLong(anyInt, anyLong);
            result = new SQLException("setLong error");
        }};
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE INTEGER_COL IN (?, ?)");
        Deencapsulation.setField(sut, mockStatement);
        sut.setLong(1, 2);
    }

    /**
     * {@link BasicSqlPStatement#setFloat(int, float)}のテスト。
     */
    @Test
    public void setFloat() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE FLOAT_COL > ?");
        sut.setFloat(1, 3.2f);
        final SqlResultSet actual = sut.retrieve();

        assertThat("1レコード取得できる", actual.size(), is(1));
        assertThat("条件の値が取得できる", actual.get(0)
                .getBigDecimal("floatCol")
                .floatValue(), is(3.3f));
    }

    /**
     * {@link BasicSqlPStatement#setFloat(int, float)}のSQLログのテスト。
     */
    @Test
    public void setFloat_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE FLOAT_COL IN (?, ?)");
        sut.setFloat(1, 2.2f);
        sut.setFloat(2, 1.1f);
        sut.retrieve();

        assertLog("パラメータが出力されていること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [2.2]" + Logger.LS
                        + "\t02 = [1.1]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setLong(int, long)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void setFloat_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setFloat(anyInt, anyFloat);
            result = new SQLException("setFloat error");
        }};
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE INTEGER_COL IN (?, ?)");
        Deencapsulation.setField(sut, mockStatement);
        sut.setFloat(1, 2.2f);
    }

    /**
     * {@link BasicSqlPStatement#setDouble(int, double)}のテスト。
     */
    @Test
    public void setDouble() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("insert into STATEMENT_TEST_TABLE (entity_id, float_col) values (?, ?)");
        sut.setString(1, "99999");
        sut.setDouble(2, 9.0);
        sut.executeUpdate();
        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "99999");

        assertThat("条件の値が取得できる", (double) actual.floatCol, is(9.0));
    }

    /**
     * {@link BasicSqlPStatement#setDouble(int, double)}のSQLログのテスト。
     */
    @Test
    public void setDouble_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE FLOAT_COL IN (?, ?)");
        sut.setDouble(1, 2.2);
        sut.setDouble(2, 3.3);
        sut.retrieve();

        assertLog("パラメータが出力されていること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [2.2]" + Logger.LS
                        + "\t02 = [3.3]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setDouble(int, double)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void setDouble_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setDouble(anyInt, anyDouble);
            result = new SQLException("setDouble error");
        }};
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE INTEGER_COL IN (?, ?)");
        Deencapsulation.setField(sut, mockStatement);
        sut.setDouble(1, 2.2);
    }

    /**
     * {@link BasicSqlPStatement#setBigDecimal(int, BigDecimal)}のテスト。
     */
    @Test
    public void setBigDecimal() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE FLOAT_COL > ?");
        sut.setBigDecimal(1, new BigDecimal("3.2"));
        final SqlResultSet actual = sut.retrieve();

        assertThat("1レコード取得できる", actual.size(), is(1));
        assertThat("条件の値が取得できる", actual.get(0)
                .getBigDecimal("floatCol")
                .floatValue(), is(3.3f));
    }

    /**
     * {@link BasicSqlPStatement#setBigDecimal(int, BigDecimal)}のSQLログのテスト。
     */
    @Test
    public void setBigDecimal_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE FLOAT_COL IN (?, ?)");
        sut.setBigDecimal(1, new BigDecimal("2.2"));
        sut.setBigDecimal(2, new BigDecimal("3.3"));
        sut.retrieve();

        assertLog("パラメータが出力されていること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [2.2]" + Logger.LS
                        + "\t02 = [3.3]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setBigDecimal(int, BigDecimal)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void setBigDecimal_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setBigDecimal(anyInt, BigDecimal.ONE);
            result = new SQLException("setBigDecimal error");
        }};
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        Deencapsulation.setField(sut, mockStatement);
        sut.setBigDecimal(1, BigDecimal.ONE);
    }

    /**
     * {@link BasicSqlPStatement#setString(int, String)}のテスト。
     */
    @Test
    public void setString() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        sut.setString(1, "10001");
        final SqlResultSet actual = sut.retrieve();

        assertThat("1レコード取得できる", actual.size(), is(1));
        assertThat("条件の値が取得できる", actual.get(0)
                .getString("entityId"), is("10001"));
    }

    /**
     * {@link BasicSqlPStatement#setString(int, String)}のSQLログのテスト。
     */
    @Test
    public void setString_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID IN(?, ?)");
        sut.setString(1, "10001");
        sut.setString(2, "99999");
        sut.retrieve();

        assertLog("パラメータが出力されていること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [10001]" + Logger.LS
                        + "\t02 = [99999]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setString(int, String)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void setString_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setString(anyInt, anyString);
            result = new SQLException("setString error");
        }};
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        Deencapsulation.setField(sut, mockStatement);
        sut.setString(1, "1");
    }

    /**
     * {@link BasicSqlPStatement#setBytes(int, byte[])}のテスト。
     */
    @Test
    public void setBytes() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, BINARY_COL) VALUES (?, ?)");

        sut.setString(1, "99999");
        sut.setBytes(2, new byte[] {0x00, 0x30, 0x31});
        final int inserted = sut.executeUpdate();
        assertThat("1レコード登録できている", inserted, is(1));
        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "99999");
        assertThat("バイナリデータが登録出来ている", actual.binaryCol, is(new byte[] {0x00, 0x30, 0x31}));
    }

     /**
     * {@link BasicSqlPStatement#setBytes(int, byte[])}のSQLログの出力テスト。
     */
    @Test
    public void setBytes_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, BINARY_COL) VALUES (?, ?)");

        sut.setString(1, "99999");
        sut.setBytes(2, new byte[] {0x00, 0x30, 0x31});
        sut.executeUpdate();

        assertLog("パラメータがログに出力されていること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [99999]" + Logger.LS
                        + "\t02 = [bytes]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setBytes(int, byte[])}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void setBytes_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setBytes(anyInt, (byte[]) withNotNull());
            result = new SQLException("setBytes error");
        }};

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, BINARY_COL) VALUES (?, ?)");
        Deencapsulation.setField(sut, mockStatement);
        sut.setBytes(2, new byte[] {0x00, 0x01});
    }

    /**
     * {@link BasicSqlPStatement#setDate(int, java.sql.Date)}のテスト。
     */
    @Test
    public void setDate() throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2015, 1, 20, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        final java.sql.Date insertDate = new java.sql.Date(calendar.getTimeInMillis());

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, DATE_COL) VALUES  (?, ?)");
        sut.setString(1, "99999");
        sut.setDate(2, insertDate);

        final int updated = sut.executeUpdate();
        assertThat("1レコード登録されている", updated, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "99999");
        assertThat("日付が登録出来ている", actual.dateCol, is((Date) insertDate));
    }

    /**
     * {@link BasicSqlPStatement#setDate(int, java.sql.Date)}のSQLログのテスト。
     */
    @Test
    public void setDate_writeSqlLog() throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2015, 1, 20, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        final java.sql.Date insertDate = new java.sql.Date(calendar.getTimeInMillis());

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, DATE_COL) VALUES  (?, ?)");
        sut.setString(1, "99999");
        sut.setDate(2, insertDate);
        sut.executeUpdate();

        assertLog("パラメータがログ出力されていること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [99999]" + Logger.LS
                        + "\t02 = [2015-02-20]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setDate(int, java.sql.Date)}のSQLExceptionが発生するケース。
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void setDate_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations(){{
            mockStatement.setDate(anyInt, withAny(new java.sql.Date(0)));
            result = new SQLException("setDate error");
        }};

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, DATE_COL) VALUES  (?, ?)");

        Deencapsulation.setField(sut, mockStatement);
        sut.setDate(2, new java.sql.Date(0));
    }

    /**
     * {@link BasicSqlPStatement#setTime(int, Time)}のテスト。
     */
    @Test
    public void setTime() throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.set(0, 0, 0, 1, 2, 3);
        calendar.set(Calendar.MILLISECOND, 0);
        final Time insertTime = new Time(calendar.getTimeInMillis());

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT  INTO STATEMENT_TEST_TABLE (ENTITY_ID, TIME_COL) VALUES (?, ?)");
        sut.setString(1, "88888");
        sut.setTime(2, insertTime);
        final int inserted = sut.executeUpdate();
        assertThat("1レコード登録される", inserted, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "88888");

        assertThat("Timeが登録されること", actual.timeCol.toString(), is(insertTime.toString()));
    }

    /**
     * {@link BasicSqlPStatement#setTime(int, Time)}のSQLログのテスト。
     */
    @Test
    public void setTime_writeSqlLog() throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.set(0, 0, 0, 1, 2, 3);
        calendar.set(Calendar.MILLISECOND, 0);
        final Time insertTime = new Time(calendar.getTimeInMillis());

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT  INTO STATEMENT_TEST_TABLE (ENTITY_ID, TIME_COL) VALUES (?, ?)");
        sut.setString(1, "88888");
        sut.setTime(2, insertTime);
        sut.executeUpdate();

        assertLog("パラメータがログ出力されること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [88888]" + Logger.LS
                        + "\t02 = [01:02:03]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setTime(int, Time)}のSQLExceptionが発生するケース。
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void setTime_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations(){{
            mockStatement.setTime(anyInt, withAny(new Time(0)));
            result = new SQLException("setTime error");
        }};

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, TIME_COL) VALUES  (?, ?)");

        Deencapsulation.setField(sut, mockStatement);
        sut.setTime(2, new Time(0));
    }

    /**
     * {@link BasicSqlPStatement#setTimestamp(int, Timestamp)}のテスト。
     */
    @Test
    public void setTimestamp() throws Exception {
        final Timestamp insertTimestamp = Timestamp.valueOf("2015-03-14 11:12:13.007");

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, TIMESTAMP_COL) VALUES (?, ?)");
        sut.setString(1, "77777");
        sut.setTimestamp(2, insertTimestamp);
        final int inserted = sut.executeUpdate();

        assertThat("1レコード登録される", inserted, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "77777");
        assertThat("登録されていること", actual.timestampCol, is(insertTimestamp));
    }

    /**
     * {@link BasicSqlPStatement#setTimestamp(int, Timestamp)}のSQLログのテスト。
     */
    @Test
    public void setTimestamp_writeSqlLog() throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2015, 1, 19, 1, 2, 3);
        calendar.set(Calendar.MILLISECOND, 321);
        final Timestamp insertTimestamp = new Timestamp(calendar.getTimeInMillis());

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, TIMESTAMP_COL) VALUES (?, ?)");
        sut.setString(1, "77777");
        sut.setTimestamp(2, insertTimestamp);
        sut.executeUpdate();

        assertLog("パラメータがログ出力されること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [77777]" + Logger.LS
                        + "\t02 = [2015-02-19 01:02:03.321]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setTimestamp(int, Timestamp)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void setTimestamp_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations(){{
            mockStatement.setTimestamp(anyInt, withAny(new Timestamp(0)));
            result = new SQLException("setTimestamp error");
        }};

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, TIMESTAMP_COL) VALUES  (?, ?)");

        Deencapsulation.setField(sut, mockStatement);
        sut.setTimestamp(2, new Timestamp(0));
    }

    /**
     * {@link BasicSqlPStatement#setAsciiStream(int, InputStream, int)}のテスト。
     */
    @Test
    @TargetDb(exclude = {TargetDb.Db.SQL_SERVER, TargetDb.Db.DB2})
    public void setAsciiStream() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, VARCHAR_COL) VALUES (?, ?)");
        sut.setString(1, "55555");
        InputStream stream = new ByteArrayInputStream("12345".getBytes("utf-8"));
        sut.setAsciiStream(2, stream, 2);
        final int inserted = sut.executeUpdate();
        stream.close();
        assertThat("1レコード登録される", inserted, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "55555");
        assertThat("登録されていること", actual.varcharCol, is("12"));

    }

    /**
     * {@link BasicSqlPStatement#setAsciiStream(int, InputStream, int)}のテスト。
     *
     * ※SQLServerは、Streamの長さとlengthを一致させる必要があるため別でテストを実施
     */
    @Test
    @TargetDb(include = {TargetDb.Db.SQL_SERVER, TargetDb.Db.DB2})
    public void setAsciiStream_SQLServer_DB2() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, VARCHAR_COL) VALUES (?, ?)");
        sut.setString(1, "55555");
        InputStream stream = new ByteArrayInputStream("12345".getBytes("utf-8"));
        sut.setAsciiStream(2, stream, 5);
        final int inserted = sut.executeUpdate();
        stream.close();
        assertThat("1レコード登録される", inserted, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "55555");
        assertThat("登録されていること", actual.varcharCol, is("12345"));

    }

    /**
     * {@link BasicSqlPStatement#setAsciiStream(int, InputStream, int)}のSQLログのテスト。
     */
    @Test
    public void setAsciiStream_writeSqlLog() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, VARCHAR_COL) VALUES (?, ?)");
        sut.setString(1, "55555");
        InputStream stream = new ByteArrayInputStream("12345".getBytes("utf-8"));
        sut.setAsciiStream(2, stream, 5);
        sut.executeUpdate();
        stream.close();

        assertLog("パラメータがログ出力されること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [55555]" + Logger.LS
                        + "\t02 = [InputStream]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setAsciiStream(int, InputStream, int)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void setAsciiStream_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations(){{
            mockStatement.setAsciiStream(anyInt, withAny(new ByteArrayInputStream(new byte[0])), anyInt);
            result = new SQLException("setAsciiStream error");
        }};

        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, VARCHAR_COL) VALUES  (?, ?)");
        Deencapsulation.setField(sut, mockStatement);
        sut.setAsciiStream(2, new ByteArrayInputStream(new byte[0]), 0);
    }

    /**
     * {@link BasicSqlPStatement#setObject(int, Object)}のテスト。
     */
    @Test
    public void setObject() throws Exception {
        final Timestamp timestamp = new Timestamp(0);

        final SqlPStatement sut = dbCon.prepareStatement(
                "insert into STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, INTEGER_COL, TIMESTAMP_COL) VALUES (?, ?, ?, ?)");
        sut.setObject(1, "44444");
        sut.setObject(2, 100L);
        sut.setObject(3, -1);
        sut.setObject(4, timestamp);
        final int inserted = sut.executeUpdate();
        assertThat("1レコード登録される", inserted, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "44444");
        assertThat(actual.id, is("44444"));
        assertThat(actual.longCol, is(100L));
        assertThat(actual.integerCol, is(-1));
        assertThat(actual.timestampCol, is(timestamp));
    }

    /**
     * {@link BasicSqlPStatement#setObject(int, Object)}のSQLログのテスト。
     */
    @Test
    public void setObject_writeSqlLog() throws Exception {

        final SqlPStatement sut = dbCon.prepareStatement(
                "insert into STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, INTEGER_COL) VALUES (?, ?, ?)");
        sut.setObject(1, "44444");
        sut.setObject(2, 100L);
        sut.setObject(3, -1);
        sut.executeUpdate();

        assertLog("パラメータがログ出力されること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [44444]" + Logger.LS
                        + "\t02 = [100]" + Logger.LS
                        + "\t03 = [-1]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setObject(int, Object)}でSQLExceptionが発生する場合は
     * DbAccessExceptionが送出される。
     */
    @Test(expected = DbAccessException.class)
    public void setObject_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setObject(anyInt, any);
            result = new SQLException("setObject error");
        }};
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE ID = ?");
        Deencapsulation.setField(sut, mockStatement);
        sut.setObject(1, "12345");
    }

    /**
     * {@link BasicSqlPStatement#setObject(int, Object, int)}のテスト。
     */
    @Test
    public void setObjectWithType() throws Exception {
        final Timestamp timestamp = new Timestamp(0);

        final SqlPStatement sut = dbCon.prepareStatement(
                "insert into STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, INTEGER_COL, TIMESTAMP_COL) VALUES (?, ?, ?, ?)");
        sut.setObject(1, "44444", Types.CHAR);
        sut.setObject(2, 100, Types.BIGINT);
        sut.setObject(3, -1, Types.INTEGER);
        sut.setObject(4, timestamp, Types.TIMESTAMP);
        final int inserted = sut.executeUpdate();
        assertThat("1レコード登録される", inserted, is(1));

        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "44444");
        assertThat(actual.id, is("44444"));
        assertThat(actual.longCol, is(100L));
        assertThat(actual.integerCol, is(-1));
        assertThat(actual.timestampCol, is(timestamp));
    }

    /**
     * {@link BasicSqlPStatement#setObject(int, Object, int)}のSQLログのテスト。
     */
    @Test
    public void setObjectWithType_writeSqlLog() throws Exception {

        final SqlPStatement sut = dbCon.prepareStatement(
                "insert into STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, INTEGER_COL) VALUES (?, ?, ?)");
        sut.setObject(1, "44444", Types.CHAR);
        sut.setObject(2, 100L, Types.BIGINT);
        sut.setObject(3, -1, Types.INTEGER);
        sut.executeUpdate();

        assertLog("パラメータがログ出力されること",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\t01 = [44444]" + Logger.LS
                        + "\t02 = [100]" + Logger.LS
                        + "\t03 = [-1]\\E"));
    }

    /**
     * {@link BasicSqlPStatement#setObject(int, Object, int)}でSQLExceptionが発生する場合は
     * DbAccessExceptionが送出される。
     */
    @Test(expected = DbAccessException.class)
    public void setObjectWithType_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setObject(anyInt, any, anyInt);
            result = new SQLException("setObjectWithType error");
        }};
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE ID = ?");
        Deencapsulation.setField(sut, mockStatement);
        sut.setObject(1, "12345", Types.CHAR);
    }

    /**
     * {@link BasicSqlPStatement#getResultSet()}のテスト。
     */
    @Test
    public void getResultSet() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT ENTITY_ID FROM  STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        sut.setString(1, "10002");
        final boolean result = sut.execute();
        assertThat(result, is(true));
        final ResultSet rs = sut.getResultSet();
        try {
            assertThat(rs.next(), is(true));
            assertThat(rs.getString(1), is("10002"));
            assertThat(rs.next(), is(false));
        } finally {
            rs.close();
        }
    }

    /**
     * {@link BasicSqlPStatement#getResultSet()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void getResultSet_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.getResultSet();
            result = new SQLException("getResultSet error");
        }};
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        Deencapsulation.setField(sut, mockStatement);
        sut.getResultSet();
    }

    /**
     * {@link BasicSqlPStatement#getUpdateCount()}のテスト。
     */
    @Test
    public void getUpdateCount() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("UPDATE STATEMENT_TEST_TABLE SET ENTITY_ID = ENTITY_ID");
        assertThat("更新件数は3", sut.executeUpdate(), is(3));
        assertThat("更新件数が取得できる", sut.getUpdateCount(), is(3));
    }

    public static class ParameterizedParameterObject {

        private String id1 = "10001";
        private String id2 = "10002";

        public String getId1() {
            return id1;
        }

        public String getId2() {
            return id2;
        }
    }

    /**
     * {@link BasicSqlPStatement#getUpdateCount()}のテスト。
     * <p/>
     * パラメタ化されたステートメントの場合でも、更新後に更新件数が取得できること。
     */
    @Test
    public void getUpdateCount_Parametarized() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "UPDATE STATEMENT_TEST_TABLE SET ENTITY_ID = ENTITY_ID WHERE ENTITY_ID = :id1 OR ENTITY_ID = :id2");
        assertThat("更新件数は2", sut.executeUpdateByObject(new ParameterizedParameterObject()), is(2));
        assertThat("更新件数が取得できる", sut.getUpdateCount(), is(2));
    }

    /**
     * {@link BasicSqlPStatement#getUpdateCount()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void getUpdateCount_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.getUpdateCount();
            result = new SQLException("getUpdateCount error");
        }};
        final SqlPStatement sut = dbCon.prepareStatement("UPDATE STATEMENT_TEST_TABLE SET ENTITY_ID = ENTITY_ID");
        Deencapsulation.setField(sut, mockStatement);
        sut.getUpdateCount();
    }

    /**
     * {@link BasicSqlPStatement#getMetaData()}のテスト。
     */
    @Test
    public void getMetaData() throws Exception {
        SqlPStatement sut = dbCon.prepareStatement("SELECT ENTITY_ID, LONG_COL from STATEMENT_TEST_TABLE where ENTITY_ID = ?");
        final ResultSetMetaData actual = sut.getMetaData();
        assertThat(actual, is(notNullValue()));
    }

    /**
     * {@link BasicSqlPStatement#getMetaData()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void getMetaData_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.getMetaData();
            result = new SQLException("getMetaData error");
        }};
        SqlPStatement sut = dbCon.prepareStatement(
                "SELECT ENTITY_ID, LONG_COL from STATEMENT_TEST_TABLE where ENTITY_ID = ?");
        Deencapsulation.setField(sut, mockStatement);
        sut.getMetaData();
    }

    /**
     * {@link BasicSqlPStatement#setMaxRows(int)}と{@link BasicSqlPStatement#getMaxRows()}のテスト。
     */
    @Test
    public void setMaxRows() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        sut.setMaxRows(2);

        assertThat("設定した値が取得できること", sut.getMaxRows(), is(2));
        final ResultSetIterator actual = sut.executeQuery();
        assertThat("1レコード目は取得できる", actual.next(), is(true));
        assertThat("2レコード目は取得できる", actual.next(), is(true));
        assertThat("maxrowsが2なので3レコード目は取得されない", actual.next(), is(false));
    }

    /**
     * {@link BasicSqlPStatement#setMaxRows(int)}と{@link BasicSqlPStatement#getMaxRows()}のテスト。
     *
     * パラメータ化されたステートメントオブジェクトでも使用できることを確認する。
     */
    @Test
    public void setMaxRows_Parametarized() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE");
        sut.setMaxRows(2);

        assertThat("設定した値が取得できること", sut.getMaxRows(), is(2));
        final ResultSetIterator actual = sut.executeQueryByObject(new Object());
        assertThat("1レコード目は取得できる", actual.next(), is(true));
        assertThat("2レコード目は取得できる", actual.next(), is(true));
        assertThat("maxrowsが2なので3レコード目は取得されない", actual.next(), is(false));
    }

    /**
     * {@link BasicSqlPStatement#setMaxRows(int)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void setMaxRows_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setMaxRows(anyInt);
            result = new SQLException("setMaxRows error");
        }};

        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        Deencapsulation.setField(sut, mockStatement);
        sut.setMaxRows(2);
    }

    /**
     * {@link BasicSqlPStatement#getMaxRows()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void getMaxRows_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.getMaxRows();
            result = new SQLException("getMaxRows error");
        }};
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        Deencapsulation.setField(sut, mockStatement);
        sut.getMaxRows();
    }

    /**
     * {@link BasicSqlPStatement#setFetchSize(int)}と{@link BasicSqlPStatement#getFetchSize()}のテスト。
     */
    @Test
    public void setFetchSize() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        sut.setFetchSize(2);
        assertThat("設定したFetchSizeが取得できること", sut.getFetchSize(), is(2));
    }

    /**
     * {@link BasicSqlPStatement#setFetchSize(int)}と{@link BasicSqlPStatement#getFetchSize()}のテスト。
     *
     * パラメータ化されたステートメントオブジェクトでも使用できることを確認する。
     */
    @Test
    public void setFetchSize_Parametarized() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE");
        sut.setFetchSize(7);
        assertThat("設定したFetchSizeが取得できること", sut.getFetchSize(), is(7));
    }

    /**
     * {@link BasicSqlPStatement#setFetchSize(int)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void setFetchSize_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setFetchSize(anyInt);
            result = new SQLException("setFetchSize error");
        }};

        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        Deencapsulation.setField(sut, mockStatement);
        sut.setFetchSize(10);
    }

    /**
     * {@link BasicSqlPStatement#getFetchSize()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void getFetchSize_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.getFetchSize();
            result = new SQLException("getFetchSize error");
        }};

        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        Deencapsulation.setField(sut, mockStatement);
        sut.setFetchSize(10);
        sut.getFetchSize();
    }

    /**
     * {@link BasicSqlPStatement#setQueryTimeout(int)}と{@link BasicSqlPStatement#getQueryTimeout()}のテスト。
     */
    @Test
    public void setQueryTimeout() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        sut.setQueryTimeout(999);
        assertThat("設定したQueryTimeoutが取得できること", sut.getQueryTimeout(), is(999));
    }

    /**
     * {@link BasicSqlPStatement#setQueryTimeout(int)}と{@link BasicSqlPStatement#getQueryTimeout()}のテスト。
     */
    @Test
    public void setQueryTimeout_Parametarized() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE");
        sut.setQueryTimeout(321);
        assertThat("設定したQueryTimeoutが取得できること", sut.getQueryTimeout(), is(321));
    }

    /**
     * {@link BasicSqlPStatement#setQueryTimeout(int)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void setQueryTimeout_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setQueryTimeout(anyInt);
            result = new SQLException("setQueryTimeout error");
        }};

        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        Deencapsulation.setField(sut, mockStatement);
        sut.setQueryTimeout(10);
    }

    /**
     * {@link BasicSqlPStatement#getQueryTimeout()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void getQueryTimeout_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.getQueryTimeout();
            result = new SQLException("getQueryTimeout error");
        }};

        final SqlPStatement sut = dbCon.prepareStatement("SELECT * FROM STATEMENT_TEST_TABLE");
        Deencapsulation.setField(sut, mockStatement);
        sut.getQueryTimeout();
    }

    /**
     * {@link BasicSqlPStatement#setBinaryStream(int, InputStream, int)}のテスト。
     */
    @Test
    @TargetDb(exclude = {TargetDb.Db.SQL_SERVER, TargetDb.Db.DB2})
    public void setBinaryStream() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, BINARY_COL) VALUES (?, ?)");
        sut.setString(1, "55555");
        sut.setBinaryStream(2, new ByteArrayInputStream(new byte[]{0x31, 0x32, 0x33}), 2);
        final int updated = sut.executeUpdate();
        assertThat("1レコード更新される", updated, is(1));
        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "55555");
        assertThat("登録されていること", actual.binaryCol, is(new byte[] {0x31, 0x32}));
    }

    /**
     * {@link BasicSqlPStatement#setBinaryStream(int, InputStream, int)}のテスト。
     *
     * ※SQLServerはStreamの長さとlengthを一致させる必要がある。
     */
    @Test
    @TargetDb(include = {TargetDb.Db.SQL_SERVER, TargetDb.Db.DB2})
    public void setBinaryStream_SQLServer() throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, BINARY_COL) VALUES (?, ?)");
        sut.setString(1, "55555");
        sut.setBinaryStream(2, new ByteArrayInputStream(new byte[]{0x31, 0x32, 0x33}), 3);
        final int updated = sut.executeUpdate();
        assertThat("1レコード更新される", updated, is(1));
        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "55555");
        assertThat("登録されていること", actual.binaryCol, is(new byte[] {0x31, 0x32, 0x33}));
    }

    /**
     * {@link BasicSqlPStatement#setBinaryStream(int, InputStream, int)}でSQLExceptionが発生した場合、
     * DbAccessExceptionが発生する。
     */
    @Test(expected = DbAccessException.class)
    public void setBinaryStream_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setBinaryStream(anyInt, withAny(new ByteArrayInputStream(new byte[0])), anyInt);
            result = new SQLException("setBinaryStream error");
        }};
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, BINARY_COL) VALUES (?, ?)");

        Deencapsulation.setField(sut, mockStatement);
        sut.setBinaryStream(2, new ByteArrayInputStream(new byte[] {0x31, 0x32, 0x33}), 2);
    }

    /**
     * {@link BasicSqlPStatement#close()}と{@link BasicSqlPStatement#isClosed()}のテスト。
     */
    @Test
    public void close() {
        final List<SqlStatement> statements = Deencapsulation.getField(dbCon, "statements");
        assertThat("ステートメントリストは空であること", statements.isEmpty(), is(true));

        final SqlPStatement sut = dbCon.prepareStatement("select * from STATEMENT_TEST_TABLE");
        assertThat("ステートメントリストに追加されていること", statements.size(), is(1));

        assertThat("クローズ前はクローズされていないこと", sut.isClosed(), is(false));
        sut.close();

        assertThat("クローズされていること", sut.isClosed(), is(true));
        assertThat("クローズされるとステートメントリストからも削除されること", statements.isEmpty(), is(true));

        try {
            sut.executeQuery();
            fail("ここはとおらない");
        } catch (SqlStatementException e) {
            assertThat(e, is(notNullValue()));
        }
    }

    /**
     * {@link BasicSqlPStatement#close()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void close_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.close();
            result = new SQLException("close error");
        }};
        final SqlPStatement sut = dbCon.prepareStatement("select * from STATEMENT_TEST_TABLE");
        Deencapsulation.setField(sut, mockStatement);
        sut.close();
    }

    /**
     * {@link BasicSqlPStatement#getMoreResults()}のテスト
     *
     * Mockを使ってテストを行う。
     */
    @Test
    public void getMoreResults(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.getMoreResults();
            returns(true, false);
        }};
        final SqlPStatement sut = dbCon.prepareStatement("select * from STATEMENT_TEST_TABLE");
        Deencapsulation.setField(sut, mockStatement);

        assertThat("初回はtrue", sut.getMoreResults(), is(true));
        assertThat("2回目はfalse", sut.getMoreResults(), is(false));
    }

    /**
     * {@link BasicSqlPStatement#getMoreResults()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void getMoreResults_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.getMoreResults();
            result = new SQLException("getMoreResults error");
        }};
        final SqlPStatement sut = dbCon.prepareStatement("select * from STATEMENT_TEST_TABLE");
        Deencapsulation.setField(sut, mockStatement);
        sut.getMoreResults();
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Map)}のテスト。
     */
    @Test
    public void retrieve_map() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id");

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "10002");

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat("1レコード取得できること", actual.size(), is(1));
        assertThat(actual.get(0)
                .getString("entityId"), is("10002"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}のテスト。
     */
    @Test
    public void retrieve_map_withOffsetAndLimit() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id ORDER BY ENTITY_ID");

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "10002");

        final SqlResultSet firstRow = sut.retrieve(1, 1, condition);
        assertThat("1レコード取得できること", firstRow.size(), is(1));
        assertThat(firstRow.get(0)
                .getString("entityId"), is("10001"));

        final SqlResultSet secondRow = sut.retrieve(2, 1, condition);
        assertThat("1レコード取得できること", secondRow.size(), is(1));
        assertThat(secondRow.get(0)
                .getString("entityId"), is("10002"));

        final SqlResultSet multiRow = sut.retrieve(1, 2, condition);
        assertThat("2レコード取得できること", multiRow.size(), is(2));
        assertThat(multiRow.get(0)
                .getString("entityId"), is("10001"));
        assertThat(multiRow.get(1)
                .getString("entityId"), is("10002"));
    }

    /**
     * ステートメント生成時に設定された検索処理オプションで検索条件にMAPを使用しても、範囲指定できる事。
     *
     * @throws Exception 例外
     */
    @Test
    public void retrieve_map_withSelectOption() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID >= :id ORDER BY ENTITY_ID", new SelectOption(2, 2));
        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "10001");

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat("2レコード取得できること", actual.size(), is(2));
        assertThat(actual.get(0).getString("entityId"), is("10002"));
        assertThat(actual.get(1).getString("entityId"), is("10003"));
    }

    /**
     * ステートメント生成時と実行時に検索範囲を指定した場合例外が発生すること。
     *
     * @throws Exception 例外
     */
    @Test(expected = IllegalOperationException.class)
    public void retrieve_map_withDuplicatePagenate() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id ORDER BY ENTITY_ID", new SelectOption(2, 2));
        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "10002");
        sut.retrieve(1, 1, condition);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}のSQLログのテスト。
     */
    @Test
    public void retrieve_map_writeSqlLog() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id ORDER BY ENTITY_ID");

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "10002");
        sut.retrieve(1, 2, condition);

        assertLog("開始ログ",
                Pattern.compile("\\Qnablarch.core.db.statement.BasicSqlPStatement#retrieve"
                        + " SQL = [SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= ? ORDER BY ENTITY_ID]"
                        + Logger.LS
                        + "\tstart position = [1] size = [2] queryTimeout = [600] fetchSize = [2]" + Logger.LS
                        + "\toriginal sql = [SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id ORDER BY ENTITY_ID]\\E"));

        assertLog("パラメータ",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\tid = [10002]\\E"));

        assertLog("終了ログ",
                Pattern.compile("nablarch.core.db.statement.BasicSqlPStatement#retrieve" + Logger.LS
                        + "\texecute time\\(ms\\) = \\[[0-9]+\\] retrieve time\\(ms\\) = \\[[0-9]+\\] "));
    }

    @Test
    public void retrieve_writeLogWithMapOffsetSupport() throws Exception {
        String convertedSql = "SELECT 'TEST_CODE' FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID > :id ORDER BY ENTITY_ID";
        setDialect(dbCon, new OffsetSupportDialcet(convertedSql));
        ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id",
                new SelectOption(2, 3));
        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "1");
        sut.retrieve(condition);
        assertLog("開始ログ",
                Pattern.compile("\\Qnablarch.core.db.statement.BasicSqlPStatement#retrieve"
                        + " SQL = [SELECT 'TEST_CODE' FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID > ? ORDER BY ENTITY_ID]"
                        + Logger.LS
                        + "\tstart position = [1] size = [0] queryTimeout = [600] fetchSize = [50]" + Logger.LS
                        + "\toriginal sql = ["+ convertedSql +"]\\E"));
        assertLog("パラメータ",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\tid = [1]\\E"));
        assertLog("終了ログ",
                Pattern.compile("nablarch.core.db.statement.BasicSqlPStatement#retrieve" + Logger.LS
                        + "\texecute time\\(ms\\) = \\[[0-9]+\\] retrieve time\\(ms\\) = \\[[0-9]+\\] "));
    }

    @Test
    public void retrieve_writeLogWithMapOffsetUnSupport() throws Exception {
        setDialect(dbCon, new DefaultDialect());
        ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement("SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id", new SelectOption(2, 3));
        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "1");
        sut.retrieve(condition);
        assertLog("開始ログ",
                Pattern.compile("\\Qnablarch.core.db.statement.BasicSqlPStatement#retrieve"
                        + " SQL = [SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= ?]"
                        + Logger.LS
                        + "\tstart position = [2] size = [3] queryTimeout = [600] fetchSize = [3]" + Logger.LS
                        + "\toriginal sql = [SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id]\\E"));
        assertLog("パラメータ",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\tid = [1]\\E"));
        assertLog("終了ログ",
                Pattern.compile("nablarch.core.db.statement.BasicSqlPStatement#retrieve" + Logger.LS
                        + "\texecute time\\(ms\\) = \\[[0-9]+\\] retrieve time\\(ms\\) = \\[[0-9]+\\] "));

    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}の前方一致のテスト
     */
    @Test
    public void retrieve_map_with_likeForwardMatch() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID LIKE :id% ORDER BY ENTITY_ID");

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "1");
        final SqlResultSet actual = sut.retrieve(1, 2, condition);
        assertThat(actual.size(), is(2));
        assertThat(actual.get(0)
                .getString("entityId"), is("10001"));
        assertThat(actual.get(1)
                .getString("entityId"), is("10002"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}の部分一致のテスト
     */
    @Test
    public void retrieve_map_with_likePartialMatch() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID LIKE :%id% ORDER BY ENTITY_ID");

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "000");
        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(3));

        assertThat(actual.get(0)
                .getString("entityId"), is("10001"));
        assertThat(actual.get(1)
                .getString("entityId"), is("10002"));
        assertThat(actual.get(2)
                .getString("entityId"), is("10003"));

    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}の後方一致のテスト
     */
    @Test
    public void retrieve_map_with_likeBackwardMatch() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID LIKE :%id ORDER BY ENTITY_ID");

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "0002");
        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));

        assertThat(actual.get(0)
                .getString("entityId"), is("10002"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}のLIKE検索でエスケープ対象の文字が含まれている場合
     */
    @Test
    public void retrieve_map_with_likeWithEscapeChar() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE VARCHAR_COL LIKE :condition% ORDER BY ENTITY_ID");

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("condition", "c_\\");
        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));

        assertThat(actual.get(0)
                .getString("entityId"), is("10003"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}で、条件の名前付きパラメータがMapに存在しない場合、
     * 例外が送出されること。
     */
    @Test
    public void retrieve_map_invalidKey() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id AND VARCHAR_COL = :varcharCol ORDER BY ENTITY_ID");

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "0002");
        try {
            sut.retrieve(condition);
            fail("とおらない");
        } catch (IllegalArgumentException e) {
            assertThat("エラーになる。", e.getMessage(), is("SQL parameter was not found in Object. parameter name=[varcharCol]"));
        }
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}で、条件の名前付きパラメータがMapに存在しない場合、
     * 例外が送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void retrieve_map_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setObject(anyInt, any);
            result = new SQLException("retrieve map error");
        }};
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id AND VARCHAR_COL = :varcharCol ORDER BY ENTITY_ID");
        Deencapsulation.setField(sut, mockStatement);

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "10002");
        condition.put("varcharCol", "b");

        sut.retrieve(condition);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}のテスト。
     */
    @Test
    public void retrieve_object() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id");

        final TestEntity condition = new TestEntity();
        condition.id = "10002";

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat("1レコード取得できること", actual.size(), is(1));
        assertThat(actual.get(0)
                .getString("entityId"), is("10002"));
    }

    public static class WithEnum {
        enum Authority {
            ADMIN,
            STANDARD
        }

        private Authority authority = Authority.ADMIN;

        public String getAuthorityString() {
            if (authority == Authority.ADMIN) {
                return "10001";
            } else {
                return "10002";
            }
        }
    }

    /**
     * フィールドにenumオブジェクトを持つオブジェクトを条件に検索。
     *
     * アクセッサ(getter)で文字列表現に変換して、その条件で検索できていることを検証する。
     */
    @Test
    public void retrieve_objectWithEnum() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :authorityString");

        final WithEnum withEnum = new WithEnum();
        withEnum.authority = WithEnum.Authority.STANDARD;

        final SqlResultSet actual = sut.retrieve(withEnum);
        assertThat(actual, hasSize(1));
        assertThat(actual.get(0).getString("entityId"), is("10002"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}のテスト。
     */
    @Test
    public void retrieve_object_withOption() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID > :id ORDER BY ENTITY_ID", new SelectOption(2, 3));
        final TestEntity condition = new TestEntity();
        condition.id = "1";

        final SqlResultSet actual = sut.retrieve(condition);
        assertThat("2レコード取得できること", actual.size(), is(2));
        assertThat(actual.get(0)
                .getString("entityId"), is("10002"));
        assertThat(actual.get(1)
                .getString("entityId"), is("10003"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Object)}のテスト。
     */
    @Test
    public void retrieve_object_withOffsetAndLimit() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id ORDER BY ENTITY_ID");

        final TestEntity condition = new TestEntity();
        condition.id = "10002";

        final SqlResultSet firstRow = sut.retrieve(1, 1, condition);
        assertThat("1レコード取得できること", firstRow.size(), is(1));
        assertThat(firstRow.get(0)
                .getString("entityId"), is("10001"));

        final SqlResultSet secondRow = sut.retrieve(2, 1, condition);
        assertThat("1レコード取得できること", secondRow.size(), is(1));
        assertThat(secondRow.get(0)
                .getString("entityId"), is("10002"));

        final SqlResultSet multiRow = sut.retrieve(1, 2, condition);
        assertThat("2レコード取得できること", multiRow.size(), is(2));
        assertThat(multiRow.get(0)
                .getString("entityId"), is("10001"));
        assertThat(multiRow.get(1)
                .getString("entityId"), is("10002"));
    }

    @Test(expected = IllegalOperationException.class)
    public void retrieve_object_withDuplicatePagenate() {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID > :id ORDER BY ENTITY_ID", new SelectOption(2, 3));
        final TestEntity condition = new TestEntity();
        condition.id = "1";
        sut.retrieve(1, 2, condition);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}のSQLログのテスト。
     */
    @Test
    public void retrieve_object_writeSqlLog() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id ORDER BY ENTITY_ID");

        final TestEntity condition = new TestEntity();
        condition.id = "10002";
        sut.setFetchSize(123);
        sut.setQueryTimeout(30);
        sut.retrieve(condition);

        assertLog("開始ログ",
                Pattern.compile("\\Qnablarch.core.db.statement.BasicSqlPStatement#retrieve"
                        + " SQL = [SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= ? ORDER BY ENTITY_ID]"
                        + Logger.LS
                        + "\tstart position = [1] size = [0] queryTimeout = [30] fetchSize = [123]" + Logger.LS
                        + "\toriginal sql = [SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID <= :id ORDER BY ENTITY_ID]\\E"));

        assertLog("パラメータ",
                Pattern.compile("\\QParameters" + Logger.LS
                        + "\tid = [10002]\\E"));

        assertLog("終了ログ",
                Pattern.compile("nablarch.core.db.statement.BasicSqlPStatement#retrieve" + Logger.LS
                        + "\texecute time\\(ms\\) = \\[[0-9]+\\] retrieve time\\(ms\\) = \\[[0-9]+\\] "));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Object)}の前方一致のテスト
     */
    @Test
    public void retrieve_object_with_likeForwardMatch() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID LIKE :id% ORDER BY ENTITY_ID");

        final TestEntity condition = new TestEntity();
        condition.id = "1";
        final SqlResultSet actual = sut.retrieve(1, 2, condition);
        assertThat(actual.size(), is(2));
        assertThat(actual.get(0)
                .getString("entityId"), is("10001"));
        assertThat(actual.get(1)
                .getString("entityId"), is("10002"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Object)}の部分一致のテスト
     */
    @Test
    public void retrieve_object_with_likePartialMatch() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID LIKE :%id% ORDER BY ENTITY_ID");

        final TestEntity condition = new TestEntity();
        condition.id = "000";
        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(3));

        assertThat(actual.get(0)
                .getString("entityId"), is("10001"));
        assertThat(actual.get(1)
                .getString("entityId"), is("10002"));
        assertThat(actual.get(2)
                .getString("entityId"), is("10003"));

    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Object)}の後方一致のテスト
     */
    @Test
    public void retrieve_object_with_likeBackwardMatch() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID LIKE :%id ORDER BY ENTITY_ID");

        final TestEntity condition = new TestEntity();
        condition.id = "0002";
        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));

        assertThat(actual.get(0)
                .getString("entityId"), is("10002"));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Object)}のLIKE検索でエスケープ対象の文字が含まれている場合
     */
    @Test
    public void retrieve_object_with_likeWithEscapeChar() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE VARCHAR_COL LIKE :id% ORDER BY ENTITY_ID");

        final TestEntity condition = new TestEntity();
        condition.id = "c_\\";
        final SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));

        assertThat(actual.get(0)
                .getString("entityId"), is("10003"));
    }


    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Object)}で、条件の名前付きパラメータがオブジェクトのフィールド
     * に存在しない場合、例外が送出されること。
     */
    @Test
    public void retrieve_object_invalidProperty() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id AND VARCHAR_COL = :invalid ORDER BY ENTITY_ID");

        final TestEntity condition = new TestEntity();
        condition.id = "0002";
        try {
            sut.retrieve(condition);
            fail("とおらない");
        } catch (IllegalArgumentException e) {
            assertThat("エラーになる。", e.getMessage(),
                    containsString("SQL parameter was not found in Object. parameter name=[invalid]"));
        }
    }

    /**
     * {@link BasicSqlPStatement#retrieve(int, int, Map)}で、条件の名前付きパラメータがMapに存在しない場合、
     * 例外が送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void retrieve_object_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setObject(anyInt, any);
            result = new SQLException("retrieve map error");
        }};
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id AND VARCHAR_COL = :varcharCol ORDER BY ENTITY_ID");
        Deencapsulation.setField(sut, mockStatement);

        final TestEntity condition = new TestEntity();
        condition.id = "10002";
        condition.varcharCol = "b";
        sut.retrieve(condition);
    }


    /**
     * {@link BasicSqlPStatement#executeQueryByMap(Map)}のテスト。
     */
    @Test
    public void executeQueryByMap() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id");

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "10002");

        final ResultSetIterator actual = sut.executeQueryByMap(condition);
        assertThat(actual.next(), is(true));
        assertThat(actual.getRow()
                .getString("entityId"), is("10002"));
        assertThat(actual.next(), is(false));
    }

    /**
     * {@link BasicSqlPStatement#executeQueryByMap(Map)}のSQLExceptionのテスト。
     */
    @Test(expected = DbAccessException.class)
    public void executeQueryByMap_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setObject(anyInt, any);
            result = new SQLException("executeQuery map error");
        }};
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id");

        Deencapsulation.setField(sut, mockStatement);
        Map<String, String> condition = new HashMap<String, String>();
        condition.put("id", "10002");
        sut.executeQueryByMap(condition);
    }

    /**
     * {@link BasicSqlPStatement#executeQueryByObject(Object)}
     */
    @Test
    public void executeQueryByObject() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id");

        final TestEntity entity = new TestEntity();
        entity.id = "10002";

        final ResultSetIterator actual = sut.executeQueryByObject(entity);
        assertThat(actual.next(), is(true));
        assertThat(actual.getRow()
                .getString("entityId"), is("10002"));
        assertThat(actual.next(), is(false));
    }

    /**
     * {@link BasicSqlPStatement#executeQueryByObject(Object)}のSQLExceptionのテスト。
     */
    @Test(expected = SqlStatementException.class)
    public void executeQueryByObject_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setObject(anyInt, any);
            result = new SQLException("executeQuery object error");
        }};
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id");

        Deencapsulation.setField(sut, mockStatement);

        final TestEntity entity = new TestEntity();
        entity.id = "10002";
        sut.executeQueryByObject(entity);
    }

    /**
     * {@link BasicSqlPStatement#executeUpdateByMap(Map)}のテスト。
     */
    @Test
    public void executeUpdateByMap() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, BINARY_COL) VALUES (:id, :long, :binary)");
        Map<String, Object> insertData = new HashMap<String, Object>();
        insertData.put("id", "44444");
        insertData.put("long", 100L);
        insertData.put("binary", new byte[]{0x00, 0x01});
        final int result = sut.executeUpdateByMap(insertData);

        assertThat("1レコード登録される", result, is(1));
        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "44444");
        assertThat(actual.id, is("44444"));
        assertThat(actual.longCol, is(100L));
        assertThat(actual.binaryCol, is(new byte[] {0x00, 0x01}));
    }

    /**
     * {@link BasicSqlPStatement#executeUpdateByMap(Map)}でSQLExceptionが発生する場合、
     * SqlStatementExceptionが送出されること。
     */
    @Test(expected = SqlStatementException.class)
    public void executeUpdateByMap_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setObject(anyInt, any);
            result = new SQLException("executeUpdate map error");
        }};
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, BINARY_COL) VALUES (:id, :long, :binary)");
        Deencapsulation.setField(sut, mockStatement);
        Map<String, Object> insertData = new HashMap<String, Object>();
        insertData.put("id", "44444");
        insertData.put("long", 100L);
        insertData.put("binary", new byte[] {0x00, 0x01});
        sut.executeUpdateByMap(insertData);
    }

    /**
     * {@link BasicSqlPStatement#executeUpdateByObject(Object)}のテスト。
     */
    @Test
    public void executeUpdateByObject() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, BINARY_COL) VALUES (:id, :longCol, :binaryCol)");
        final TestEntity entity = new TestEntity();
        entity.id = "44444";
        entity.longCol = 100L;
        entity.binaryCol = new byte[]{0x00, 0x01};
        final int result = sut.executeUpdateByObject(entity);
        dbCon.commit();

        final TestEntity actual = VariousDbTestHelper.findById(TestEntity.class, "44444");
        assertThat(actual.id, is(entity.id));
        assertThat(actual.longCol, is(entity.longCol));
        assertThat(actual.binaryCol, is(entity.binaryCol));
    }

    /**
     * {@link BasicSqlPStatement#executeUpdateByObject(Object)}のテスト。
     */
    @Test(expected = SqlStatementException.class)
    public void executeUpdateByObject_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setObject(anyInt, any);
            result = new SQLException("executeUpdate object error");
        }};
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, BINARY_COL) VALUES (:id, :long, :binary)");
        Deencapsulation.setField(sut, mockStatement);
        final TestEntity entity = new TestEntity();
        entity.id = "44444";
        entity.longCol = 100L;
        entity.binaryCol = new byte[]{0x00, 0x01};
        sut.executeUpdateByObject(entity);
    }

    /**
     * {@link BasicSqlPStatement#addBatchMap(Map)}のテスト。
     */
    @Test
    public void addBatchMap() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, BINARY_COL) VALUES (:id, :long, :binary)");
        Map<String, Object> insertData = new HashMap<String, Object>();
        insertData.put("id", "44444");
        insertData.put("long", 100L);
        insertData.put("binary", new byte[]{0x00, 0x01});
        sut.addBatchMap(insertData);
        assertThat("バッチサイズがインクリメントされること", sut.getBatchSize(), is(1));

        insertData.put("id", "55555");
        insertData.put("long", Long.MAX_VALUE);
        sut.addBatchMap(insertData);
        assertThat("バッチサイズがインクリメントされること", sut.getBatchSize(), is(2));

        sut.executeBatch();

        dbCon.commit();

        final TestEntity actual1 = VariousDbTestHelper.findById(TestEntity.class, "44444");
        assertThat(actual1.id, is("44444"));
        assertThat(actual1.longCol, is(100L));

        final TestEntity actual2 = VariousDbTestHelper.findById(TestEntity.class, "55555");
        assertThat(actual2.id, is("55555"));
        assertThat(actual2.longCol, is(Long.MAX_VALUE));
    }

    /**
     * {@link BasicSqlPStatement#addBatchMap(Map)}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void addBatchMap_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setObject(anyInt, any);
            result = new SQLException("addBatch map error");
        }};
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, BINARY_COL) VALUES (:id, :long, :binary)");
        Deencapsulation.setField(sut, mockStatement);
        Map<String, Object> insertData = new HashMap<String, Object>();
        insertData.put("id", "44444");
        insertData.put("long", 100L);
        insertData.put("binary", new byte[]{0x00, 0x01});
        sut.addBatchMap(insertData);
    }


    /**
     * {@link BasicSqlPStatement#addBatchObject(Object)}のテスト。
     */
    @Test
    public void addBatchObject() throws Exception {
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, BINARY_COL) VALUES (:id, :longCol, :binaryCol)");
        final TestEntity entity = new TestEntity();
        entity.id = "44444";
        entity.longCol = 100L;
        entity.binaryCol = new byte[]{0x00, 0x01};
        sut.addBatchObject(entity);

        assertThat("バッチサイズがインクリメントされる", sut.getBatchSize(), is(1));

        entity.id = "55555";
        entity.longCol = Long.MAX_VALUE;
        sut.addBatchObject(entity);
        assertThat("バッチサイズがインクリメントされる", sut.getBatchSize(), is(2));

        sut.executeBatch();
        dbCon.commit();

        final TestEntity actual1 = VariousDbTestHelper.findById(TestEntity.class, "44444");
        assertThat(actual1.id, is("44444"));
        assertThat(actual1.longCol, is(100L));

        final TestEntity actual2 = VariousDbTestHelper.findById(TestEntity.class, "55555");
        assertThat(actual2.id, is("55555"));
        assertThat(actual2.longCol, is(Long.MAX_VALUE));
    }

    /**
     * {@link BasicSqlPStatement#addBatchObject(Object)}でSQLExcepitonが発生した場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void addBatchObject_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.setObject(anyInt, any);
            result = new SQLException("addBatch object error");
        }};
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID, LONG_COL, BINARY_COL) VALUES (:id, :long, :binary)");
        Deencapsulation.setField(sut, mockStatement);
        final TestEntity entity = new TestEntity();
        entity.id = "12345";
        sut.addBatchObject(entity);
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}で可変条件(IF文)を持つSQLの場合
     */
    @Test
    public void retrieve_withIfCondition() throws Exception {
        final TestEntity condition = new TestEntity();
        condition.id = "1";

        ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID LIKE :id% AND $if(varcharCol) {VARCHAR_COL = :varcharCol}",
                condition);

        SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(3));

        condition.varcharCol = "";
        sut = dbCon.prepareParameterizedSqlStatement(
                "select * from STATEMENT_TEST_TABLE WHERE ENTITY_ID LIKE :id% AND $if(varcharCol) {VARCHAR_COL = :varcharCol}",
                condition);
        actual = sut.retrieve(condition);
        assertThat(actual.size(), is(3));

        condition.varcharCol = "a";
        sut = dbCon.prepareParameterizedSqlStatement(
                "select * from STATEMENT_TEST_TABLE WHERE ENTITY_ID LIKE :id% AND $if(varcharCol) {VARCHAR_COL = :varcharCol}",
                condition);
        actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}でIN条件を持つSQL文の場合
     */
    @Test
    public void retrieve_withInCondition() throws Exception {
        final TestEntity condition = new TestEntity();
        condition.varchars = new String[]{"a", "b"};
        ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE VARCHAR_COL IN (:varchars[]) ORDER BY ENTITY_ID",
                condition);
        SqlResultSet actual = sut.retrieve(condition);
        assertThat(actual.size(), is(2));
        assertThat(actual.get(0)
                .getString("varcharcol"), is("a"));
        assertThat(actual.get(1)
                .getString("varcharcol"), is("b"));

        sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE VARCHAR_COL IN (:varchars[1]) ORDER BY ENTITY_ID",
                condition);
        actual = sut.retrieve(condition);
        assertThat(actual.size(), is(1));
        assertThat(actual.get(0)
                .getString("varcharCol"), is("b"));

        condition.varchars = new String[0];
        sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE VARCHAR_COL IN (:varchars[]) ORDER BY ENTITY_ID",
                condition);
        actual = sut.retrieve(condition);
        assertThat(actual.size(), is(0));
    }

    /**
     * {@link BasicSqlPStatement#retrieve(Object)}でIN条件が不正な場合
     */
    @Test(expected = IllegalArgumentException.class)
    public void retrieve_invalidInCondition() throws Exception {
        final TestEntity condition = new TestEntity();
        condition.varchars = new String[]{"a", "b"};
        final ParameterizedSqlPStatement sut = dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE VARCHAR_COL IN (:varchars[9999999999]) ORDER BY ENTITY_ID",
                condition);
        sut.retrieve(condition);
    }


    /**
     * {@link BasicSqlPStatement#getGeneratedKeys()}でSQLExceptionが発生する場合、
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void getGeneratedKeys_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.getGeneratedKeys();
            result = new SQLException("getGeneratedKeys error");
        }};
        SqlPStatement sut = dbCon.prepareStatement("INSERT INTO STATEMENT_TEST_TABLE (ENTITY_ID) VALUES ('12345')");
        Deencapsulation.setField(sut, mockStatement);
        sut.getGeneratedKeys();
    }

    /**
     * {@link BasicSqlPStatement#getGeneratedKeys()}のテスト。
     */
    @Test
    @TargetDb(exclude = TargetDb.Db.SQL_SERVER)
    public void getGeneratedKeys() throws Exception {
        final Connection connection = VariousDbTestHelper.getNativeConnection();
        String pkName = "entity_id";
        try {
            final DatabaseMetaData data = connection.getMetaData();
            if (!data.storesMixedCaseIdentifiers() && data.storesUpperCaseIdentifiers()) {
                pkName = "ENTITY_ID";
            }
        } finally {
            connection.close();
        }
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO STATEMENT_TEST_TABLE(ENTITY_ID) VALUES (?)", new String[] {pkName});
        sut.setString(1, "12345");
        final int inserted = sut.executeUpdate();
        assertThat(inserted, is(1));

        final ResultSet actual = sut.getGeneratedKeys();
        try {
            assertThat(actual.next(), is(true));
            assertThat(actual.getString(1), is("12345"));
        } finally {
            actual.close();
        }
    }

    /**
     * {@link BasicSqlPStatement#getGeneratedKeys()}のテスト。
     *
     * ※SQLServerは、自動生成キーは自動生成カラムのみ対応
     */
    @Test
    @TargetDb(include = TargetDb.Db.SQL_SERVER)
    public void getGeneratedKeys_SQLServer() throws Exception {
        VariousDbTestHelper.createTable(SqlServerTestEntity.class);
        final SqlPStatement sut = dbCon.prepareStatement(
                "INSERT INTO statement_test_sqlserver(name) VALUES (?)", new String[] {"id"});
        sut.setString(1, "name");
        final int inserted = sut.executeUpdate();
        assertThat(inserted, is(1));

        final ResultSet actual = sut.getGeneratedKeys();
        try {
            assertThat(actual.next(), is(true));
            assertThat(actual.getLong(1), is(1L));
        } finally {
            actual.close();
        }
    }


    /**
     * {@link BasicSqlPStatement#clearParameters()} のテスト。
     */
    @Test
    public void clearParameters(@Mocked final PreparedStatement mockStatement) throws Exception {
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        Deencapsulation.setField(sut, mockStatement);
        sut.setString(1, "10001");
        sut.clearParameters();

        new Verifications(){{
            mockStatement.clearParameters();
            times = 1;
        }};
    }

    /**
     * {@link BasicSqlPStatement#clearParameters()}でSQLExceptinoが発生する場合
     * DbAccessExceptionが送出されること
     */
    @Test(expected = DbAccessException.class)
    public void clearParameters_SQLException(@Mocked final PreparedStatement mockStatement) throws Exception {
        new Expectations() {{
            mockStatement.clearParameters();
            result = new SQLException("clearParameters error");
        }};
        final SqlPStatement sut = dbCon.prepareStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = ?");
        Deencapsulation.setField(sut, mockStatement);
        sut.clearParameters();
    }

    /**
     * プレフックハンドラが未設定の場合でもエラーとならずに処理が実行出来ること。
     */
    @Test
    public void preHookHandlerWasNull() throws Exception {
        final BasicSqlPStatement sut = (BasicSqlPStatement) dbCon.prepareParameterizedSqlStatement(
                "SELECT * FROM STATEMENT_TEST_TABLE WHERE ENTITY_ID = :id");
        sut.setUpdatePreHookObjectHandlerList(null);

        final ParamObject object = new ParamObject();
        final SqlResultSet rs = sut.retrieve(object);
        assertThat("検索できること", rs, is(notNullValue()));
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

    /**
     * 任意のDialectをテスト用のコネクションに設定する。
     */
    private void setDialect(TransactionManagerConnection dbCon, Dialect dialect) {
        ((BasicDbConnection)dbCon).setContext(new DbExecutionContext(dbCon, dialect, TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
    }

    private static class OffsetSupportDialcet extends DefaultDialect {
        private String convertedSql;
        private OffsetSupportDialcet(String convertedSql) {
            this.convertedSql = convertedSql;
        }
        @Override
        public boolean supportsOffset() {
            return true;
        }
        @Override
        public String convertPaginationSql(String sql,
                SelectOption selectOption) {
            return convertedSql;
        }
    }
}


