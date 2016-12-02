package nablarch.core.db.statement;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import nablarch.core.db.DbAccessException;
import nablarch.core.db.DbExecutionContext;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.dialect.DefaultDialect;
import nablarch.core.transaction.TransactionContext;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;

/**
 * {@link ResultSetIterator}のテストクラス。
 *
 * @author hisaaki sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class ResultSetIteratorTest {

    @ClassRule
    public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    private final TransactionManagerConnection connection =
            repositoryResource.getComponentByType(ConnectionFactory.class)
                    .getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);

    /**
     * db connection for test
     */
    @BeforeClass
    public static void setupDatabase() {
        VariousDbTestHelper.createTable(TestEntity.class);
    }
    
    /** テストでDATE型に格納する値 */
    private Date testDate;

    private Timestamp testTimestamp = Timestamp.valueOf("2015-03-17 10:20:30.997");

    @Before
    public void setup() throws SQLException, ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2015, 2, 17, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        testDate = calendar.getTime();

        VariousDbTestHelper.setUpTable(
                new TestEntity("00001"),
                new TestEntity("00002", "あいうえお", (short) 12, 12345, 1234554321L, new BigDecimal("12.3"), testDate,
                        testTimestamp, new byte[] {0x30, 0x40, 0x31, 0x41}),
                new TestEntity("00003")
        );
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.terminate();
        }
    }

    /**
     * {@link ResultSetIterator#next()}のテスト。
     */
    @Test
    public void next() throws Exception {
        SqlPStatement statement = connection.prepareStatement("SELECT * FROM RS_TEST ORDER BY CHAR_COL");
        ResultSetIterator sut = statement.executeQuery();
        assertThat("1レコード目はある。", sut.next(), is(true));
        assertThat("2レコード目はある。", sut.next(), is(true));
        assertThat("3レコード目はある。", sut.next(), is(true));
        assertThat("4レコード目はない。", sut.next(), is(false));
    }

    /**
     * {@link ResultSetIterator#next()}でSQLExceptionが発生した場合、
     * が送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void next_SQLException(@Mocked final ResultSet mockRs) throws Exception {
        new Expectations() {{
            mockRs.next();
            result = new SQLException("next error");
        }};
        final ResultSetIterator sut = new ResultSetIterator(mockRs, null, null);
        sut.next();
    }

    /**
     * {@link ResultSetIterator#getRow()}のテスト。
     */
    @Test
    public void getRow() throws Exception {
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM RS_TEST ORDER BY CHAR_COL");
        final ResultSetIterator sut = statement.executeQuery();

        assertThat(sut.next(), is(true));
        assertThat("1レコード目", sut.getRow()
                .getString("char_col"), is("00001"));
        assertThat(sut.next(), is(true));
        assertThat("2レコード目", sut.getRow()
                .getString("char_col"), is("00002"));
        assertThat(sut.next(), is(true));
        assertThat("3レコード目", sut.getRow()
                .getString("char_col"), is("00003"));
    }

    /**
     * {@link ResultSetIterator#getRow()}でConvertorが設定されているケース。
     */
    @Test
    public void getRowWithConvertor() throws Exception {

        class Convertor implements ResultSetConvertor {

            @Override
            public Object convert(ResultSet rs, ResultSetMetaData rsmd, int columnIndex) throws SQLException {
                return rs.getString(columnIndex).substring(4);
            }

            @Override
            public boolean isConvertible(ResultSetMetaData rsmd, int columnIndex) throws SQLException {
                return columnIndex == 1;
            }
        }

        final PreparedStatement statement = connection.getConnection().prepareStatement(
                "SELECT * FROM RS_TEST ORDER BY CHAR_COL");
        final ResultSet rs = statement.executeQuery();
        final ResultSetIterator sut = new ResultSetIterator(rs, new Convertor(),
                new DbExecutionContext(connection, new DefaultDialect(), "default"));

        int index = 1;
        for (SqlRow row : sut) {
            assertThat("最後の一文字だけかえされる", row.getString("charCol"), is(String.valueOf(index)));
            index++;
        }

    }

    /**
     * {@link ResultSetIterator#iterator()}のテスト。
     */
    @Test
    public void iterator() throws Exception {
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM RS_TEST ORDER BY CHAR_COL");
        final ResultSetIterator sut = statement.executeQuery();
        final Iterator<SqlRow> iterator = sut.iterator();

        assertThat(iterator.hasNext(), is(true));
        assertThat("1レコード目", iterator.next()
                .getString("charCol"), is("00001"));
        assertThat(iterator.hasNext(), is(true));
        assertThat("2レコード目", iterator.next()
                .getString("charCol"), is("00002"));
        assertThat(iterator.hasNext(), is(true));
        assertThat("3レコード目", iterator.next()
                .getString("charCol"), is("00003"));
        assertThat("4レコード目は存在しない", iterator.hasNext(), is(false));
    }

    /**
     * {@link ResultSetIterator#iterator()}のテスト。
     * iteratorを拡張for文で処理できること。
     */
    @Test
    public void iterator_forEach() throws Exception {
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM RS_TEST ORDER BY CHAR_COL");
        final ResultSetIterator sut = statement.executeQuery();

        int index = 1;
        for (SqlRow row : sut) {
            assertThat(index + "レコード目", row.getString("charCol"), is("0000" + index));
            index++;
        }
    }

    /**
     * {@link ResultSetIterator#iterator()}のテスト。
     * SQLException発生時は、DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void iterator_SQLException(@Mocked final ResultSet mockRs) throws Exception {
        new Expectations() {{
            mockRs.next();
            result = new SQLException("error");
        }};
        final ResultSetIterator sut = new ResultSetIterator(mockRs, null, null);

        sut.iterator();
    }

    /**
     * {@link ResultSetIterator#iterator()}のテスト。
     * iteratorを複数回呼び出した場合はエラーとなること。
     */
    @Test(expected = IllegalStateException.class)
    public void iterator_multipleCall() throws Exception {
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM RS_TEST ORDER BY CHAR_COL");
        final ResultSetIterator sut = statement.executeQuery();
        final Iterator<SqlRow> iterator = sut.iterator();
        assertThat("1回目の呼び出しは正常", iterator, is(notNullValue()));

        // 2回目の呼び出し（ここでエラーとなる）
        sut.iterator();
    }

    /**
     * {@link ResultSetIterator#iterator()}のテスト。
     * removeはサポートしていないのでエラーとなること。
     */
    @Test(expected = UnsupportedOperationException.class)
    public void iterator_remove() throws Exception {
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM RS_TEST ORDER BY CHAR_COL");
        final ResultSetIterator sut = statement.executeQuery();
        final Iterator<SqlRow> iterator = sut.iterator();
        iterator.remove();
    }

    /**
     * {@link ResultSetIterator#iterator()}のテスト。
     * nextでSQLExceptionが発生した場合は、DbAccessExceptionとなること。
     */
    @Test(expected = DbAccessException.class)
    public void iterator_next_SQLException(@Mocked final ResultSet mockRs) throws Exception {
        new Expectations() {{
            mockRs.next();
            result = true;
            result = new SQLException("error");
        }};
        Iterator<SqlRow> iterator = null;
        try {
            final ResultSetIterator sut = new ResultSetIterator(
                    mockRs, null, new DbExecutionContext(connection, connection.getDialect(), "default"));
            iterator = sut.iterator();
        } catch (Exception e) {
            fail("ここは通らない");
        }
        iterator.next();
    }

    /**
     * {@link ResultSetIterator#getMetaData()}のテスト。
     */
    @Test
    public void getMetaData() throws Exception {
        final SqlPStatement statement = connection.prepareStatement("SELECT CHAR_COL, '1' hoge, '2' fuga FROM RS_TEST ORDER BY CHAR_COL");
        final ResultSetIterator sut = statement.executeQuery();
        final ResultSetMetaData data = sut.getMetaData();
        assertThat("取得できること", data, is(notNullValue()));
        assertThat("一応カラム数のみ確認", data.getColumnCount(), is(3));
    }

    /**
     * {@link ResultSetIterator#ResultSetIterator(ResultSet, ResultSetConvertor, DbExecutionContext)}でSQLExceptionが発生するケース。
     *
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void initialize_SQLException(@Mocked final ResultSet mockRs) throws Exception {
        new Expectations() {{
            mockRs.getMetaData();
            result = new SQLException("getMetaData error");
        }};
        new ResultSetIterator(mockRs, null, null);
    }

    /**
     * {@link ResultSetIterator#close()}のテスト。
     *
     * モックオブジェクトを使用して、内部の{@link ResultSet#close()}が確実に呼び出されていることを検証する。
     */
    @Test
    public void close(@Mocked final ResultSet mockRs) throws Exception {
        final ResultSetIterator sut = new ResultSetIterator(mockRs, null, null);
        sut.close();

        new Verifications() {{
            mockRs.close();
            times = 1;
        }};
    }

    /**
     * {@link ResultSetIterator#close()}のテスト。
     * SQLExceptionが発生した場合は、DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void close_SQLException(@Mocked final ResultSet mockRs) throws Exception {
        new Expectations() {{
            mockRs.close();
            result = new SQLException("close error");
        }};
        final ResultSetIterator sut = new ResultSetIterator(mockRs, null, null);
        sut.close();
    }

    /**
     * {@link ResultSetIterator#getObject(int)}のテスト。
     * <p/>
     * データベースの値がObjectで取得できること。
     */
    @Test
    public void getObject() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00002");
        assertThat("レコードが取得出来ていること", sut.next(), is(true));

        assertThat("文字列が取得できること", sut.getObject(1)
                .toString(), is("あいうえお"));
        assertThat("数値が取得できること", sut.getObject(2)
                .toString(), is("12345"));
        assertThat("数値(long)が取得できること", sut.getObject(3)
                .toString(), is("1234554321"));
    }

    /**
     * {@link ResultSetIterator#getObject(int)}でnullの取得ができること。
     */
    @Test
    public void getObject_null() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00001");
        assertThat("レコードが取得出来ていること", sut.next(), is(true));
        assertThat("DBがnullの場合、nullが取得されること", sut.getObject(1), is(nullValue()));
    }

    /**
     * {@link ResultSetIterator#getObject(int)}のテスト。
     * <p/>
     * {@link ResultSet}アクセス時に{@link SQLException}が発生するケース。
     */
    @Test
    public void getObject_SQLException(@Mocked final ResultSet mockRs) throws Exception {
        new Expectations() {{
            mockRs.getObject(anyInt);
            result = new SQLException("getObject error");
        }};

        final ResultSetIterator sut = new ResultSetIterator(mockRs, null, null);
        try {
            sut.getObject(2);
            fail("とおらないこと");
        } catch (DbAccessException e) {
            assertThat("エラーメッセージ", e.getMessage(), is("failed to getObject. column index = [2]"));
            assertThat("causeはSQLException", e.getCause(), is(instanceOf(SQLException.class)));
        }
    }

    /**
     * {@link ResultSetIterator#getString(int)}のテスト。
     */
    @Test
    public void getString() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00002");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));

        assertThat("文字列型が取得できること", sut.getString(1), is("あいうえお"));
        assertThat("正数型(int)が文字列表記で取得できること", sut.getString(2), is("12345"));
        assertThat("正数型(long)が文字列表記で取得できること", sut.getString(3), is("1234554321"));
        assertThat("正数型(short)が文字列表記で取得できること", sut.getString(4), is("12"));
    }

    /**
     * {@link ResultSetIterator#getString(int)}のテスト。
     * <p/>
     * DBの値がnullの場合、nullが取得できること。
     */
    @Test
    public void getString_null() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00003");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));

        assertThat("nullが取得できること", sut.getString(1), is(nullValue()));
    }

    /**
     * {@link ResultSetIterator#getString(int)}のテスト。
     * <p/>
     * {@link ResultSet}アクセス時に{@link SQLException}が発生するケース。
     */
    @Test
    public void getString_SQLException(@Mocked final ResultSet mockRs) throws Exception {
        new Expectations() {{
            mockRs.getString(anyInt);
            result = new SQLException("getString error");
        }};

        final ResultSetIterator sut = new ResultSetIterator(mockRs, null, null);
        try {
            sut.getString(5);
            fail("とおらない");
        } catch (Exception e) {
            assertThat("メッセージ", e.getMessage(), is("failed to getString. column index = [5]"));
            assertThat("causeはSQLExceptionであること", e.getCause(), is(instanceOf(SQLException.class)));
        }
    }

    /**
     * {@link ResultSetIterator#getInteger(int)}のテスト。
     */
    @Test
    public void getInteger() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00002");

        assertThat("レコードが取得できること", sut.next(), is(true));

        assertThat("Integerで取得できること", sut.getInteger(2), is(12345));
        assertThat("smallint互換の型もIntegerで取得できる", sut.getInteger(4), is(12));
        assertThat("文字列型の値もIntegerで取得できる", sut.getInteger(6), is(2));
    }

    /**
     * {@link ResultSetIterator#getInteger(int)}のテスト。
     * <p/>
     * DBの値がnullの場合、nullが取得できること。
     */
    @Test
    public void getInteger_null() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00003");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));

        assertThat("nullが取得できること", sut.getInteger(2), is(nullValue()));
        assertThat("nullが取得できること", sut.getInteger(4), is(nullValue()));
    }

    /**
     * {@link ResultSetIterator#getInteger(int)}のテスト。
     * <p/>
     * 非数値の場合には、例外が発生すること。
     */
    @Test(expected = NumberFormatException.class)
    public void getInteger_error() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00002");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));
        sut.getInteger(1);      // 1 -> あいうえお
    }

    /**
     * {@link ResultSetIterator#getLong(int)}のテスト。
     */
    @Test
    public void getLong() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00002");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));

        assertThat("Longで取得できること", sut.getLong(3), is(1234554321L));
        assertThat("文字列型の値もLongで取得できる", sut.getLong(6), is(2L));
    }

    /**
     * {@link ResultSetIterator#getLong(int)}のテスト。
     * <p/>
     * DBの値がnullの場合、nullが取得できること。
     */
    @Test
    public void getLong_null() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00003");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));

        assertThat("nullが取得できること", sut.getLong(2), is(nullValue()));
        assertThat("nullが取得できること", sut.getLong(4), is(nullValue()));
    }

    /**
     * {@link ResultSetIterator#getLong(int)}のテスト。
     * <p/>
     * 非数値の場合には、例外が発生すること。
     */
    @Test(expected = NumberFormatException.class)
    public void getLong_error() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00002");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));
        sut.getLong(1);      // 1 -> あいうえお
    }

    /**
     * {@link ResultSetIterator#getShort(int)}のテスト。
     */
    @Test
    public void getShort() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00002");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));

        assertThat("shortで取得できること", sut.getShort(4), is((short) 12));
        assertThat("文字列型もShortで取得できる", sut.getShort(6), is((short) 2));
    }

    /**
     * {@link ResultSetIterator#getShort(int)}のテスト。
     * <p/>
     * DBの値がnullの場合、nullが取得できること。
     */
    @Test
    public void getShort_null() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00003");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));

        assertThat("nullが撮れること", sut.getShort(4), is(nullValue()));
    }

    /**
     * {@link ResultSetIterator#getShort(int)}のテスト。
     * <p/>
     * 非数値の場合には、例外が発生すること。
     */
    @Test(expected = NumberFormatException.class)
    public void getShort_error() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00002");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));
        sut.getShort(1);      // 1 -> あいうえお
    }

    /**
     * {@link ResultSetIterator#getBigDecimal(int)}のテスト。
     */
    @Test
    public void getBigDecimal() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00002");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));

        assertThat("BigDecimalで取得できること", sut.getBigDecimal(2), is(new BigDecimal("12345")));
        assertThat(sut.getBigDecimal(3), is(new BigDecimal("1234554321")));
        assertThat(sut.getBigDecimal(4), is(new BigDecimal("12")));
        assertThat(sut.getBigDecimal(5), is(new BigDecimal("12.3")));
        assertThat(sut.getBigDecimal(6), is(new BigDecimal("2")));
    }

    /**
     * {@link ResultSetIterator#getBigDecimal(int)}のテスト。
     * <p/>
     * DBの値がnullの場合、nullが取得できること。
     */
    @Test
    public void getBigDecimal_null() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00003");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));

        assertThat("null", sut.getBigDecimal(1), is(nullValue()));
    }

    /**
     * {@link ResultSetIterator#getBigDecimal(int)} のテスト。
     * <p/>
     * {@link ResultSet}アクセス時に{@link SQLException}が発生するケース。
     */
    @Test
    public void getBigDecimal_SQLException(@Mocked final ResultSet mockRs) throws Exception {
        new Expectations(){{
            mockRs.getBigDecimal(anyInt);
            result = new SQLException("getBigdecimal error");
        }};

        final ResultSetIterator sut = new ResultSetIterator(mockRs, null, null);

        try {
            sut.getBigDecimal(9);
            fail("とおらない");
        } catch (Exception e) {
            assertThat("メッセージ", e.getMessage(), is("failed to getBigDecimal. column index = [9]"));
            assertThat("causeはSQLExceptionであること", e.getCause(), is(instanceOf(SQLException.class)));
        }
    }

    /**
     * {@link ResultSetIterator#getDate(int)}のテスト。
     */
    @Test
    public void getDate() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00002");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));

        assertThat("date型で取得できること", sut.getDate(7), is(testDate));

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(testTimestamp.getTime());
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        assertThat("timestampがたもDATEで取れる", sut.getDate(8).toString(), is("2015-03-17"));
    }

    /**
     * {@link ResultSetIterator#getDate(int)}のテスト。
     * <p/>
     * DBの値がnullの場合、nullが取得できること。
     */
    @Test
    public void getDate_null() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00003");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));

        assertThat("nullが取得できること", sut.getDate(7), is(nullValue()));
        assertThat("nullが取得できること", sut.getDate(8), is(nullValue()));
    }

    /**
     * {@link ResultSetIterator#getDate(int)} のテスト。
     * <p/>
     * {@link ResultSet}アクセス時に{@link SQLException}が発生するケース。
     */
    @Test
    public void getDate_SQLException(@Mocked final ResultSet mockRs) throws Exception {
        new Expectations() {{
            mockRs.getDate(anyInt);
            result = new SQLException("getDate error");
        }};

        final ResultSetIterator sut = new ResultSetIterator(mockRs, null, null);
        try {
            sut.getDate(7);
            fail("とおらない");
        } catch (Exception e) {
            assertThat("メッセージ", e.getMessage(), is("failed to getDate. column index = [7]"));
            assertThat("causeはSQLExceptionであること", e.getCause(), is(instanceOf(SQLException.class)));
        }
    }

    /**
     * {@link ResultSetIterator#getTimestamp(int)}のテスト。
     */
    @Test
    public void getTimestamp() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00002");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));

        assertThat("timestampで取得ができること", sut.getTimestamp(8), is(testTimestamp));
        assertThat("date型の値もtimestampで取得ができること", sut.getTimestamp(7), is(new Timestamp(testDate.getTime())));
    }

    /**
     * {@link ResultSetIterator#getTimestamp(int)}のテスト。
     * <p/>
     * DBの値がnullの場合、nullが取得できること。
     */
    @Test
    public void getTimestamp_null() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00003");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));
        assertThat("nullが取得できること", sut.getTimestamp(7), is(nullValue()));
        assertThat("nullが取得できること", sut.getTimestamp(8), is(nullValue()));
    }

    /**
     * {@link ResultSetIterator#getTimestamp(int)} のテスト。
     * <p/>
     * {@link ResultSet}アクセス時に{@link SQLException}が発生するケース。
     */
    @Test
    public void getTimestamp_SQLException(@Mocked final ResultSet mockRs) throws Exception {
        new Expectations() {{
            mockRs.getTimestamp(anyInt);
            result = new SQLException("getTimestamp error");
        }};

        final ResultSetIterator sut = new ResultSetIterator(mockRs, null, null);

        try {
            sut.getTimestamp(6);
            fail("とおらない");
        } catch (Exception e) {
            assertThat("メッセージ", e.getMessage(), is("failed to getTimestamp. column index = [6]"));
            assertThat("causeはSQLExceptionであること", e.getCause(), is(instanceOf(SQLException.class)));
        }
    }

    /**
     * {@link ResultSetIterator#getBytes(int)}のテスト。
     */
    @Test
    public void getBytes() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00002");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));

        assertThat("バイナリが取得できる", sut.getBytes(9), is(new byte[] {0x30, 0x40, 0x31, 0x41}));
    }

    /**
     * {@link ResultSetIterator#getBytes(int)}のテスト。
     * <p/>
     * DBの値がnullの場合、nullが取得できること。
     */
    @Test
    public void getBytes_null() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00003");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));

        assertThat("nullが取得できる", sut.getBytes(9), is(nullValue()));
    }

    /**
     * {@link ResultSetIterator#getBytes(int)} のテスト。
     * <p/>
     * {@link ResultSet}アクセス時に{@link SQLException}が発生するケース。
     */
    @Test
    public void getBytes_SQLException(@Mocked final ResultSet mockRs) throws Exception {
        new Expectations() {{
            mockRs.getBytes(anyInt);
            result = new SQLException("getBytes error");
        }};

        final ResultSetIterator sut = new ResultSetIterator(mockRs, null, null);

        try {
            sut.getBytes(5);
            fail("とおらない");
        } catch (Exception e) {
            assertThat("メッセージ", e.getMessage(), is("failed to getBytes. column index = [5]"));
            assertThat("causeはSQLExceptionであること", e.getCause(), is(instanceOf(SQLException.class)));
        }
    }

    /**
     * {@link ResultSetIterator#getBlob(int)}のテスト。
     */
    @Test
    @TargetDb(exclude = {TargetDb.Db.POSTGRE_SQL, TargetDb.Db.SQL_SERVER})
    public void getBlob() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00002");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));

        Blob actual = sut.getBlob(9);
        assertThat("長さは4バイト", actual.length(), is(4L));
        assertThat("中身", actual.getBytes(1, 4), is(new byte[] {0x30, 0x40, 0x31, 0x41}));
    }

    /**
     * {@link ResultSetIterator#getBlob(int)}のテスト。
     * <p/>
     * DBの値がnullの場合、nullが取得できること。
     */
    @Test
    @TargetDb(exclude = {TargetDb.Db.POSTGRE_SQL, TargetDb.Db.SQL_SERVER})
    public void getBlob_null() throws Exception {
        final ResultSetIterator sut = createResultSetIterator("00003");

        assertThat("レコードが取得出来ていること", sut.next(), is(true));

        Blob actual = sut.getBlob(9);
        assertThat("nullが取得できる", actual, is(nullValue()));
    }

    /**
     * {@link ResultSetIterator#getBlob(int)} のテスト。
     * <p/>
     * {@link ResultSet}アクセス時に{@link SQLException}が発生するケース。
     */
    @Test
    @TargetDb(exclude = TargetDb.Db.POSTGRE_SQL)
    public void getBlob_SQLException(@Mocked final ResultSet mockRs) throws Exception {
        new Expectations() {{
            mockRs.getBlob(anyInt);
            result = new SQLException("getBlob error.");
        }};

        final ResultSetIterator sut = new ResultSetIterator(mockRs, null, null);

        try {
            sut.getBlob(4);
            fail("とおらない");
        } catch (Exception e) {
            assertThat("メッセージ", e.getMessage(), is("failed to getBlob. column index = [4]"));
            assertThat("causeはSQLExceptionであること", e.getCause(), is(instanceOf(SQLException.class)));
        }
    }

    /**
     * テスト対象の{@link ResultSetIterator}を生成する。
     *
     * @param id 主キー値
     * @return 生成した{@link ResultSetIterator}
     */
    private ResultSetIterator createResultSetIterator(String id) {
        final SqlPStatement statement = connection.prepareStatement("SELECT"
                + " VARCHAR_COL, INT_COL, LONG_COL, SHORT_COL, FLOAT_COL, CHAR_COL, DATE_COL, TIMESTAMP_COL, BIN_COL FROM RS_TEST WHERE CHAR_COL = ?");
        statement.setString(1, id);
        return statement.executeQuery();
    }

    @Entity
    @Table(name = "RS_TEST")
    public static class TestEntity {

        @Column(name = "char_col", length = 5)
        @Id
        public String charCol;

        @Column(name = "varchar_col", length = 1000)
        public String varcharCol;

        @Column(name = "short_col", length = 2)
        public Short shortCol;

        @Column(name = "int_col", length = 9)
        public Integer intCol;

        @Column(name = "long_col", length = 18)
        public Long longCol;

        @Column(name = "float_col", precision = 3, scale = 1)
        public BigDecimal floatCol;

        @Column(name = "date_col")
        @Temporal(TemporalType.DATE)
        public Date dateCol;

        @Column(name = "timestamp_col")
        public Timestamp timestampCol;

        @Column(name = "bin_col")
        public byte[] binCol;

        public TestEntity() {
        }

        public TestEntity(String charCol) {
            this.charCol = charCol;
        }

        public TestEntity(String charCol, String varcharCol, Short shortCol, Integer intCol, Long longCol,
                BigDecimal floatCol, Date dateCol, Timestamp timestampCol, byte[] binCol) {
            this.charCol = charCol;
            this.varcharCol = varcharCol;
            this.shortCol = shortCol;
            this.intCol = intCol;
            this.longCol = longCol;
            this.floatCol = floatCol;
            this.dateCol = dateCol;
            this.timestampCol = timestampCol;
            this.binCol = binCol;
        }
    }
}

