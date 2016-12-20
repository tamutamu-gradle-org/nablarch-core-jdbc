package nablarch.core.db.dialect;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.text.IsEmptyString.isEmptyString;
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.Calendar;
import java.util.Date;

import nablarch.core.db.statement.ResultSetConvertor;
import nablarch.core.db.statement.SelectOption;
import nablarch.core.db.util.DbUtil;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.DbTestRule;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

/**
 * {@link OracleDialect}のテストクラス。
 *
 *
 * @author hisaaki shioiri
 */
@TargetDb(include = TargetDb.Db.ORACLE)
@RunWith(DatabaseTestRunner.class)
public class OracleDialectTest {

    @Rule
    public DbTestRule dbTestRule = new DbTestRule();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /** テスト対象 */
    private OracleDialect sut = new OracleDialect();

    /** Native Connection */
    private Connection connection;

    @BeforeClass
    public static void setUp() throws Exception {
        VariousDbTestHelper.createTable(DialectEntity.class);
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * {@link OracleDialect#supportsIdentity()}のテスト。
     *
     * falseがかえされること
     */
    @Test
    public void supportsIdentity() throws Exception {
        assertThat("falseがかえされること", sut.supportsIdentity(), is(false));
    }

    /**
     * {@link OracleDialect#supportsSequence()}のテスト。
     *
     * trueがかえされること。
     */
    @Test
    public void supportsSequence() throws Exception {
        assertThat("trueがかえされること", sut.supportsSequence(), is(true));
    }

    /**
     * {@link OracleDialect#isDuplicateException(java.sql.SQLException)}のテスト。
     */
    @Test
    public void isDuplicateException() throws Exception {
        assertThat("errorCodeがなければfalse", sut.isDuplicateException(new SQLException("error")), is(false));
        assertThat("errorCodeがなければStateが1でもfalse",
                sut.isDuplicateException(new SQLException("error", "1")), is(false));
        assertThat("errorCodeが1ならtrue",
                sut.isDuplicateException(new SQLException("error", "1", 1)), is(true));
        assertThat("errorCodeが1以外ならfalse",
                sut.isDuplicateException(new SQLException("error", "1", 2)), is(false));
        assertThat("Stateがnullでも判定できること",
                sut.isDuplicateException(new SQLException("error", null, 1)), is(true));
        assertThat("Stateがnullでも判定できること",
                sut.isDuplicateException(new SQLException("error", null, 2)), is(false));
    }

    /**
     * {@link OracleDialect#supportsOffset()}のテスト。
     * <p/>
     * trueがかえされること。
     */
    @Test
    public void supportsOffset() throws Exception {
        assertThat("trueがかえされること", sut.supportsOffset(), is(true));
    }

    /**
     * {@link OracleDialect#isTransactionTimeoutError(SQLException)}のテスト。
     */
    @Test
    public void isTransactionTimeoutError() throws Exception {
        assertThat("エラーコード1はタイムアウトではない",
                sut.isTransactionTimeoutError(new SQLException("", "", 1)), is(false));
        assertThat("ロック要求タイムアウト時に発生するエラー(30006)はタイムアウト対象ではない",
                sut.isTransactionTimeoutError(new SQLException("", "", 30006)), is(false));

        assertThat("クエリータイムアウト時に発生するエラー(1013)はタイムアウト対象例外",
                sut.isTransactionTimeoutError(new SQLException("", "", 1013)), is(true));
    }

    /**
     * {@link OracleDialect#buildSequenceGeneratorSql(String)}のテスト。
     *
     * Oracleのシーケンスオブジェクトの次の値を取得する、以下形式のSQL文が生成されること。
     * {@code SELECT $sequence_name$.NEXTVAL FROM DUAL}
     */
    @Test
    public void buildSequenceGeneratorSql() throws Exception {
        assertThat("SQL文が生成されること",
                sut.buildSequenceGeneratorSql("sequence"),
                is("SELECT sequence.NEXTVAL FROM DUAL"));
    }

    /**
     * {@link OracleDialect#getResultSetConvertor()}のテスト。
     * <p/>
     * Convertorが狙った通りの型で値を取得できることの確認を行う。
     */
    @Test
    public void getResultSetConvertor() throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2015, 2, 9, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        Date date = calendar.getTime();
        Timestamp timestamp = Timestamp.valueOf("2015-03-16 01:02:03.123456");
        VariousDbTestHelper.setUpTable(
                new DialectEntity(1L, "12345", 100, 1234554321L, date, new BigDecimal("12345.54321"), timestamp,
                        new byte[] {0x00, 0x50, (byte) 0xFF}));
        connection = VariousDbTestHelper.getNativeConnection();
        final PreparedStatement statement = connection.prepareStatement(
                "SELECT ENTITY_ID, STR, NUM, BIG_INT, DECIMAL_COL, DATE_COL, TIMESTAMP_COL, BINARY_COL FROM DIALECT WHERE ENTITY_ID = ?");
        statement.setLong(1, 1L);
        final ResultSet rs = statement.executeQuery();

        assertThat("1レコードは取得できているはず", rs.next(), is(true));

        final ResultSetConvertor convertor = sut.getResultSetConvertor();

        final ResultSetMetaData meta = rs.getMetaData();
        final int columnCount = meta.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            assertThat("変換するか否かの結果は全てtrue", convertor.isConvertible(meta, i), is(true));
        }

        assertThat("文字列はStringで取得できる", (String) convertor.convert(rs, meta, 2), is("12345"));
        assertThat("数値型はIntegerで取得できる", (Integer) convertor.convert(rs, meta, 3), is(Integer.valueOf("100")));
        assertThat("10桁以上の数値型はLongで取得できる", (Long) convertor.convert(rs, meta, 4), is(Long.valueOf("1234554321")));
        assertThat("小数部ありはBigDecimalで取得できる", (BigDecimal) convertor.convert(rs, meta, 5), is(new BigDecimal(
                "12345.54321")));
        assertThat("DATE型はTimestampで取得できる", (Timestamp) convertor.convert(rs, meta, 6), is(new Timestamp(
                date.getTime())));
        assertThat("TIMESTAMP型はTimestampで取得できる", (Timestamp) convertor.convert(rs, meta, 7), is(timestamp));

        // binaryはblobで取得される
        final Blob blob = (Blob) convertor.convert(rs, meta, 8);
        assertThat("長さは3", blob.length(), is(3L));
        assertThat("値が取得出来ていること", blob.getBytes(1, 3), is(new byte[] {0x00, 0x50, (byte) 0xFF}));
    }

    /**
     * {@link OracleDialect#getResultSetConvertor()}のテスト。
     * データベースから返却される値がnullの場合でも問題ないこと。
     */
    @Test
    public void getResultSetConvertor_DB_null() throws Exception {
        VariousDbTestHelper.setUpTable(
                new DialectEntity(2L, null, null, null, null, null, null, null)
        );
        connection = VariousDbTestHelper.getNativeConnection();
        final PreparedStatement statement = connection.prepareStatement(
                "SELECT ENTITY_ID, STR, NUM, BIG_INT, DECIMAL_COL, DATE_COL, TIMESTAMP_COL, BINARY_COL FROM DIALECT WHERE ENTITY_ID = ?");
        statement.setLong(1, 2L);
        final ResultSet rs = statement.executeQuery();

        assertThat("1レコードは取得できているはず", rs.next(), is(true));

        final ResultSetConvertor convertor = sut.getResultSetConvertor();

        final ResultSetMetaData meta = rs.getMetaData();
        final int columnCount = meta.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            assertThat("変換するか否かの結果は全てtrue", convertor.isConvertible(meta, i), is(true));
        }

        assertThat(convertor.convert(rs, meta, 2), is(nullValue()));
        assertThat(convertor.convert(rs, meta, 3), is(nullValue()));
        assertThat(convertor.convert(rs, meta, 4), is(nullValue()));
        assertThat(convertor.convert(rs, meta, 5), is(nullValue()));
        assertThat(convertor.convert(rs, meta, 6), is(nullValue()));
        assertThat(convertor.convert(rs, meta, 7), is(nullValue()));
        assertThat(convertor.convert(rs, meta, 8), is(nullValue()));
    }

    /**
     * {@link OracleDialect#convertPaginationSql(String, SelectOption)}のテスト。
     */
    @Test
    public void convertPaginationSql() throws Exception {

        assertThat("rownumでの取得件数のフィルターが行われていること(offsetとlimit両方指定)",
                sut.convertPaginationSql("SELECT * FROM DUAL", new SelectOption(5, 10)),
                is(
                        "SELECT SUB2.* FROM (SELECT SUB1.*, ROWNUM ROWNUM_ FROM (SELECT * FROM DUAL) SUB1 ) SUB2 WHERE SUB2.ROWNUM_ > 4 AND SUB2.ROWNUM_ <= 14"
                ));

        assertThat("rownumでの取得件数のフィルタが行われていること(offsetのみ指定)",
                sut.convertPaginationSql(
                        "SELECT HOGE, FUGA FROM HOGE_TABLE INNER JOIN FUGA_TABLE ON HOGE_TABLE.ID = FUGA_TABLE.HOGE_ID ORDER BY HOGE_TABLE.ID, HOGE_TABLE.NAME",
                        new SelectOption(50, 0)),
                is("SELECT SUB2.* FROM (SELECT SUB1.*, ROWNUM ROWNUM_"
                        + " FROM (SELECT HOGE, FUGA FROM HOGE_TABLE INNER JOIN FUGA_TABLE ON HOGE_TABLE.ID = FUGA_TABLE.HOGE_ID ORDER BY HOGE_TABLE.ID, HOGE_TABLE.NAME) SUB1 ) SUB2"
                        + " WHERE SUB2.ROWNUM_ > 49"));

        assertThat("rownumでの取得件数のフィルタが行われていること(limitのみ指定)",
                sut.convertPaginationSql(
                        "SELECT HOGE, FUGA FROM HOGE_TABLE INNER JOIN FUGA_TABLE ON HOGE_TABLE.ID = FUGA_TABLE.HOGE_ID ORDER BY HOGE_TABLE.ID, HOGE_TABLE.NAME",
                        new SelectOption(0, 25)
                ),
                is("SELECT SUB2.* FROM (SELECT SUB1.*, ROWNUM ROWNUM_"
                        + " FROM (SELECT HOGE, FUGA FROM HOGE_TABLE INNER JOIN FUGA_TABLE ON HOGE_TABLE.ID = FUGA_TABLE.HOGE_ID ORDER BY HOGE_TABLE.ID, HOGE_TABLE.NAME) SUB1 ) SUB2"
                        + " WHERE SUB2.ROWNUM_ <= 25"));
    }

    /**
     * {@link OracleDialect#convertPaginationSql(String, SelectOption)}で生成したSQL文が実行できること。
     *
     * offsetのみを指定した場合
     */
    @Test
    public void convertPaginationSql_executeOffsetOnly() throws Exception {
        VariousDbTestHelper.delete(DialectEntity.class);
        for (int i = 0; i < 100; i++) {
            VariousDbTestHelper.insert(new DialectEntity((long) i + 1, "name_" + i));
        }
        connection = VariousDbTestHelper.getNativeConnection();

        String sql = "select entity_id, str from dialect where str like ? order by entity_id";
        final PreparedStatement statement = connection.prepareStatement(
                sut.convertPaginationSql(sql, new SelectOption(50, 0)));
        statement.setString(1, "name%");

        final ResultSet rs = statement.executeQuery();
        int index = 50;
        while (rs.next()) {
            assertThat(rs.getLong(1), is((long) index));
            index++;
        }
    }

    /**
     * {@link OracleDialect#convertPaginationSql(String, SelectOption)}で生成したSQL文が実行できること。
     *
     * limitのみを指定した場合
     */
    @Test
    public void convertPaginationSql_executeLimitOnly() throws Exception {
        VariousDbTestHelper.delete(DialectEntity.class);
        for (int i = 0; i < 100; i++) {
            VariousDbTestHelper.insert(new DialectEntity((long) i + 1, "name_" + i));
        }
        connection = VariousDbTestHelper.getNativeConnection();

        String sql = "select entity_id, str from dialect where str like ? order by entity_id";
        final PreparedStatement statement = connection.prepareStatement(
                sut.convertPaginationSql(sql, new SelectOption(0, 25)));
        statement.setString(1, "name%");

        final ResultSet rs = statement.executeQuery();
        int index = 0;
        while (rs.next()) {
            index++;
            assertThat(rs.getLong(1), is((long) index));
        }
        assertThat("取得件数は25であること", index, is(25));
    }

    /**
     * {@link OracleDialect#convertPaginationSql(String, SelectOption)}で生成したSQL文が実行できること。
     *
     * offsetとlimitの両方を指定
     */
    @Test
    public void convertPaginationSql_executeOffsetAndLimit() throws Exception {
        VariousDbTestHelper.delete(DialectEntity.class);
        for (int i = 0; i < 100; i++) {
            VariousDbTestHelper.insert(new DialectEntity((long) i + 1, "name_" + i));
        }
        connection = VariousDbTestHelper.getNativeConnection();

        String sql = "select entity_id, str from dialect where str like ? order by entity_id";
        final PreparedStatement statement = connection.prepareStatement(
                sut.convertPaginationSql(sql, new SelectOption(31, 15)));
        statement.setString(1, "name%");

        final ResultSet rs = statement.executeQuery();
        int index = 30;
        while (rs.next()) {
            index++;
            assertThat(rs.getLong(1), is((long) index));
        }
        assertThat("最後に取得されたレコードの番号は45であること", index, is(45));
    }

    /**
     * {@link OracleDialect#convertCountSql(String)}のテスト。
     */
    @Test
    public void convertCountSql() throws Exception {
        final String actual = sut.convertCountSql("SELECT * FROM DUAL");
        assertThat("変換されていること", actual, is("SELECT COUNT(*) COUNT_ FROM (SELECT * FROM DUAL) SUB_"));
    }

    /**
     * {@link OracleDialect#convertCountSql(String)}で変換したSQL文が実行可能であることを確認する。
     */
    @Test
    public void convertCountSql_execute() throws Exception {
        VariousDbTestHelper.delete(DialectEntity.class);
        for (int i = 0; i < 100; i++) {
            VariousDbTestHelper.insert(new DialectEntity((long) i + 1, "name_" + i));
        }
        connection = VariousDbTestHelper.getNativeConnection();
        String sql = "select entity_id, str from dialect where str like ? order by entity_id";
        final PreparedStatement statement = connection.prepareStatement(sut.convertCountSql(sql));
        statement.setString(1, "name_3%");
        final ResultSet rs = statement.executeQuery();

        assertThat(rs.next(), is(true));
        assertThat(rs.getInt(1), is(11));       // name_3とname_3x
    }

    /**
     * {@link OracleDialect#getPingSql()}のテスト。
     */
    @Test
    public void getPingSql() throws Exception {
        final String pingSql = sut.getPingSql();
        assertThat(pingSql, is("select 1 from dual"));

        // SQL文が実行可能であること
        connection = VariousDbTestHelper.getNativeConnection();
        final PreparedStatement statement = connection.prepareStatement(pingSql);
        final ResultSet rs = statement.executeQuery();
        assertThat(rs, is(notNullValue()));
        rs.close();
    }

    @Test
    public void convertToDatabaseFromSqlType() throws Exception {
        // 文字データ型として出力
        Object converted = sut.convertToDatabase("", Types.VARCHAR);
        assertThat("empty string->null", converted, is(nullValue()));

        String stringTarget = "test";
        assertThat("string->CHAR", (String) sut.convertToDatabase(stringTarget, Types.CHAR), is(stringTarget));
        assertThat("string->VARCHAR", (String) sut.convertToDatabase(stringTarget, Types.VARCHAR), is(stringTarget));
        assertThat("string->LONG", (String) sut.convertToDatabase(stringTarget, Types.LONGVARCHAR), is(stringTarget));

        // 数値データ型として出力
        BigDecimal bigDecimalTarget = BigDecimal.ONE;
        assertThat("BigDecimal->NUMERIC", (BigDecimal) sut.convertToDatabase(bigDecimalTarget, Types.NUMERIC), is(BigDecimal.ONE));
        assertThat("BigDecimal->DECIMAL", (BigDecimal) sut.convertToDatabase(bigDecimalTarget, Types.DECIMAL), is(BigDecimal.ONE));

        boolean booleanTarget = true;
        assertThat("boolean->BIT", (Boolean) sut.convertToDatabase(booleanTarget, Types.BIT), is(true));

        int integerTarget = 1;
        assertThat("int->INTEGER", (Integer) sut.convertToDatabase(integerTarget, Types.INTEGER), is(1));

        long longTarget = 1L;
        assertThat("long->BIGINT", (Long) sut.convertToDatabase(longTarget, Types.BIGINT), is(longTarget));

        // RAWデータ型として出力
        byte[] bytesTarget = new byte[] {0x30, 0x39};
        assertThat("byte->BINARY", (byte[]) sut.convertToDatabase(bytesTarget, Types.BINARY), is(bytesTarget));
        assertThat("byte->VARBINARY", (byte[]) sut.convertToDatabase(bytesTarget, Types.VARBINARY), is(bytesTarget));
        assertThat("byte->LONGVARBINARY", (byte[]) sut.convertToDatabase(bytesTarget, Types.LONGVARBINARY), is(bytesTarget));

        // DATEデータ型として出力
        Date dateTarget = new Date(System.currentTimeMillis());
        assertThat("Date->DATE", (java.sql.Date) sut.convertToDatabase(dateTarget, Types.DATE),
                is(new java.sql.Date(DbUtil.trimTime(new Date(System.currentTimeMillis())).getTimeInMillis())));

        String timestampTargetString = "2016-12-13 12:34:56.789012";
        assertThat("Timestamp->TIMESTAMP",
                (java.sql.Timestamp) sut.convertToDatabase(Timestamp.valueOf(timestampTargetString), Types.TIMESTAMP),
                is(java.sql.Timestamp.valueOf(timestampTargetString)));

        // LOBデータ型として出力
        String clobTarget = "clob";
        assertThat("string->CLOB", (String) sut.convertToDatabase(clobTarget, Types.CLOB), is(clobTarget));
        byte[] blobTarget = new byte[] {0x01, 0x02};
        assertThat("string->BLOB", (byte[]) sut.convertToDatabase(blobTarget, Types.BLOB), is(new byte[] {0x01, 0x02}));
    }

    @Test
    public void convertToDatabaseFromSqlType_unsupportedSqlType() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("unsupported sqlType: 2006");
        // 未定義のSQL型を指定
        Object converted = sut.convertToDatabase("123", Types.REF);
        assertThat("empty string->null", converted, is(nullValue()));
    }

    @Test
    public void convertToDatabase() throws Exception {
        assertThat("empty string->null", sut.convertToDatabase("", String.class), is(nullValue()));
        assertThat("string", sut.convertToDatabase("あいうえお", String.class), is("あいうえお"));
        assertThat("Short", sut.convertToDatabase(Short.valueOf("100"), BigDecimal.class), is(BigDecimal.valueOf(100)));
        assertThat("Integer", sut.convertToDatabase(101, String.class), is("101"));
        assertThat("Long", sut.convertToDatabase(102L, Long.class), is(102L));
        assertThat("BigDecimal", sut.convertToDatabase(BigDecimal.ONE, Integer.class), is(1));
        assertThat("java.sql.Date", sut.convertToDatabase(java.sql.Date.valueOf("2016-12-02"), Timestamp.class),
                is(Timestamp.valueOf("2016-12-02 00:00:00.000000")));
        assertThat("java.util.Date", sut.convertToDatabase(new java.util.Date(System.currentTimeMillis()), java.sql.Date.class),
                is(new java.sql.Date(DbUtil.trimTime(new Date(System.currentTimeMillis())).getTimeInMillis())));
        assertThat("Timestamp", sut.convertToDatabase(Timestamp.valueOf("2016-12-02 11:22:33.123321"), java.sql.Date.class),
                is(java.sql.Date.valueOf("2016-12-02")));
        assertThat("byte[]", sut.convertToDatabase(new byte[] {0x30, 0x39}, byte[].class), is(new byte[] {0x30, 0x39}));
        assertThat("Boolean", sut.convertToDatabase(true, Boolean.class), is(Boolean.TRUE));
    }

    /**
     * DBに出力するとき、
     * コンバータが設定されていないと例外を送出する。
     */
    @Test
    public void testConvertToDatabaseNotFound() {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("This dialect does not support [BigInteger] type.");

        sut.convertToDatabase(BigInteger.valueOf(100), Integer.class);
    }

    @Test
    public void convertFromDatabase() throws Exception {
        assertThat("String", sut.convertFromDatabase("あああ", String.class), is("あああ"));
        assertThat("empty string", sut.convertFromDatabase("", String.class), is(isEmptyString()));
        assertThat("Short", sut.convertFromDatabase(100, Short.class), is(Short.valueOf("100")));
        assertThat("short", sut.convertFromDatabase(99, short.class), is((short) 99));
        assertThat("null -> short", sut.convertFromDatabase(null, short.class), is(((short) 0)));
        assertThat("Integer", sut.convertFromDatabase(101L, Integer.class), is(101));
        assertThat("int", sut.convertFromDatabase("102", int.class), is(102));
        assertThat("null -> int", sut.convertFromDatabase(null, int.class), is(0));
        assertThat("Long", sut.convertFromDatabase("103", Long.class), is(103L));
        assertThat("long", sut.convertFromDatabase(104, long.class), is(104L));
        assertThat("null -> long", sut.convertFromDatabase(null, long.class), is(0L));
        assertThat("BigDecimal", sut.convertFromDatabase("11.1", BigDecimal.class), is(new BigDecimal("11.1")));
        assertThat("java.sql.Date", sut.convertFromDatabase(java.sql.Date.valueOf("2016-12-03"), java.sql.Date.class),
                is(java.sql.Date.valueOf("2016-12-03")));
        assertThat("java.util.Date", sut.convertFromDatabase(java.sql.Date.valueOf("2016-12-04"), Date.class), is(new Date(
                java.sql.Date.valueOf("2016-12-04")
                             .getTime())));
        assertThat("Timestamp", sut.convertFromDatabase("2016-12-03 01:02:03.123321", Timestamp.class),
                is(Timestamp.valueOf("2016-12-03 01:02:03.123321")));
        assertThat("byte[]", sut.convertFromDatabase(new byte[] {0x00, 0x30}, byte[].class), is(new byte[] {0x00, 0x30}));
        assertThat("Boolean", sut.convertFromDatabase("on", Boolean.class), is(Boolean.TRUE));
        assertThat("boolean", sut.convertFromDatabase("off", boolean.class), is(false));
        assertThat("null -> boolean", sut.convertFromDatabase(null, boolean.class), is(false));
    }

    /**
     * DBから入力を変換するとき、
     * コンバータが設定されていないと例外を送出する。
     */
    @Test
    public void testConvertFromDatabaseNotFound() {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("This dialect does not support [BigInteger] type.");

        sut.convertFromDatabase("100", BigInteger.class);
    }
}

