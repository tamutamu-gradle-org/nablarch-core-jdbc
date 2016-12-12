package nablarch.core.db.dialect;

import nablarch.core.db.statement.ResultSetConvertor;
import nablarch.core.db.util.DbUtil;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.Date;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * {@link DefaultDialect}のテストクラス。
 * 方言は無効化されている。
 */
@RunWith(DatabaseTestRunner.class)
public class DefaultDialectTest {

    private DefaultDialect sut = new DefaultDialect();

    @Rule
    public ExpectedException exception = ExpectedException.none();

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
     * IDENTITY(オートインクリメントカラム)が使用できない。
     */
    @Test
    public void testSupportsIdentity() {
        assertThat(sut.supportsIdentity(), is(false));
    }

    /**
     * SEQUENCEが使用できない。
     */
    @Test
    public void testSupportsSequence() {
        assertThat(sut.supportsSequence(), is(false));
    }

    /**
     * SQL文でのオフセット指定が使用できない。
     */
    @Test
    public void testSupportsOffset() {
        assertThat(sut.supportsOffset(), is(false));
    }

    /**
     * SQL例外がトランザクションタイムアウトと判断すべき例外でない。
     */
    @Test
    public void testIsTransactionTimeoutError() {
        assertThat(sut.isTransactionTimeoutError(new SQLException()), is(false));
    }

    /**
     * SQL例外が一意制約違反による例外でない。
     */
    @Test
    public void testIsDuplicateException() {
        assertThat(sut.isDuplicateException(new SQLException()), is(false));
    }

    /**
     * {@link java.sql.ResultSet}から値を取得するための変換クラスのデフォルトを確認する。
     * メタ情報を使わず{@link java.sql.ResultSet}からカラム番号で取得する。
     */
    @Test
    public void testGetResultSetConvertor() throws Exception {
        Date date = new Date();
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

        ResultSetConvertor resultSetConvertor = sut.getResultSetConvertor();
        assertThat(resultSetConvertor.isConvertible(null, 0), is(true));
        assertThat((String)resultSetConvertor.convert(rs, null, 2), is("12345"));
    }

    /**
     * シーケンス採番はサポートしない。
     */
    @Test
    public void testBuildSequenceGeneratorSql() {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("sequence generator is unsupported.");

        sut.buildSequenceGeneratorSql("sequence name");
    }

    /**
     * ページング用のSQL文は変換せず、そのまま返す。
     */
    @Test
    public void testConvertPaginationSql() {
        assertThat(sut.convertPaginationSql("sql", null), is("sql"));
    }

    /**
     * レコード数取得用のSQL文に変換する。
     */
    @Test
    public void testConvertCountSql() {
        assertThat(sut.convertCountSql("sql"), is("SELECT COUNT(*) COUNT_ FROM (sql) SUB_"));
    }

    /**
     * ping用のSQL文はサポートしない。
     */
    @Test
    public void testGetPingSql() {
        exception.expect(UnsupportedOperationException.class);
        exception.expectMessage("unsupported getPingSql.");

        sut.getPingSql();
    }

    /**
     * DBに出力する値へ変換する。
     * デフォルトで設定されてる型すべて。
     */
    @Test
    public void testConvertToDatabase() {
        assertThat("String", sut.convertToDatabase("string", String.class), is("string"));
        assertThat("Short", sut.convertToDatabase(Short.valueOf("100"), BigDecimal.class), is(BigDecimal.valueOf(100)));
        assertThat("Integer", sut.convertToDatabase(Integer.valueOf(1000), Long.class), is(Long.valueOf(1000)));
        assertThat("Long", sut.convertToDatabase(Long.valueOf("10000"), BigDecimal.class), is(BigDecimal.valueOf(10000)));
        assertThat("BigDecimal", sut.convertToDatabase(BigDecimal.valueOf(100000), String.class), is("100000"));
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
     * DBに出力するとき、nullはnullとして返す。
     */
    @Test
    public void testConvertToDatabaseNull() {
        assertThat("null value", sut.convertToDatabase(null, String.class), is(nullValue()));
    }

    /**
     * DBに出力するとき、
     * コンバータが設定されていないと例外を送出する。
     */
    @Test
    public void testConvertToDatabaseNotFound() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("This dialect does not support [BigInteger] type.");

        sut.convertToDatabase(BigInteger.valueOf(100), Integer.class);
    }

    /**
     * DBから入力を変換する。
     * デフォルトで設定されている型すべて。
     */
    @Test
    public void testConvertFromDatabase() {
        assertThat("String", sut.convertFromDatabase("あああ", String.class), is("あああ"));
        assertThat("Short", sut.convertFromDatabase(100, Short.class), is(Short.valueOf("100")));
        assertThat("short", sut.convertFromDatabase(99, short.class), is((short) 99));
        assertThat("Integer", sut.convertFromDatabase(101L, Integer.class), is(101));
        assertThat("int", sut.convertFromDatabase("102", int.class), is(102));
        assertThat("Long", sut.convertFromDatabase("103", Long.class), is(103L));
        assertThat("long", sut.convertFromDatabase(104, long.class), is(104L));
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
    }

    /**
     * DBから入力を変換するとき、
     * コンバータが設定されていないと例外を送出する。
     */
    @Test
    public void testConvertFromDatabaseNotFound() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("This dialect does not support [BigInteger] type.");

        sut.convertFromDatabase("100", BigInteger.class);
    }
}
