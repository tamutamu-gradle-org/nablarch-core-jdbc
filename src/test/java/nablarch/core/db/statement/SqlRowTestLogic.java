package nablarch.core.db.statement;

import static mockit.Deencapsulation.invoke;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;

import nablarch.core.db.DbAccessException;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.transaction.TransactionContext;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import mockit.Expectations;
import mockit.Mocked;

/**
 * {@link SqlRow}およびそのサブクラスをテストするためのテストロジック。
 * サブクラスにて、{@link #createConnectionFactory}を実装し、
 * 生成するファクトリを切り替えることで、
 * 様々な組み合わせで、本クラスで用意されたテストメソッドを実行することができる。
 * これにより{@link SqlRow}サブクラスの動作がスーパクラスと同等であることが
 * 確認できる。
 *
 * @author T.Kawasaki
 */
public abstract class SqlRowTestLogic {

    /** テストで使用する日付 */
    private static Date testDate;

    /** テストで使用する時間 */
    private static Time testTime;

    /** テストで使用する日時 */
    private static Timestamp testTimestamp = Timestamp.valueOf("2015-03-15 11:12:13.007");

    /** java.sql.Dateの文字列フォーマット */
    private static DateFormat sqlDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    /** java.sql.Timeの文字列フォーマット */
    private static DateFormat sqlTimeFormat = new SimpleDateFormat("HH:mm:ss");

    /**
     * テスト対象となる{@link ConnectionFactory}を生成する。
     *
     * @return テスト対象となるファクトリ
     */
    protected abstract ConnectionFactory createConnectionFactory();

    /** テスト対象となるコネクション */
    protected TransactionManagerConnection connection = createConnectionFactory().getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);

    @BeforeClass
    public static void setupDatabase() {
        VariousDbTestHelper.createTable(SqlRowEntity.class);

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        testDate = calendar.getTime();

        calendar.set(Calendar.HOUR, 1);
        calendar.set(Calendar.MINUTE, 2);
        calendar.set(Calendar.SECOND, 3);
        testTime = new Time(calendar.getTimeInMillis());
    }

    /** 終了処理 */
    @After
    public void after() {
        connection.terminate();
    }

    /**
     * {@link SqlRow#getString(String)}のテスト。
     */
    @Test
    public void getString() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L));

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        SqlRow sut = rs.get(0);
        assertThat("文字列データが取得できること", sut.getString("charCol"), is("a"));
        assertThat("文字列データが取得できること", sut.getString("varcharCol"), is("あいうえおかきくけこ"));
        assertThat("数値(Short)が文字列で取得できること", sut.getString("shortCol"), is("9999"));
        assertThat("数値(Integer)が文字列で取得できること", sut.getString("integerCol"), is("999999999"));
        assertThat("数値(Long)が文字列で取得できること", sut.getString("longCol"), is("999999999999999999"));
        assertThat("数値(BigDecimal)が文字列で取得できること", sut.getString("bigDecimalCol"), is("1234512345.12345"));
        assertThat("日付が文字列で取得できること", sut.getString("dateCol"), containsString(sqlDateFormat.format(testDate)));
        assertThat("時間が文字列で取得できること", sut.getString("timeCol"), containsString(sqlTimeFormat.format(testTime)));
        assertThat("日時が文字列で取得できること", sut.getString("timestampCol"), is(testTimestamp.toString()));
        assertThat("指数表現とならないこと", sut.getString("bigDecimalCol2"),is("0.0000000001"));
    }

    /**
     * {@link SqlRow#getString(String)}でnullが取得されるテスト。
     */
    @Test
    public void getString_getNull() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createSetNullValue(1L));

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        assertThat("nullが取得されること", sut.getString("varcharCol"), is(nullValue()));
        assertThat("nullが取得されること", sut.getString("integerCol"), is(nullValue()));

    }

    /**
     * {@link SqlRow#getString(String)}で存在しないカラムを指定した場合エラーとなること。
     */
    @Test(expected = IllegalArgumentException.class)
    public void getString_columnNotFound() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        sut.getString("notFound");
    }

    /**
     * {@link SqlRow#getInteger(String)}のテスト。
     */
    @Test
    public void getInteger() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L),
                SqlRowEntity.createSetIntegerValueInstance(2L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST WHERE SQLROW_ID = 2");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        assertThat("文字列(char)がIntegerで取得できる", sut.getInteger("charCol"), is(0));
        assertThat("文字列(varchar)がIntegerで取得できる", sut.getInteger("varcharCol"), is(Integer.MAX_VALUE));
        assertThat("数値(Short)がIntegerで取得できる", sut.getInteger("shortCol"), is(1));
        assertThat("数値(Integer)がIntegerで取得できる", sut.getInteger("integerCol"), is(2));
        assertThat("数値(Long)がIntegerで取得できる", sut.getInteger("longCol"), is(3));
    }

    /**
     * {@link SqlRow#getInteger(String)}のテスト。
     * nullの取得もできること
     */
    @Test
    public void getInteger_getNull() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createSetNullValue(999L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        assertThat("nullが取得されること", sut.getInteger("integerCol"), is(nullValue()));
        assertThat("nullが取得されること", sut.getInteger("varcharCol"), is(nullValue()));
        assertThat("nullが取得されること", sut.getInteger("timeCol"), is(nullValue()));
    }

    /**
     * {@link SqlRow#getInteger(String)}のテストで、
     * 対象の値がIntegerの範囲外の場合エラーとなること。
     */
    @Test(expected = IllegalStateException.class)
    public void getInteger_overflow() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        sut.getInteger("longCol");              // Integerの範囲外の値
    }

    /**
     * {@link SqlRow#getInteger(String)}のテストで、
     * 対象の値が数値ではない場合はエラーとなること。
     */
    @Test(expected = IllegalStateException.class)
    public void getInteger_notNumeric() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        sut.getInteger("varcharCol");              // 非数値の値
    }

    /**
     * {@link SqlRow#getInteger(String)}のテストで、
     * 対象のカラムが存在しない場合はエラーとなること。
     */
    @Test(expected = IllegalArgumentException.class)
    public void getInteger_columnNotFound() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        sut.getInteger("notFound");
    }

    /**
     * {@link SqlRow#getLong(String)}のテスト。
     */
    @Test
    public void getLong() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createSetLongValueInstance(10L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        assertThat("文字列(char)がLongで取得できる", sut.getLong("charCol"), is(1L));
        assertThat("文字列(varchar)がLongで取得できる", sut.getLong("varcharCol"), is(10L));
        assertThat("数値(Short)がLongで取得できる", sut.getLong("shortCol"), is(100L));
        assertThat("数値(Long)がLongで取得できる", sut.getLong("integerCol"), is(1000L));
        assertThat("数値(Long)がLongで取得できる", sut.getLong("longCol"), is(10000L));
    }

    /**
     * {@link SqlRow#getLong(String)}のテストでnullが取得されるテスト。
     */
    @Test
    public void getLong_getNull() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createSetNullValue(10L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        assertThat("nullが取得されること", sut.getLong("longCol"), is(nullValue()));
        assertThat("nullが取得されること", sut.getLong("charCol"), is(nullValue()));
        assertThat("nullが取得されること", sut.getLong("timestampCol"), is(nullValue()));
    }

    /**
     * {@link SqlRow#getLong(String)}のテストで、
     * 対象の値がLongの範囲外の場合エラーとなること。
     */
    @Test(expected = IllegalStateException.class)
    public void getLong_overflow() throws Exception {
        // ------------------------------------------------ setup
        final SqlRowEntity entity = SqlRowEntity.createDefaultValueInstance(1L);
        entity.varcharCol = "99999999999999999999";
        VariousDbTestHelper.setUpTable(entity);

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        sut.getLong("varcharCol");              // Longの範囲外の値
    }

    /**
     * {@link SqlRow#getLong(String)}のテストで、
     * 対象の値が数値ではない場合はエラーとなること。
     */
    @Test(expected = IllegalStateException.class)
    public void getLong_notNumeric() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        sut.getLong("varcharCol");              // 非数値の値
    }

    /**
     * {@link SqlRow#getLong(String)}のテストで、
     * 対象のカラムが存在しない場合はエラーとなること。
     */
    @Test(expected = IllegalArgumentException.class)
    public void getLong_columnNotFound() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        sut.getLong("notFound");
    }

    /**
     * {@link SqlRow#getBigDecimal(String)}のテスト。
     */
    @Test
    public void getBigDecimal() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createSetBigDecimalValueInstance(100L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        assertThat(sut.getBigDecimal("charCol"), is(new BigDecimal("9")));
        assertThat(sut.getBigDecimal("varcharCol"), is(new BigDecimal("99999.9999999999")));
        assertThat(sut.getBigDecimal("shortCol"), is(new BigDecimal("9999")));
        assertThat(sut.getBigDecimal("integerCol"), is(new BigDecimal("999999999")));
        assertThat(sut.getBigDecimal("longCol"), is(new BigDecimal("999999999999999999")));
        assertThat(sut.getBigDecimal("bigDecimalCol"), is(new BigDecimal("1234512345.12345")));
    }

    /**
     * {@link SqlRow#getBigDecimal(String)}のテストでnullが取得されるケース。
     */
    @Test
    public void getBigDecimal_getNull() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createSetNullValue(999L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        assertThat("nullが取得されること", sut.getBigDecimal("integerCol"), is(nullValue()));
        assertThat("nullが取得されること", sut.getBigDecimal("varcharCol"), is(nullValue()));
        assertThat("nullが取得されること", sut.getBigDecimal("bigDecimalCol"), is(nullValue()));
    }

    /**
     * {@link SqlRow#getBigDecimal(String)}のテストで、
     * 対象の値が数値ではない場合はエラーとなること。
     */
    @Test(expected = IllegalStateException.class)
    public void getBigDecimal_notNumeric() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        sut.getBigDecimal("varcharCol");              // 非数値の値
    }

    /**
     * {@link SqlRow#getBigDecimal(String)}のテストで、
     * 対象のカラムが存在しない場合はエラーとなること。
     *
     */
    @Test(expected = IllegalArgumentException.class)
    public void getBigDecimal_columnNotFound() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        sut.getBigDecimal("notFound");
    }

    /**
     * {@link SqlRow#getDate(String)}のテスト。
     *
     * ※Oracle11g以降は、日付型は全てTimestampとして扱われるので、本テストは実施しない。
     * (OracleのDateは時間情報を保持しているので、Dateで取得するとその情報が欠落するため。）
     */
    @Test
    @TargetDb(exclude = TargetDb.Db.ORACLE)
    public void getDate() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        assertThat(sut.getDate("dateCol"), is(testDate));
    }

    /**
     * {@link SqlRow#getDate(String)}のテスト。
     *
     * nullの取得ができること。
     */
    @Test
    @TargetDb(exclude = TargetDb.Db.ORACLE)
    public void getDate_getNull() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createSetNullValue(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        assertThat(sut.getDate("dateCol"), is(nullValue()));
    }

    /**
     * {@link SqlRow#getDate(String)}で日付以外のカラムを指定した場合、例外が送出されること。
     */
    @Test(expected = IllegalStateException.class)
    public void getDate_notDateColumn() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        sut.getDate("charCol");
    }

    /**
     * {@link SqlRow#getDate(String)}でカラムが存在しない場合は例外が送出されること。
     */
    @Test(expected = IllegalArgumentException.class)
    public void getDate_columnNotFound() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        sut.getDate("notFound");
    }

    /**
     * {@link SqlRow#getTimestamp(String)}のテスト。
     */
    @Test
    public void getTimestamp() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        assertThat(sut.getTimestamp("timestampCol"), is(testTimestamp));
    }

    /**
     * {@link SqlRow#getTimestamp(String)}でnullが取得できること。
     */
    @Test
    public void getTimestamp_getNull() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createSetNullValue(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        assertThat(sut.getTimestamp("timestampCol"), is(nullValue()));
    }

    /**
     * {@link SqlRow#getTimestamp(String)}でタイムスタンプ型以外のカラムを取得しようとした場合、
     * 例外が送出されること。
     */
    @Test(expected = IllegalStateException.class)
    public void getTimestamp_notTimestamp() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        sut.getTimestamp("charCol");
    }

    /**
     * {@link SqlRow#getTimestamp(String)}で存在しないカラムを指定した場合、
     * 例外が送出されること。
     */
    @Test(expected = IllegalArgumentException.class)
    public void getTimestamp_columnNotFound() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        sut.getTimestamp("notFound");
    }

    /**
     * {@link SqlRow#getBytes(String)}のテスト。
     */
    @Test
    public void getBytes() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        assertThat(sut.getBytes("binaryCol"), is(new byte[] {0x00, 0x01, 0x30, 0x31}));
    }

    /**
     * {@link SqlRow#getBytes(String)}でnullの取得ができること。
     */
    @Test
    public void getBytes_getNull() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createSetNullValue(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        assertThat(sut.getBytes("binaryCol"), is(nullValue()));
    }

    /**
     * {@link SqlRow#getBytes(String)}で指定されたカラムがバイナリ型ではない場合、
     * 例外が送出されること。
     */
    @Test(expected = IllegalStateException.class)
    public void getBytes_notBinaryType() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        sut.getBytes("longCol");
    }

    /**
     * {@link SqlRow#getBytes(String)}で指定されたカラムが存在しない場合
     * 例外が送出されること。
     */
    @Test(expected = IllegalArgumentException.class)
    public void getBytes_columnNotFound() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        sut.getBytes("notFound");
    }

    /**
     * {@link SqlRow#getBytes(String)}でBLOBアクセス時にSQLExceptionが発生するケース。
     * DbAccessExceptionが送出されること。
     */
    @Test(expected = DbAccessException.class)
    public void getBytes_blogSQLException(@Mocked final Blob mockBlob) throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        new Expectations(sut) {{
            invoke(sut, "getObject", "binaryCol");
            result = mockBlob;
            mockBlob.length();
            result = new SQLException("blob access error");
        }};
        sut.getBytes("binaryCol");
    }

    /**
     * {@link SqlRow#getBoolean(String)}のテスト。
     *
     * DBの値が文字列の場合、"1" or "true" or "on"の場合、trueがかえされること。（大文字、小文字は区別しない)
     */
    @Test
    public void getBoolean_String() throws Exception {
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");

        VariousDbTestHelper.setUpTable(SqlRowEntity.createFromStringCol(1L, "1", null));
        assertThat("1はtrue", statement.retrieve()
                .get(0)
                .getBoolean("charCol"), is(true));

        VariousDbTestHelper.setUpTable(SqlRowEntity.createFromStringCol(1L, null, "on"));
        assertThat("onはtrue", statement.retrieve()
                .get(0)
                .getBoolean("varcharCol"), is(true));

        VariousDbTestHelper.setUpTable(SqlRowEntity.createFromStringCol(1L, null, "ON"));
        assertThat("ONはtrue", statement.retrieve()
                .get(0)
                .getBoolean("varcharCol"), is(true));

        VariousDbTestHelper.setUpTable(SqlRowEntity.createFromStringCol(1L, null, "true"));
        assertThat("trueはtrue", statement.retrieve()
                .get(0)
                .getBoolean("varcharCol"), is(true));

        VariousDbTestHelper.setUpTable(SqlRowEntity.createFromStringCol(1L, null, "True"));
        assertThat("Trueはtrue", statement.retrieve()
                .get(0)
                .getBoolean("varcharCol"), is(true));

        VariousDbTestHelper.setUpTable(SqlRowEntity.createFromStringCol(1L, "0", null));
        assertThat("0はfalse", statement.retrieve()
                .get(0)
                .getBoolean("charCol"), is(false));

        VariousDbTestHelper.setUpTable(SqlRowEntity.createFromStringCol(1L, null, "off"));
        assertThat("offはfalse", statement.retrieve()
                .get(0)
                .getBoolean("varcharCol"), is(false));

        VariousDbTestHelper.setUpTable(SqlRowEntity.createFromStringCol(1L, null, "false"));
        assertThat("falseはfalse", statement.retrieve()
                .get(0)
                .getBoolean("varcharCol"), is(false));

        VariousDbTestHelper.setUpTable(SqlRowEntity.createFromStringCol(1L, null, "othre"));
        assertThat("適当な値はfalse", statement.retrieve()
                .get(0)
                .getBoolean("varcharCol"), is(false));

        VariousDbTestHelper.setUpTable(SqlRowEntity.createFromStringCol(1L, null, null));
        assertThat("nullはnull", statement.retrieve()
                .get(0)
                .getBoolean("charCol"), is(nullValue()));
    }

    /**
     * {@link SqlRow#getBoolean(String)}のテスト。
     * <p/>
     * DBの値が数値の場合、0以外の場合trueとなること
     */
    @Test
    public void getBoolean_Numeric() throws Exception {
        VariousDbTestHelper.setUpTable(SqlRowEntity.createFromNumberCol(1L, (short) 0, 1, -1L));
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlRow result = statement.retrieve()
                .get(0);

        assertThat("0はfalse", result.getBoolean("shortCol"), is(false));
        assertThat("1はtrue", result.getBoolean("integerCol"), is(true));
        assertThat("-1はtrue", result.getBoolean("longCol"), is(true));
    }

    /**
     * {@link SqlRow#getBoolean(String)}のテスト。
     * <p/>
     * DBの値がBooleanの場合
     */
    @Test
    @TargetDb(include = {TargetDb.Db.SQL_SERVER, TargetDb.Db.POSTGRE_SQL})
    public void getBoolean_Boolean() throws Exception {
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");

        VariousDbTestHelper.setUpTable(SqlRowEntity.createFromBooleanCol(1L, true));

        assertThat("true", statement.retrieve().get(0)
                .getBoolean("booleanCol"), is(true));

        VariousDbTestHelper.setUpTable(SqlRowEntity.createFromBooleanCol(1L, false));
        assertThat("false", statement.retrieve().get(0)
                .getBoolean("booleanCol"), is(false));
    }

    /**
     * {@link SqlRow#getBoolean(String)}のテスト。
     *
     * 不正なデータ型の場合エラーとなること。
     */
    @Test(expected = IllegalStateException.class)
    public void getBoolean_invalidType() throws Exception {
        VariousDbTestHelper.setUpTable(SqlRowEntity.createDefaultValueInstance(1L));

        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        statement.retrieve()
                .get(0)
                .getBoolean("binaryCol");
    }

    @Test(expected = IllegalArgumentException.class)
    public void getBoolean_columnNotFound() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        sut.getBoolean("notFound");
    }

    @Test
    public void getObject_String() throws Exception {
        VariousDbTestHelper.setUpTable(SqlRowEntity.createDefaultValueInstance(1L));

        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        SqlRow sut = statement.retrieve().get(0);
        assertThat("Stringを指定して取得できる",
                sut.getObject("varcharCol", String.class), is("あいうえおかきくけこ"));
        assertThat("Stringを指定して取得できる",
                sut.getObject("charCol", String.class), is("a"));
    }

    @Test
    public void getObject_Integer() throws Exception {
        VariousDbTestHelper.setUpTable(SqlRowEntity.createSetIntegerValueInstance(1L));

        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        SqlRow sut = statement.retrieve().get(0);
        assertThat("Integerを指定して取得できる",
                sut.getObject("varcharCol", Integer.class), is(Integer.MAX_VALUE));
        assertThat("Integerを指定して取得できる",
                sut.getObject("integerCol", Integer.class), is(2));
        assertThat("Integerを指定して取得できる",
                sut.getObject("longCol", Integer.class), is(3));
    }

    @Test(expected = IllegalArgumentException.class)
    public void getObject_columnNotFound() throws Exception {
        VariousDbTestHelper.setUpTable(SqlRowEntity.createDefaultValueInstance(1L));
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlRow sut = statement.retrieve().get(0);

        sut.getObject("notFound", String.class);
    }

    @Test(expected = IllegalStateException.class)
    public void getObject_invalidType() throws Exception {
        VariousDbTestHelper.setUpTable(SqlRowEntity.createDefaultValueInstance(1L));
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlRow sut = statement.retrieve().get(0);

        sut.getObject("longCol", BigInteger.class);
    }

    @Test(expected = DbAccessException.class)
    public void getObject_DbAccessException(@Mocked final Blob mockBlob) throws Exception {
        VariousDbTestHelper.setUpTable(SqlRowEntity.createDefaultValueInstance(1L));

        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlRow sut = statement.retrieve().get(0);

        new Expectations(sut) {{
            invoke(sut, "getObject", "binaryCol");
            result = mockBlob;
            mockBlob.length();
            result = new SQLException("blob access error");
        }};
        sut.getObject("binaryCol", byte[].class);
    }

    /**
     * {@link SqlRow#get(Object)}のテスト。
     */
    @Test
    public void get() throws Exception {

        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement("SELECT * FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);

        assertThat("キー値の大文字小文字は区別しないこと", (String) sut.get("charcol"), is("a"));
        assertThat("キー値の大文字小文字は区別しないこと", (String) sut.get("charCol"), is("a"));
        assertThat("キー値の大文字小文字は区別しないこと", (String) sut.get("CHARCOL"), is("a"));
        assertThat("キー値の大文字小文字は区別しないこと", (String) sut.get("CharCol"), is("a"));

        assertThat("アンダースコアの有無は無視されること", (String) sut.get("_char_col"), is("a"));
        assertThat("アンダースコアの有無は無視されること", (String) sut.get("_c_h_a_r_c_o_l_"), is("a"));

        assertThat("ハイフンは区別されるので、取得されなくなる", sut.get("char-col"), is(nullValue()));


        // ------------------------------------------------ put -> get
        sut.put("new_key", "value");
        assertThat("追加した値も大文字小文字区別しない", (String) sut.get("NEW_KEY"), is("value"));
        assertThat("追加した値も大文字小文字区別しない", (String) sut.get("newKey"), is("value"));
        assertThat("アンダースコアな無視される", (String) sut.get("_new_key_"), is("value"));

        // ------------------------------------------------ putAll -> get
        sut.putAll(new TreeMap<String, Object>() {{
            put("new_key_1", "value1");
            put("new_key_2", "value2");
            put("new_key_3", "value3");
        }});

        assertThat("putAllした値も大文字小文字は区別しない", (String) sut.get("NEW_KEY_1"), is("value1"));
        assertThat("putAllした値も大文字小文字は区別しない", (String) sut.get("newKey1"), is("value1"));
        assertThat("putAllした値もアンダースコアは無視される", (String) sut.get("NEW_K_EY_1"), is("value1"));
    }

    /**
     * select句に別名を設定した場合でも、正しく値が取得できること。
     *
     */
    @Test
    public void getAliasNameColumn() throws Exception {
        // ------------------------------------------------ setup
        VariousDbTestHelper.setUpTable(
                SqlRowEntity.createDefaultValueInstance(1L)
        );

        // ------------------------------------------------ find
        final SqlPStatement statement = connection.prepareStatement(
                "SELECT SQLROW_ID id,"
                        + " CHAR_COL c,"
                        + " VARCHAR_COL v"
                        + " FROM SQLROW_TEST");
        final SqlResultSet rs = statement.retrieve();

        // ------------------------------------------------ assert
        final SqlRow sut = rs.get(0);
        assertThat("id", sut.getLong("id"), is(1L));
        assertThat("c", sut.getString("c"), is("a"));
        assertThat("v", sut.getString("v"), is("あいうえおかきくけこ"));
    }

    /**
     * Mapインタフェースを実装したメソッド全般のテスト
     *
     * @throws Exception
     */
    @Test
    public void mapMethod() throws Exception {
        SqlRow sut = new SqlRow(new HashMap<String, Object>(), new HashMap<String, Integer>(),
                new HashMap<String, String>());

        assertThat("初期化後のサイズは0", sut.size(), is(0));
        assertThat("初期化直後は空", sut.isEmpty(), is(true));
        assertThat("値を追加した場合、古い値が存在しないのでnullが返却される。", sut.put("key1", "value1"), is(nullValue()));
        assertThat("putした値がgetできる", (String) sut.get("key1"), is("value1"));
        assertThat("putしたキーが存在している", sut.containsKey("key1"), is(true));
        assertThat("putした値が存在する。", sut.containsValue("value1"), is(true));

        sut.putAll(new HashMap<String, Object>() {{
            put("key2", "value2");
            put("key3", "value3");
            put("key4", "value4");
        }});

        assertThat("putAllした要素が増えている(元々1)", sut.size(), is(4));

        assertThat("putした場合元の値が戻される", (String) sut.put("key4", "new_value4"), is("value4"));
        assertThat("新しい値が取得できる", (String) sut.get("key4"), is("new_value4"));

        assertThat("removeで削除された要素が戻される", (String) sut.remove("key4"), is("new_value4"));
        assertThat("removeした要素は存在しない", sut.containsKey("key4"), is(false));
        assertThat("removeした値は存在しない", sut.containsValue("new_value4"), is(false));
        assertThat("削除したので要素が減る", sut.size(), is(3));

        Collection<Object> objects = sut.values();
        assertThat(objects, IsIterableContainingInAnyOrder.<Object>containsInAnyOrder("value1", "value2", "value3"));

        List<String> keys = new ArrayList<String>(sut.keySet());
        assertThat("要素数は、3なのでサイズは3", keys.size(), is(3));
        assertThat(keys, hasItems("key1", "key2", "key3"));

        Map<String, Object> expected = new HashMap<String, Object>();
        expected.put("key1", "value1");
        expected.put("key2", "value2");
        expected.put("key3", "value3");
        for (Map.Entry<String, Object> entry : sut.entrySet()) {
            assertThat(expected.remove(entry.getKey()), is(entry.getValue()));
        }
        assertThat("全ての要素を消したので期待値のMapは空", expected.isEmpty(), is(true));

        assertThat("クリア前の要素は3", sut.size(), is(3));
        sut.clear();
        assertThat("クリア後の要素は0", sut.size(), is(0));

        sut.putAll(new HashMap<String, Object>() {{
            put("key2", "value2");
            put("key3", "value3");
            put("key4", "value4");
        }});
        String str = sut.toString();
        String[] values = str.substring(1, str.length() - 1)
                .split(",");
        assertThat(values.length, is(3));
        List<String> expectedValues = new ArrayList<String>() {{
            add("key4=value4");
            add("key2=value2");
            add("key3=value3");
        }};
        for (String value : values) {
            expectedValues.remove(value.trim());
        }
        if (!expectedValues.isEmpty()) {
            fail("toString()の結果が合わない。toString()で出力されなかった期待値" + expectedValues);
        }
    }

    @Entity
    @Table(name = "SQLROW_TEST")
    private static class SqlRowEntity {

        @Id
        @Column(name = "SQLROW_ID", length = 20)
        public Long sqlRowId;

        @Column(name = "CHAR_COL", columnDefinition = "char(1)")
        public String charCol = "a";

        @Column(name = "VARCHAR_COL", length = 100)
        public String varcharCol = "あいうえおかきくけこ";

        @Column(name = "SHORT_COL", length = 4)
        public Short shortCol = 9999;

        @Column(name = "INTEGER_COL", length = 9)
        public Integer integerCol = 999999999;

        @Column(name = "LONG_COL", length = 18)
        public Long longCol = 999999999999999999L;

        @Column(name = "FLOAT_COL", precision = 10, scale = 5)
        public Float floatCol = 123.123F;

        @Column(name = "DOUBLE_COL", precision = 20, scale = 10)
        public Double doubleCol = 12345.12345;

        @Column(name = "BIG_DECIMAL_COL", precision = 15, scale = 5)
        public BigDecimal bigDecimalCol = new BigDecimal("1234512345.12345");
        
        @Column(name = "BIG_DECIMAL_COL2", precision = 15, scale = 10)
        public BigDecimal bigDecimalCol2 = new BigDecimal("0.0000000001");

        @Column(name = "DATE_COL", columnDefinition = "DATE")
        @Temporal(TemporalType.DATE)
        public Date dateCol = testDate;

        @Column(name = "TIME_COL")
        public Time timeCol = testTime;

        @Column(name = "TIMESTAMP_COL")
        public Timestamp timestampCol = testTimestamp;

        @Column(name = "BINARY_COL")
        public byte[] binaryCol = {0x00, 0x01, 0x30, 0x31};

        @Column(name = "BOOLEAN_COL")
        public Boolean booleanCol = false;

        public SqlRowEntity() {
        }

        public SqlRowEntity(Long sqlRowId) {
            this.sqlRowId = sqlRowId;
        }

        public static SqlRowEntity createDefaultValueInstance(Long id) {
            return new SqlRowEntity(id);
        }


        /**
         * カラムにIntegerの範囲内の値を設定し新しいインスタンスを生成する。
         *
         * @param id 主キー
         * @return 生成したインスタンス
         */
        public static SqlRowEntity createSetIntegerValueInstance(Long id) {
            final SqlRowEntity entity = new SqlRowEntity(id);
            entity.charCol = "0";
            entity.varcharCol = String.valueOf(Integer.MAX_VALUE);
            entity.shortCol = 1;
            entity.integerCol = 2;
            entity.longCol = 3L;
            entity.floatCol = 4F;
            entity.doubleCol = 5D;
            entity.bigDecimalCol = new BigDecimal("6");
            return entity;
        }

        /**
         * カラムにLongの範囲内の値を設定し新しいインスタンスを生成する。
         *
         * @param id 主キー
         * @return 生成したインスタンス
         */
        public static SqlRowEntity createSetLongValueInstance(long id) {
            final SqlRowEntity entity = new SqlRowEntity(id);
            entity.charCol = "1";
            entity.varcharCol = "10";
            entity.shortCol = 100;
            entity.integerCol = 1000;
            entity.longCol = 10000L;
            entity.floatCol = 99999F;
            entity.doubleCol = 1000000D;
            entity.bigDecimalCol = new BigDecimal("10000000");
            return entity;
        }

        /**
         * 各カラムにBigDecimalとして取得可能な値を設定し新しいインスタンスを生成する。
         *
         * @param id 主キー
         * @return 生成したインスタンス
         */
        public static SqlRowEntity createSetBigDecimalValueInstance(long id) {
            final SqlRowEntity entity = new SqlRowEntity(id);
            entity.charCol = "9";
            entity.varcharCol = "99999.9999999999";
            return entity;
        }

        /**
         * 属性値を全てnullとしたオブジェクトを生成する。
         *
         * @param id 主キー
         * @return 生成したオブジェクト
         */
        public static SqlRowEntity createSetNullValue(Long id) {
            final SqlRowEntity entity = new SqlRowEntity(id);
            entity.charCol = null;
            entity.varcharCol = null;
            entity.shortCol = null;
            entity.integerCol = null;
            entity.longCol = null;
            entity.floatCol = null;
            entity.doubleCol = null;
            entity.bigDecimalCol = null;
            entity.dateCol = null;
            entity.timeCol = null;
            entity.timestampCol = null;
            entity.binaryCol = null;
            return entity;
        }

        static SqlRowEntity createFromStringCol(Long id, String charCol, String varcharCol) {
            final SqlRowEntity entity = new SqlRowEntity(id);
            entity.charCol = charCol;
            entity.varcharCol = varcharCol;
            return entity;
        }

        static SqlRowEntity createFromNumberCol(Long id, Short shortCol, Integer integerCol, Long longCol) {
            final SqlRowEntity entity = new SqlRowEntity(id);
            entity.shortCol = shortCol;
            entity.integerCol = integerCol;
            entity.longCol = longCol;
            return entity;
        }

        static SqlRowEntity createFromBooleanCol(Long id, boolean booleanCol) {
            final SqlRowEntity entity = new SqlRowEntity(id);
            entity.booleanCol = booleanCol;
            return entity;
        }
    }
}

