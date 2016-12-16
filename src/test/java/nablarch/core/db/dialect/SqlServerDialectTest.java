package nablarch.core.db.dialect;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import nablarch.core.db.statement.ResultSetConvertor;
import nablarch.core.db.statement.SelectOption;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.DbTestRule;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link SqlServerDialect}のテストクラス。
 */
@TargetDb(include = TargetDb.Db.SQL_SERVER)
@RunWith(DatabaseTestRunner.class)
public class SqlServerDialectTest {


    @Rule
    public DbTestRule dbTestRule = new DbTestRule();

    /** テスト対象 */
    private SqlServerDialect sut = new SqlServerDialect();

    /** Native Connection */
    private Connection connection;

    @BeforeClass
    public static void setUpClass() {
        VariousDbTestHelper.createTable(SqlServerDialectEntity.class);
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * {@link SqlServerDialect#supportsIdentity()}のテスト。
     * <p/>
     * trueがかえされること。
     */
    @Test
    public void supportsIdentity() throws Exception {
        assertThat("trueがかえされること", sut.supportsIdentity(), is(true));
    }


    /**
     * {@link SqlServerDialect#supportsSequence()}のテスト。
     * <p/>
     * シーケンスは使えないのでfalseがかえされること。
     */
    @Test
    public void supportsSequence() throws Exception {
        assertThat("falseがかえされること", sut.supportsSequence(), is(false));
    }

    /**
     * {@link SqlServerDialect#supportsOffset()}のテスト。
     * <p/>
     * offsetは使えないのでfalseがかえされること。
     */
    @Test
    public void supportsOffset() throws Exception {
        assertThat("falseがかえされること", sut.supportsOffset(), is(false));
    }


    /**
     * {@link SqlServerDialect#isDuplicateException(SQLException)}のテスト。
     * <p/>
     * 2627(キー重複)または、2601(一意制約インデックスの重複)の場合にも、重複エラーとなること。
     */
    @Test
    public void isDuplicateException() throws Exception {
        assertThat("2627は、true", sut.isDuplicateException(new SQLException("", "", 2627)), is(true));
        assertThat("2601は、true", sut.isDuplicateException(new SQLException("", "", 2601)), is(true));

        assertThat("2627、2601以外はfalse", sut.isDuplicateException(new SQLException("", "", 2600)), is(false));
    }

    /**
     * {@link SqlServerDialect#isTransactionTimeoutError(SQLException)}のテスト。
     * <p/>
     * クエリーキャンセルのSQLState:HY008の場合のみトランザクションタイムアウト対象例外となること。
     */
    @Test
    public void isTransactionTimeoutError() throws Exception {
        assertThat("SQLState:HY008なので対象", sut.isTransactionTimeoutError(new SQLException("", "HY008")), is(true));

        assertThat("SQLStateがHY008以外の場合は、タイムアウト対象外", sut.isTransactionTimeoutError(new SQLException("", "HY007")), is(
                false));
    }

    /**
     * {@link SqlServerDialect#getResultSetConvertor()} のテスト。
     * 取得したConvertorを使って、値の取得ができること。
     */
    @Test
    public void getResultSetConvertor() throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2015, 2, 9, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        Date date = calendar.getTime();
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        VariousDbTestHelper.setUpTable(
                new SqlServerDialectEntity(1L, "12345", 100, 1234554321L, date, new BigDecimal("12345.54321"),
                        timestamp,
                        new byte[] {0x00, 0x50, (byte) 0xFF}));
        connection = VariousDbTestHelper.getNativeConnection();
        final PreparedStatement statement = connection.prepareStatement(
                "SELECT ENTITY_ID, STR, NUM, BIG_INT, DECIMAL_COL, DATE_COL, TIMESTAMP_COL, BINARY_COL, LONG_VAR_BINARY"
                        + " FROM SQL_SERVER_DIALECT WHERE ENTITY_ID = ?");
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
        assertThat("DATE型はTimestampで取得できる", (Timestamp) convertor.convert(rs, meta, 6), is(new Timestamp(date.getTime())));
        assertThat("TIMESTAMP型はTimestampで取得できる", (Timestamp) convertor.convert(rs, meta, 7), is(timestamp));

        // binaryはbyte[]で取得される
        final byte[] bytes = (byte[]) convertor.convert(rs, meta, 8);
        assertThat("値が取得出来ていること", bytes, is(new byte[] {0x00, 0x50, (byte) 0xFF}));

        // long_var_binaryはBinaryInputStreamで取得できること
        final InputStream stream = (InputStream) convertor.convert(rs, meta, 9);

        byte[] b = new byte[1024];
        final int readed = stream.read(b);
        assertThat("3バイトリードできる", readed, is(3));

        assertThat(Arrays.copyOf(b, 3), is(new byte[] {0x00, 0x50, (byte) 0xFF}));

    }

    /**
     * {@link SqlServerDialect#buildSequenceGeneratorSql(String)}のテスト。
     * シーケンスはサポートしないので例外が送出されること。
     */
    @Test(expected = UnsupportedOperationException.class)
    public void buildSequenceGeneratorSql() throws Exception {
        sut.buildSequenceGeneratorSql("seq_name");
    }

    /**
     * {@link SqlServerDialect#convertPaginationSql(String, SelectOption)}のテスト。
     * <p/>
     * offsetがサポートされていないので、SQLの変換がされないこと。
     */
    @Test
    public void convertPaginationSql() throws Exception {
        assertThat(sut.convertPaginationSql("select object_name(12345)", new SelectOption(1, 1)), is(
                "select object_name(12345)"));
    }

    /**
     * {@link SqlServerDialect#convertCountSql(String)}のテスト。
     */
    @Test
    public void convertCountSql() throws Exception {
        final String actual = sut.convertCountSql("SELECT * FROM DUAL");

        assertThat("変換されていること", actual, is("SELECT COUNT(*) COUNT_ FROM (SELECT * FROM DUAL) SUB_"));
        assertThat("order byは削除されること",
                sut.convertCountSql("select * from hog_table order by id, name"),
                is("SELECT COUNT(*) COUNT_ FROM (select * from hog_table ) SUB_"));
    }

    /**
     * {@link SqlServerDialect#convertCountSql(String)}で変換したSQL文が実行可能であることを確認する。
     */
    @Test
    public void convertCountSql_execute() throws Exception {
        VariousDbTestHelper.delete(SqlServerDialectEntity.class);
        for (int i = 0; i < 100; i++) {
            VariousDbTestHelper.insert(new SqlServerDialectEntity((long) i + 1, "name_" + i));
        }
        connection = VariousDbTestHelper.getNativeConnection();
        String sql = "select entity_id, str from sql_server_dialect where str like ? order by entity_id";
        final PreparedStatement statement = connection.prepareStatement(sut.convertCountSql(sql));
        statement.setString(1, "name_3%");
        final ResultSet rs = statement.executeQuery();

        assertThat(rs.next(), is(true));
        assertThat(rs.getInt(1), is(11));       // name_3とname_3x
    }

    /**
     * {@link SqlServerDialect#getPingSql()}のテスト。
     */
    @Test
    public void getPingSql() throws Exception {
        final String pingSql = sut.getPingSql();
        assertThat(pingSql, is("select 1"));

        connection = VariousDbTestHelper.getNativeConnection();
        final PreparedStatement statement = connection.prepareStatement(pingSql);
        final ResultSet rs = statement.executeQuery();
        assertThat(rs, is(notNullValue()));
        rs.close();
    }

    @Entity
    @Table(name = "SQL_SERVER_DIALECT")
    public static class SqlServerDialectEntity {

        @Id
        @Column(name = "entity_id", length = 18, nullable = false)
        public Long id;

        @Column(name = "str", length = 10)
        public String string;

        @Column(name = "num", length = 9)
        public Integer numeric;

        @Column(name = "big_int", columnDefinition = "bigint")
        public Long bigInt;

        @Column(name = "date_col", columnDefinition = "date")
        @Temporal(TemporalType.DATE)
        public Date date;

        @Column(name = "timestamp_col", columnDefinition = "datetime2")
        public Timestamp timestamp;

        @Column(name = "decimal_col", precision = 15, scale = 5)
        public BigDecimal decimal;

        @Column(name = "binary_col", columnDefinition = "varbinary(20)")
        public byte[] binary;

        @Column(name = "long_var_binary", columnDefinition = "image")
        public byte[] longVarBinary;

        public SqlServerDialectEntity() {
        }

        public SqlServerDialectEntity(Long id, String str) {
            this.id = id;
            this.string = str;
        }

        public SqlServerDialectEntity(
                Long id, String string, Integer numeric, Long bigInt, Date date, BigDecimal decimal,
                Timestamp timestamp, byte[] binary) {
            this.id = id;
            this.string = string;
            this.numeric = numeric;
            this.bigInt = bigInt;
            this.date = date;
            this.decimal = decimal;
            this.timestamp = timestamp;
            this.binary = binary;
            this.longVarBinary = binary;
        }

    }
}
