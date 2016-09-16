package nablarch.core.db.dialect;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

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
 * {@link DB2Dialect}のテストクラス。
 */
@TargetDb(include = TargetDb.Db.DB2)
@RunWith(DatabaseTestRunner.class)
public class DB2DialectTest {

    @Rule
    public DbTestRule dbTestRule = new DbTestRule();

    /** テスト対象 */
    private DB2Dialect sut = new DB2Dialect();

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
     * {@link DB2Dialect#supportsIdentity()}のテスト。
     * <p/>
     * trueがかえされること。
     */
    @Test
    public void supportsIdentity() throws Exception {
        assertThat("DB2では、identityが使えるのでtrueが返される", sut.supportsIdentity(), is(true));
    }

    /**
     * {@link DB2Dialect#supportsSequence()}のテスト。
     * <p/>
     * trueがかえされること。
     */
    @Test
    public void supportsSequence() throws Exception {
        assertThat("DB2では、シーケンスオブジェクトが疲れるのでtrueが返される", sut.supportsSequence(), is(true));
    }

    /**
     * {@link DB2Dialect#supportsOffset()}のテスト。
     * <p/>
     * DB2は、offsetを使えないのでfalseを返す。
     */
    @Test
    public void supportsOffset() throws Exception {
        assertThat("DB2は、offsetを使えないのでfalse", sut.supportsOffset(), is(false));
    }

    /**
     * {@link DB2Dialect#convertPaginationSql(String, SelectOption)}のテスト。
     * <p/>
     * DB2では、サポートしないのでSQLが変換されないこと。
     */
    @Test
    public void convertPaginationSql() throws Exception {
        assertThat("SQLが変換されないこと", sut.convertPaginationSql("sql", new SelectOption(1, 1)), is("sql"));
    }

    /**
     * {@link DB2Dialect#isDuplicateException(SQLException)}のテスト。
     * <p/>
     * DB2では、SQLStateが23505の場合に一意制約違反となる。
     */
    @Test
    public void isDuplicateException() throws Exception {
        assertThat("SQLStateが23505なので一意制約違反", sut.isDuplicateException(new SQLException("", "23505")), is(true));

        assertThat("SQLStateが23505以外なので一意制約違反ではない", sut.isDuplicateException(new SQLException("", "23504")), is(false));
        assertThat("エラーコードが23505は一意制約違反ではない", sut.isDuplicateException(new SQLException("", "", 23505)), is(false));
    }

    ///** トランザクションタイムアウトと判断する例外のリスト */
    //private static final String[] TRANSACTION_TIMEOUT_ERROR_LIST = {"57014", "57033", "40001"};

    /**
     * {@link DB2Dialect#isTransactionTimeoutError(SQLException)}のテスト。
     */
    @Test
    public void isTransactionTimeoutError() throws Exception {
        assertThat("SQLStateが57014なのでタイムアウトエラー対象",
                sut.isTransactionTimeoutError(new SQLException("", "57014")), is(true));


        assertThat("SQLStateが57033のロック要求タイムアウトは、対象外",
                sut.isTransactionTimeoutError(new SQLException("", "57033")), is(false));
        assertThat("SQLStateが40001のロック要求タイムアウトは対象外",
                sut.isTransactionTimeoutError(new SQLException("", "40001")), is(false));

        assertThat("SQLStateが57014以外なのでトランザクションタイムアウトエラー対象ではない",
                sut.isTransactionTimeoutError(new SQLException("", "57032")), is(false));
    }

    /**
     * {@link DB2Dialect#buildSequenceGeneratorSql(String)}のテスト。
     */
    @Test
    public void buildSequenceGeneratorSql() throws Exception {
        assertThat("シーケンスで次の順序を取得する「VALUES NEXTVAL FOR」文が構築されること",
                sut.buildSequenceGeneratorSql("test_seq"), is("VALUES NEXTVAL FOR test_seq"));
    }

    /**
     * {@link DB2Dialect#getResultSetConvertor()}のテスト。
     * <p/>
     * Convertorが狙った通りの型で値を取得できることの確認を行う。
     */
    @Test
    public void getResultSetConvertor() throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2015, 2, 9, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        Date date = calendar.getTime();
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
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
        assertThat("DATE型はDateで取得できる", (Date) convertor.convert(rs, meta, 6), is(date));
        assertThat("TIMESTAMP型はTimestampで取得できる", (Timestamp) convertor.convert(rs, meta, 7), is(timestamp));

        // binaryはblobで取得される
        final Blob blob = (Blob) convertor.convert(rs, meta, 8);
        assertThat("長さは3", blob.length(), is(3L));
        assertThat("値が取得出来ていること", blob.getBytes(1, 3), is(new byte[] {0x00, 0x50, (byte) 0xFF}));
    }

    /**
     * {@link DB2Dialect#convertCountSql(String)}のテスト。
     */
    @Test
    public void convertCountSql() throws Exception {
        final String actual = sut.convertCountSql("SELECT * FROM DUAL");
        assertThat("変換されていること", actual, is("SELECT COUNT(*) COUNT_ FROM (SELECT * FROM DUAL) SUB_"));
    }

    /**
     * {@link DB2Dialect#convertCountSql(String)}で変換したSQL文が実行可能であることを確認する。
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
     * {@link DB2Dialect#getPingSql()}のテスト。
     */
    @Test
    public void getPingSql() throws Exception {
        final String pingSql = sut.getPingSql();
        assertThat(pingSql, is("select 1 from SYSIBM.DUAL"));

        final Connection connection = VariousDbTestHelper.getNativeConnection();
        final PreparedStatement statement = connection.prepareStatement(pingSql);

        final ResultSet rs = statement.executeQuery();
        assertThat(rs, is(notNullValue()));
        rs.close();
    }
}

