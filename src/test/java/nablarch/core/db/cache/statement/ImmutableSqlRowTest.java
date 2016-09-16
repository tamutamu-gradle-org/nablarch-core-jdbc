package nablarch.core.db.cache.statement;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;

import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import nablarch.core.db.cache.statement.ImmutableSqlResultSet.ImmutableSqlRow;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlRow;
import nablarch.core.db.statement.SqlRowTestLogic;
import nablarch.core.util.DateUtil;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link ImmutableSqlRow}のテストクラス。
 * {@link SqlRow}との互換性テストもあわせて実施する。
 *
 * @author T.Kawasaki
 * @see SqlRowTestLogic
 */
@RunWith(DatabaseTestRunner.class)
public class ImmutableSqlRowTest extends SqlRowTestLogic {

    /** SQL IDのプレフィックス */
    private static final String PREFIX
            = "nablarch/core/db/cache/statement/MutableGuardingSqlRowTest#";

    @ClassRule
    public static final SystemRepositoryResource systemRepositoryResource = new SystemRepositoryResource(
            "nablarch/core/db/cache/statement/ImmutableSqlRowTest.xml");

    @Override
    protected ConnectionFactory createConnectionFactory() {
        return systemRepositoryResource.getComponentByType(ConnectionFactory.class);
    }

    @BeforeClass
    public static void createImmutableSqlRowTable() {
        VariousDbTestHelper.createTable(ImmutableSqlRowEntity.class);
    }

    /** Date型の値が変更から保護されていること。*/
    @Test
    public void testGetDateGuarded() {
        SqlRow orig = new SqlRow(
                new HashMap<String, Object>() {
                    {
                        put("col1", DateUtil.getDate("20140101"));
                        put("col2", null);
                    }
                },
                new HashMap<String, Integer>() {
                    {
                        put("col1", Types.DATE);
                        put("col2", Types.DATE);
                    }
                },
                new HashMap<String, String>() {
                    {
                        put("col1", "col1");
                        put("col2", "col2");
                    }
                });
        ImmutableSqlRow target = new ImmutableSqlRow(orig);
        Date date1 = target.getDate("col1");
        assertThat(date1, is(DateUtil.getDate("20140101")));


        Date date2 = target.getDate("col1");
        assertThat(date2, is(not(sameInstance(date1))));
        date1.setTime(0);  // 書き換え
        assertThat(date2, is(DateUtil.getDate("20140101")));

        // null test.
        assertThat(target.getDate("col2"), is(nullValue()));
        assertThat(target.get("col2"), is(nullValue()));
    }

    /** Timestamp型の値が変更から保護されていること。*/
    @Test
    public void testGetTimestampGuarded() throws ParseException {

        final Timestamp timestamp = Timestamp.valueOf("2010-07-09 00:00:00.000");
        VariousDbTestHelper.setUpTable(
                new ImmutableSqlRowEntity(1L, timestamp, null, null),
                new ImmutableSqlRowEntity(2L, null, null, null)
        );

        SqlPStatement statement = connection.prepareStatementBySqlId(
                PREFIX + "SELECT_TIMESTAMP");

        ImmutableSqlResultSet rs = (ImmutableSqlResultSet) statement.retrieve();

        statement.close();

        assertThat(rs.size(), is(2));

        ImmutableSqlRow sqlRow = (ImmutableSqlRow) rs.get(0);

        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        Timestamp ts1st = sqlRow.getTimestamp("timestamp_col");
        Timestamp ts2nd = sqlRow.getTimestamp("timestamp_col");
        Timestamp ts3rd = (Timestamp) sqlRow.get("timestamp_col");


        assertThat(ts1st, is(new Timestamp(format.parse("20100709").getTime())));
        assertThat(ts2nd, is(not(sameInstance(ts1st))));
        ts1st.setTime(0); // 書き換え
        assertThat(ts2nd, is(new Timestamp(format.parse("20100709").getTime()))); //2ndに影響なし

        assertThat(ts3rd, is(not(sameInstance(ts1st))));
        ts3rd.setTime(0); // 書き換え
        assertThat(ts2nd, is(new Timestamp(format.parse("20100709").getTime()))); //2ndに影響なし


        // null test.
        ImmutableSqlRow nullRow = (ImmutableSqlRow) rs.get(1);
        assertThat(nullRow.getTimestamp("timestamp_col"), is(nullValue()));
        assertThat(nullRow.get("timestamp_col"), is(nullValue()));
    }

    /** byte[]型の値が変更から保護されていること。*/
    @Test
    public void testBytesGuarded() {
        VariousDbTestHelper.setUpTable(
                new ImmutableSqlRowEntity(1L, null, new byte[] {0x31, 0x32, 0x33, 0x34}, null),
                new ImmutableSqlRowEntity(2L, null, null, null)
        );


        SqlPStatement statement = connection.prepareStatementBySqlId(
                PREFIX + "SELECT_BYTES");

        ImmutableSqlResultSet rs = (ImmutableSqlResultSet) statement.retrieve();

        statement.close();

        ImmutableSqlRow sqlRow = (ImmutableSqlRow) rs.get(0);
        {
            byte[] bytes1st = sqlRow.getBytes("binary_col");
            assertThat(bytes1st, is(new byte[] {0x31, 0x32, 0x33, 0x34}));
            bytes1st[0] = 0x00;  // 書き換え

            byte[] bytes2nd = sqlRow.getBytes("binary_col");
            assertThat(bytes2nd, is(new byte[] {0x31, 0x32, 0x33, 0x34}));
        }
        {
            if (sqlRow.get("binary_col") instanceof byte[]) {
                byte[] bytes1st = (byte[]) sqlRow.get("binary_col");
                assertThat(bytes1st, is(new byte[] {0x31, 0x32, 0x33, 0x34}));
                bytes1st[0] = 0x00;  // 書き換え

                byte[] bytes2nd = (byte[]) sqlRow.get("binary_col");
                assertThat(bytes2nd, is(new byte[] {0x31, 0x32, 0x33, 0x34}));
            }
        }

        // null test.
        ImmutableSqlRow nullRow = (ImmutableSqlRow) rs.get(1);
        assertThat(nullRow.getBytes("binary_col"), is(nullValue()));
        assertThat(nullRow.get("binary_col"), is(nullValue()));
    }

    @Entity
    @Table(name = "IMMUTABLE_SQL_ROW")
    public static class ImmutableSqlRowEntity {

        @Column(name = "SQL_ROW_ID", length = 18)
        @Id
        public Long sqlRowId;

        @Column(name = "TIMESTAMP_COL")
        public Timestamp timestampCol;

        @Column(name = "BINARY_COL")
        public byte[] binary;

        @Column(name = "CHAR_COL", length = 5)
        public String charCol;

        public ImmutableSqlRowEntity() {
        }

        public ImmutableSqlRowEntity(Long sqlRowId, Timestamp timestampCol, byte[] binary, String charCol) {
            this.sqlRowId = sqlRowId;
            this.timestampCol = timestampCol;
            this.binary = binary;
            this.charCol = charCol;
        }
    }
}
