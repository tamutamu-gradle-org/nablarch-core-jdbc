package nablarch.core.db.cache;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import nablarch.core.db.DbAccessException;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.transaction.TransactionContext;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.persistence.*;
import java.sql.*;
import java.util.Date;

/**
 * @author ryo asato
 */
@RunWith(DatabaseTestRunner.class)
public class DataBaseMetaDataCacheTest {

    @ClassRule
    public static SystemRepositoryResource repositoryResource =
            new SystemRepositoryResource("db-default.xml");

    private final TransactionManagerConnection connection =
            repositoryResource.getComponentByType(ConnectionFactory.class)
                    .getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);

    private final Connection nativeConnection = connection.getConnection();

    private static DataBaseMetaDataCache sut = DataBaseMetaDataCache.getInstance();

    @BeforeClass
    public static void setupDatabase() {
        VariousDbTestHelper.createTable(TestEntity.class);
        VariousDbTestHelper.createTable(TestEntity2.class);
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.terminate();
        }
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testGetTableDescriptor() {

        TableDescriptor tableDescriptor = sut.getTableDescriptor("ssd", "RS_TEST", nativeConnection);
        // テーブルの情報が取得できる
        assertThat("テーブル名の検証", tableDescriptor.getTableName(), is("RS_TEST"));
        // 列情報が取得できる
        ColumnDescriptor colDescriptor = tableDescriptor.getColumnDescriptor("VARCHAR_COL");
        assertThat("列名の検証", colDescriptor.getColumnName(), is("VARCHAR_COL"));
        assertThat("SQL型の検証", colDescriptor.getColumnType(), is(Types.VARCHAR));

        colDescriptor = tableDescriptor.getColumnDescriptor("INT_COL");
        assertThat("列名の検証", colDescriptor.getColumnName(), is("INT_COL"));
        assertThat("SQL型の検証", colDescriptor.getColumnType(), is(Types.NUMERIC));

        ColumnDescriptor checkColumnFromSsd = tableDescriptor.getColumnDescriptor("CHECK_COL");

        // スキーマ名がnullの場合
        tableDescriptor = sut.getTableDescriptor(null, "RS_TEST", nativeConnection);
        assertThat("テーブル名の検証", tableDescriptor.getTableName(), is("RS_TEST"));
        colDescriptor = tableDescriptor.getColumnDescriptor("VARCHAR_COL");
        assertThat("列名の検証", colDescriptor.getColumnName(), is("VARCHAR_COL"));
        assertThat("SQL型の検証", colDescriptor.getColumnType(), is(Types.VARCHAR));

        // 指定したスキーマを参照できるか
        // ssdスキーマ
        assertThat("列名の検証", checkColumnFromSsd.getColumnName(), is("CHECK_COL"));
        assertThat("SQL型の検証", checkColumnFromSsd.getColumnType(), is(Types.VARCHAR));
        // nablarchスキーマ
        tableDescriptor = sut.getTableDescriptor("nablarch","RS_TEST", nativeConnection);
        assertThat("テーブル名の検証", tableDescriptor.getTableName(), is("RS_TEST"));
        colDescriptor = tableDescriptor.getColumnDescriptor("CHECK_COL");
        assertThat("列名の検証", colDescriptor.getColumnName(), is("CHECK_COL"));
        assertThat("SQL型の検証", colDescriptor.getColumnType(), is(Types.NUMERIC));
    }

    @Test
    public void testGetColumnDescriptor() {
        // 列情報が取得できる
        ColumnDescriptor columnDescriptor = sut.getColumnDescriptor("ssd", "RS_TEST", "CHECK_COL", nativeConnection);
        assertThat("列名の検証", columnDescriptor.getColumnName(), is("CHECK_COL"));
        assertThat("SQL型の検証", columnDescriptor.getColumnType(), is(Types.VARCHAR));

        // スキーマ名がnullの場合
        columnDescriptor = sut.getColumnDescriptor(null, "RS_TEST", "CHECK_COL", nativeConnection);
        assertThat("列名の検証", columnDescriptor.getColumnName(), is("CHECK_COL"));
        assertThat("SQL型の検証", columnDescriptor.getColumnType(), is(Types.VARCHAR));

        // 指定したスキーマからデータを取得する
        columnDescriptor = sut.getColumnDescriptor("nablarch", "RS_TEST", "CHECK_COL", nativeConnection);
        assertThat("列名の検証", columnDescriptor.getColumnName(), is("CHECK_COL"));
        assertThat("SQL型の検証", columnDescriptor.getColumnType(), is(Types.NUMERIC));
    }

    // 異常系
    @Test
    public void testAbnormalSchemaForTableDesc() {
        expectedException.expect(DbAccessException.class);
        sut.getTableDescriptor("DUMMY_SCHEMA", "RS_TEST", nativeConnection);
    }

    @Test
    public void testAbnormalTableForTableDesc() {
        expectedException.expect(DbAccessException.class);
        expectedException.expectMessage("Can not access to metadata. tablename = DUMMY_TABLE");
        sut.getTableDescriptor("SSD", "DUMMY_TABLE", nativeConnection);
    }

    @Test
    public void testNullTableForTableDesc() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("tableName or connection is null or empty.");
        sut.getTableDescriptor("SSD", null, nativeConnection);
    }

    @Test
    public void testNullConnectionForTableDesc() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("tableName or connection is null or empty.");
        sut.getTableDescriptor("SSD", "RS_TEST", null);
    }

    @Test
    public void testAbnormalSchemaForColumnDesc() {
        expectedException.expect(DbAccessException.class);
        sut.getColumnDescriptor("DUMMY_SCHEMA", "RS_TEST", "VARCHAR_COL", nativeConnection);
    }

    @Test
    public void testAbnormalTableForColumnDesc() {
        expectedException.expect(DbAccessException.class);
        sut.getColumnDescriptor("SSD", "DUMMY_TABLE", "VARCHAR_COL", nativeConnection);
    }

    @Test
    public void testAbnormalColumnForColumnDesc() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("column not found. column: DUMMY_COL");
        sut.getColumnDescriptor("SSD", "RS_TEST", "DUMMY_COL", nativeConnection);
    }

    @Test
    public void testNullTableForColumnDesc() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("tableName, columnName or connection is null or empty.");
        sut.getColumnDescriptor("SSD", null, "VARCHAR_COL", nativeConnection);
    }

    @Test
    public void testNullColumnForColumnDesc() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("tableName, columnName or connection is null or empty.");
        sut.getColumnDescriptor("SSD", "RS_TEST", null, nativeConnection);
    }

    @Test
    public void testNullConectionForColumnDesc() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("tableName, columnName or connection is null or empty.");
        sut.getColumnDescriptor("SSD", "RS_TEST", "VARCHAR_COL", null);
    }

    @Entity
    @Table(name = "RS_TEST")
    public static class TestEntity {

        @Column(name = "varchar_col", length = 5)
        @Id
        public String varcharCol;

        @Column(name = "int_col", length = 9)
        public Integer intCol;

        @Column(name = "date_col")
        @Temporal(TemporalType.DATE)
        public Date dateCol;

        @Column(name = "timestamp_col")
        public Timestamp timestampCol;

        @Column(name = "blob_col")
        public byte[] blobCol;

        @Column(name = "check_col")
        public String schemaCheckCol;

        public TestEntity() {
        }
    }

    @Entity
    @Table(name = "nablarch.RS_TEST")
    public static class TestEntity2 {

        @Column(name = "check_col", length = 1)
        @Id
        public Integer schemaCheckCol;

        public TestEntity2() {
        }
    }
}