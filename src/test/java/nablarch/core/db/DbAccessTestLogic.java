package nablarch.core.db;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import nablarch.core.ThreadContext;
import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.ParameterizedSqlPStatement;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.db.statement.SqlRow;
import nablarch.core.db.statement.autoproperty.UserId;
import nablarch.core.db.statement.exception.DuplicateStatementException;
import nablarch.core.db.statement.exception.SqlStatementException;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.core.repository.di.DiContainer;
import nablarch.core.repository.di.config.xml.XmlComponentDefinitionLoader;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * データベースアクセス機能全般のテストクラス。
 * サブクラスにて、{@link #getConfigUrl}を実装し、
 * 使用するコンポーネント定義を変更することにより、
 * 様々な組み合わせで、本クラスで用意されたテストメソッドを実行することができる。
 *
 * @author T.Kawasaki
 */
public abstract class DbAccessTestLogic {

    private static final String TABLE_NAME = TestEntity.class.getAnnotation(Table.class).name();

    private static final String SELECT_TEST_TABLE = "select * from " + TABLE_NAME + " order by col1";
    private static final String DELETE_TEST_TABLE = "delete from " + TABLE_NAME;
    private static final String INSERT_TEST_TABLE = "insert into " + TABLE_NAME + " (col1) values (?) ";
    private static final String UPDATE_TEST_TABLE_ID = "update " + TABLE_NAME + " set col1 = ? where col1 = ?";

    /** テスト対象 */
    private final SimpleDbTransactionManager dbManager = createDbManager();

    /**
     * セットアップ
     */
    @BeforeClass
    public static void setUpTable() throws Exception {
        VariousDbTestHelper.createTable(TestEntity.class);
        VariousDbTestHelper.createTable(Test2Entity.class);
    }

    @After
    public void tearDown() throws Exception {
        if (DbConnectionContext.containConnection("transaction")) {
            dbManager.endTransaction();
        }
    }

    /**
     * テストに使用するコンポーネント定義ファイルのパスを取得する。
     * サブクラスで、このメソッドの返却値を変えることで、
     * 様々なコンポーネント定義で同一のテストを実行できる。
     *
     * @return コンポーネント定義ファイルのパス
     */
    protected abstract String getConfigUrl();

    /**
     * テスト対象となる{@link SimpleDbTransactionManager}を生成する。
     * @return テスト対象
     */
    protected SimpleDbTransactionManager createDbManager() {
        String url = getConfigUrl();
        return createDbManagerFrom(url);
    }

    /**
     * テスト１。<br>
     * <ul>
     * <li>取得のテスト</li>
     * <li>コミットのテスト</li>
     * <li>キャッシュのテスト(同一のSQLの場合は、同一のインスタンスが返却される。)</li>
     * <li>キャッシュのテスト(異なるSQLの場合は、異なるインスタンスが返却される。)</li>
     * </ul>
     */
    @Test
    public void testTransactionCommit() {

        VariousDbTestHelper.setUpTable(TestEntity.create("10001", "a", 10000)
                        , TestEntity.create("10002", "b", 20000)
                        , TestEntity.create("10003", "c", 30000));

        // start tran
        dbManager.beginTransaction();

        AppDbConnection dbConnection = DbConnectionContext.getConnection();
        String sql = SELECT_TEST_TABLE;
        SqlPStatement select = dbConnection.prepareStatement(sql);

        SqlResultSet resultSet = select.retrieve();
        assertEquals("取得件数は、3件であること。", 3, resultSet.size());

        SqlRow row = resultSet.get(0);
        assertEquals("10001", row.getString("COL1"));
        assertEquals("a", row.getString("COL2"));
        assertEquals("10000", row.getString("COL3"));

        SqlPStatement delete = dbConnection.prepareStatement(DELETE_TEST_TABLE);
        assertEquals("3件削除されること", 3, delete.executeUpdate());

        assertEquals("同一のトランザクションでは削除済み", 0, select.retrieve().size());
        List<TestEntity> beforeCommit = VariousDbTestHelper.findAll(TestEntity.class);
        assertEquals("コミット前は、削除されていないこと。", 3, beforeCommit.size());

        dbManager.commitTransaction();

        List<TestEntity> afterCommit = VariousDbTestHelper.findAll(TestEntity.class);
        assertEquals("コミット後は、削除されていること。", 0, afterCommit.size());

        dbManager.endTransaction();
    }

    /**
     * ステートメントのキャッシュのテスト。
     */
    @Test
    public void testStatementCache() throws Exception {
        // start tran
        dbManager.beginTransaction();

        AppDbConnection dbConnection = DbConnectionContext.getConnection();
        String sql = SELECT_TEST_TABLE;
        SqlPStatement statement = dbConnection.prepareStatement(sql);

        // 同一のSQLの場合は、同一のインスタンスが返却されること。
        assertThat(dbConnection.prepareStatement(sql), sameInstance(statement));
        // 異なるSQLの場合は、異なるインスタンスが返却されること。
        assertThat(dbConnection.prepareStatement(sql + " "), not(sameInstance(statement)));
        // statementをクローズした場合は、異なるインスタンスが返却されること。
        statement.close();
        assertThat(dbConnection.prepareStatement(sql), not(sameInstance(statement)));

        dbManager.endTransaction();
    }

    /**
     * ロールバックのテスト。
     */
    @Test
    public void testRollback() {

        VariousDbTestHelper.setUpTable(TestEntity.create("10001", "a", 10000)
                        , TestEntity.create("10002", "b", 20000)
                        , TestEntity.create("10003", "c", 30000));

        dbManager.beginTransaction();
        AppDbConnection dbConnection = DbConnectionContext.getConnection();
        SqlPStatement statement;
        try {
            SqlPStatement delete = dbConnection.prepareStatement(DELETE_TEST_TABLE);
            assertEquals("3件削除されること", 3, delete.executeUpdate());

            statement = dbConnection.prepareStatement(SELECT_TEST_TABLE);
            SqlResultSet sqlResultSet = statement.retrieve();
            assertEquals("削除されたので0件", 0, sqlResultSet.size());
        } finally {
            dbManager.rollbackTransaction();
        }
        SqlResultSet sqlResultSet = statement.retrieve();
        assertEquals("rollbackしたので3件", 3, sqlResultSet.size());
        dbManager.endTransaction();
    }

    /**
     * 一意制約違反が発生した場合のテスト。
     */
    @Test
    public void testDuplicateError() {
        VariousDbTestHelper.setUpTable(TestEntity.create("10001", "a", 10000)
                        , TestEntity.create("10002", "b", 20000)
                        , TestEntity.create("10003", "c", 30000));

        dbManager.beginTransaction();
        AppDbConnection connection = DbConnectionContext.getConnection();
        SqlPStatement statement = connection.prepareStatement(INSERT_TEST_TABLE);
        statement.setString(1, "10001");
        try {
            statement.executeUpdate();
            fail("一意制約違反が発生するため、ここは通らない。");
        } catch (DuplicateStatementException ignored) {
        }
        statement = connection.prepareStatement(UPDATE_TEST_TABLE_ID);
        statement.setString(1, "10002");
        statement.setString(2, "10001");
        try {
            statement.execute();
            fail("一意制約違反が発生するため、ここは通らない。");
        } catch (DbAccessException ignored) {
        }
        statement.setString(1, "100022");
        statement.setString(2, "10001");
        try {
            statement.execute();
            fail("桁数オーバーが発生するため、ここは通らない。");
        } catch (SqlStatementException e) {
            assertThat(e, not(instanceOf(DuplicateStatementException.class)));
        }
        dbManager.endTransaction();
    }

    /**
     * トランザクション終了後の確認。<br>
     * <ul>
     * <li>コミット後に更新されたデータはトランザクションの終了時にはロールバックされる</li>
     * <li>close後にSQL文を実行した場合は、例外が発生する</li>
     * </ul>
     */
    @Test
    public void testAfterTransactionEnd() {
        VariousDbTestHelper.setUpTable(TestEntity.create("10001", "a", 10000)
                        , TestEntity.create("10002", "b", 20000)
                        , TestEntity.create("10003", "c", 30000));

        dbManager.beginTransaction();
        dbManager.commitTransaction();

        AppDbConnection dbConnection = DbConnectionContext.getConnection();
        SqlPStatement pst = dbConnection.prepareStatement("update test_table set col2 = 'c'");
        assertEquals(3, pst.executeUpdate());

        // クローズ呼び出すと、関連するリソースがすべて開放される。
        dbManager.endTransaction();

        List<TestEntity> entities = VariousDbTestHelper.findAll(TestEntity.class, "col1");

        // 全レコード'c'に更新したが、ロールバックされる。
        assertEquals("a", entities.get(0).col2);
        assertEquals("b", entities.get(1).col2);
        assertEquals("c", entities.get(2).col2);

        // close後にSQLの実行
        try {
            pst.executeUpdate();
            fail("");
        } catch (DbAccessException e) {
            assertEquals(
                    "failed to executeUpdate. SQL = [update test_table set col2 = 'c']",
                    e.getMessage());
        }
    }

    /**
     * 設定された内容が正しく反映されるかのテスト。<br>
     * <ul>
     * <li>initSqlの確認</li>
     * <li>アイソレーションレベルの確認</li>
     * </ul>
     */
    @Test
    public void testDbAccessConfigurations() throws Exception {
        VariousDbTestHelper.setUpTable(Test2Entity.create("1")
                        , Test2Entity.create("2")
                        , Test2Entity.create("3"));

        assertEquals(3, VariousDbTestHelper.findAll(Test2Entity.class).size());

        // トランザクション開始時にinitSqlが実行されデータが削除されること。
        dbManager.beginTransaction();

        assertEquals(0, VariousDbTestHelper.findAll(Test2Entity.class).size());

        AppDbConnection dbConnection = DbConnectionContext.getConnection();

        // アイソレーションレベルの確認(READ_COMMITTEDと設定した場合)
        Field field = dbConnection.getClass().getDeclaredField("con");
        field.setAccessible(true);
        Connection connection = (Connection) field.get(dbConnection);
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, connection.getTransactionIsolation());
        dbManager.endTransaction();
    }

    /**
     * Object、Mapを引数にした機能のテスト。<br>
     * <ul>
     * <li>executeUpdateByMapの確認</li>
     * <li>executeUpdateByObjectの確認</li>
     * </ul>
     */
    @Test
    public void testObjectOrMapParamStatement() throws Exception {
        VariousDbTestHelper.setUpTable(TestEntity.create("10001", "a", 10000)
                        , TestEntity.create("10002", "b", 20000)
                        , TestEntity.create("10003", "c", 30000));

        dbManager.beginTransaction();

        AppDbConnection connection = DbConnectionContext.getConnection();
        ParameterizedSqlPStatement save = connection.prepareParameterizedSqlStatement(
                "insert into test_table (col1, col2, col3) values (:col1, :col2, :col3)");
        Map<String, Object> insertData = new HashMap<String, Object>();
        insertData.put("col1", "00004");
        insertData.put("col2", "あ");
        insertData.put("col3", 1000);
        assertEquals(1, save.executeUpdateByMap(insertData));

        dbManager.commitTransaction();
        TestEntity entity4 = VariousDbTestHelper.findById(TestEntity.class, "00004");
        assertThat(entity4, not(nullValue()));
        assertThat(entity4.col1, is("00004"));
        assertThat(entity4.col2, is("あ"));

        ThreadContext.setUserId("12345");
        TestEntity entity = new TestEntity();
        entity.col2 = "aaa";
        entity.col3 = 123;
        assertEquals(1, save.executeUpdateByObject(entity));
        dbManager.endTransaction();
    }

    public static class ParamObject {
        private String col1 = "10001";

        public String getCol1() {
            return col1;
        }
    }

    /**
     * SQLを外出しにしたテスト。
     * <ul>
     * <li>リソースに記載されたSQLが実行できる。</li>
     * <li>Ojbectを引数に処理できる。</li>
     * </ul>
     */
    @Test
    public void testExternalSqlResource() throws Exception {

        // SQL_IDのプレフィックス(リソース名)
        String prefix = DbAccessTestLogic.class.getName() + "#";

        dbManager.beginTransaction();

        VariousDbTestHelper.setUpTable(TestEntity.create("10001", "a", 10000)
                        , TestEntity.create("10002", "b", 20000)
                        , TestEntity.create("10003", "c", 30000));

        AppDbConnection connection = DbConnectionContext.getConnection();
        SqlPStatement sql001 = connection.prepareStatementBySqlId(prefix + "SQL_001");
        SqlResultSet sql001Result = sql001.retrieve();
        assertEquals(3, sql001Result.size());
        assertEquals("10001", sql001Result.get(0).get("col1"));
        assertEquals("10002", sql001Result.get(1).get("col1"));
        assertEquals("10003", sql001Result.get(2).get("col1"));


        SqlPStatement sql002 = connection.prepareStatementBySqlId(prefix + "SQL_002");
        sql002.setString(1, "10002");
        SqlResultSet sql002Result = sql002.retrieve();
        assertEquals(1, sql002Result.size());
        assertEquals("b", sql002Result.get(0).get("col2"));


        ParameterizedSqlPStatement sql003 = connection.prepareParameterizedSqlStatementBySqlId(
                prefix + "SQL_003");
        SqlResultSet sql003Result = sql003.retrieve(new ParamObject());
        assertEquals(1, sql003Result.size());
        assertEquals("a", sql003Result.get(0).get("col2"));

        Map<String, Object> o = new HashMap<String, Object>();
        o.put("col1", null);
        o.put("col2", "b");

        ParameterizedSqlPStatement sql004 = connection.prepareParameterizedSqlStatementBySqlId(
                prefix + "SQL_004", o);
        SqlResultSet sql004Result = sql004.retrieve(o);
        assertEquals(1, sql004Result.size());
        assertEquals("10002", sql004Result.get(0).get("col1"));

        final ArrayPropertyBean bean = new ArrayPropertyBean();
        bean.col1 = new String[] {"10001", "10003"};
        ParameterizedSqlPStatement sql005 = connection.prepareParameterizedSqlStatementBySqlId(
                prefix + "SQL_005", bean);
        SqlResultSet sql005Result = sql005.retrieve(bean);
        assertEquals(2, sql005Result.size());
        assertEquals("10001", sql005Result.get(0).get("col1"));
        assertEquals("10003", sql005Result.get(1).get("col1"));

        dbManager.endTransaction();
    }

    public static class ArrayPropertyBean {

        private String[] col1;

        public String[] getCol1() {
            return col1;
        }
    }

    /** テスト用のエンティティ */
    @Entity
    @Table(name="TEST_TABLE")
    public static class TestEntity {

        @UserId
        @Id
        @Column(name="col1", length=5, nullable=false)
        public String col1;

        @Column(name="col2", length=10)
        public String col2;

        @Column(name="col3", length=10)
        public Integer col3;

        @Column(name="col4", columnDefinition="date", nullable=true)
        @Temporal(TemporalType.DATE)
        public Date col4;

        @Column(name="col5", nullable=true)
        public byte[] col5;

        private static TestEntity create(String col1, String col2, Integer col3) {
            TestEntity entity = new TestEntity();
            entity.col1 = col1;
            entity.col2 = col2;
            entity.col3 = col3;
            return entity;
        }

        public String getCol1() {
            return col1;
        }

        public String getCol2() {
            return col2;
        }

        public Integer getCol3() {
            return col3;
        }

        public Date getCol4() {
            return col4;
        }

        public byte[] getCol5() {
            return col5;
        }
    }

    @Entity
    @Table(name="TEST_TABLE2")
    public static class Test2Entity {
        @Id
        @Column(name="col1", length=1)
        public String col1;

        private static Test2Entity create(String id) {
            Test2Entity entity = new Test2Entity();
            entity.col1 = id;
            return entity;
        }
    }

    /**
     * データベースアクセス機能全般のテストクラス。
     * 別のコンポーネント定義で動作させる必要があるため、別クラスとした。
     */
    public static abstract class ConfigurationsTest {

        /** テスト対象 */
        private final SimpleDbTransactionManager dbManager = createDbManager();

        /**
         * テストに使用するコンポーネント定義ファイルのパスを取得する。
         * サブクラスで、このメソッドの返却値を変えることで、
         * 様々なコンポーネント定義で同一のテストを実行できる。
         *
         * @return コンポーネント定義ファイルのパス
         */
        protected abstract String getConfigUrl();

        /**
         * テスト対象となる{@link SimpleDbTransactionManager}を生成する。
         * @return テスト対象
         */
        protected SimpleDbTransactionManager createDbManager() {
            String url = getConfigUrl();
            return createDbManagerFrom(url);
        }

        /**
         * テスト８。<br>
         * <ul>
         * <li>initSqlのデフォルト値確認</li>
         * <li>アイソレーションレベルのデフォルト値確認</li>
         * <li>ステートメントキャッシュのオフ時の確認</li>
         * </ul>
         */
        @Test
        public void testDbAccessConfigurations() throws Exception {

            // initSqlなしでもエラーが起きないこと。
            dbManager.beginTransaction();
            AppDbConnection dbConnection = DbConnectionContext.getConnection();
            // アイソレーションレベルの確認
            Field field = dbConnection.getClass().getDeclaredField("con");
            field.setAccessible(true);
            Connection connection = (Connection) field.get(dbConnection);
            assertEquals("デフォルト値は、TRANSACTION_READ_COMMITTEDであること。",
                         Connection.TRANSACTION_READ_COMMITTED, connection.getTransactionIsolation());

            String sql = "select * from test_table";
            // キャッシュがオフのため、異なるインスタンスが生成される。
            assertThat(dbConnection.prepareStatement(sql),
                    not(sameInstance(dbConnection.prepareStatement(sql))));

            dbManager.endTransaction();
        }
    }

    /**
     * 指定されたコンポーネント定義ファイルから{@link SimpleDbTransactionManager}を生成する。
     * @param url コンポーネント定義ファイルのURL
     * @return 指定されたコンポーネント定義ファイルにて定義された{@link SimpleDbTransactionManager}
     */
    private static SimpleDbTransactionManager createDbManagerFrom(String url) {
        final XmlComponentDefinitionLoader loader = new XmlComponentDefinitionLoader(url);
        DiContainer container = new DiContainer(loader);

        return container.getComponentByName("db-manager");
    }
}

