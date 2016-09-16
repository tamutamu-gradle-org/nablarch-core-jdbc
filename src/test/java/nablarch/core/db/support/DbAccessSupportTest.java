package nablarch.core.db.support;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.dialect.Dialect;
import nablarch.core.db.dialect.OracleDialect;
import nablarch.core.db.statement.BasicSqlLoader;
import nablarch.core.db.statement.ParameterizedSqlPStatement;
import nablarch.core.db.statement.ResultSetIterator;
import nablarch.core.db.statement.SqlCStatement;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
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

/**
 * {@link DbAccessSupport}のテストクラス。
 *
 * @author hisaaki sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class DbAccessSupportTest extends DbAccessSupport {

    @ClassRule
    public static SystemRepositoryResource repository = new SystemRepositoryResource("classpath:nablarch/core/db/support/DbAccessSupportTest.xml");

    private SimpleDbTransactionManager tranManager;

    @BeforeClass
    public static void beforeClass() throws Exception {
        VariousDbTestHelper.createTable(TestEntity.class);
        Dialect dialect = repository.getComponentByType(Dialect.class);
        if (dialect == null || dialect.getClass() != OracleDialect.class) {
            return;
        }
        createProcedure();
    }

    /**
     * Procedureの生成を行う。
     */
    private static void createProcedure() throws SQLException {
        BasicSqlLoader loader = repository.getComponentByType(BasicSqlLoader.class);
        Map<String, String> sqlResource = loader.getValue(DbAccessSupportTest.class.getName().replace('.', '/'));

        Connection con = VariousDbTestHelper.getNativeConnection();
        con.setAutoCommit(false);
        PreparedStatement statement = null;

        String add = sqlResource.get("TEST_PROC");
        try {
            statement = con.prepareStatement(add);
            statement.execute();
            close(statement);
        } finally {
            close(statement);
            terminate(con);
        }
    }

    @Before
    public void beginTran() {
        tranManager = repository.getComponentByType(SimpleDbTransactionManager.class);
        tranManager.beginTransaction();
    }

    @After
    public void endTransaction() {
        tranManager.endTransaction();
    }

    /**
     * コネクションの終端処理。
     *
     * @param con コネクション
     */
    private static void terminate(Connection con) {
        try {
            con.commit();
            con.close();
        } catch (Exception ignore) {
            ignore.printStackTrace();
        }
    }

    /**
     * ステートメントをクローズする。
     *
     * @param statement ステートメント
     */
    private static void close(PreparedStatement statement) {
        try {
            if (statement == null || statement.isClosed()) {
                return;
            }
            statement.close();
        } catch (Exception ignore) {
            ignore.printStackTrace();
        }
    }

    @Test
    public void testGetParameterizedSqlStatement() throws Exception {
        VariousDbTestHelper.setUpTable(
                          TestEntity.create("id01", "11111", "22222")
                        , TestEntity.create("id02", "11111", "33332")
                        , TestEntity.create("id03", "11112", "44442"));

        ParameterizedSqlPStatement statement = super.getParameterizedSqlStatement("SQL001");
        SqlResultSet retrieve = statement.retrieve(new ListSearchInfoImpl());
        assertThat(retrieve.size(), is(2));

        final ParamObject object = new ParamObject();

        DbAccessSupport support = new DbAccessSupport(getClass()) {
        };
        ParameterizedSqlPStatement sql002 = support.getParameterizedSqlStatement("SQL002", object);
        SqlResultSet retrieve1 = sql002.retrieve(object);
        assertThat(retrieve1.size(), is(2));

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("col1", "11111");
        condition.put("col2", null);

        ParameterizedSqlPStatement sql003 = support.getParameterizedSqlStatement("SQL002", condition);
        SqlResultSet retrieve2 = sql003.retrieve(condition);
        assertThat(retrieve2.size(), is(2));
    }

    public static class ParamObject {
        private String col1 = "11111";
        private String col2 = null;

        public String getCol1() {
            return col1;
        }

        public String getCol2() {
            return col2;
        }
    }

    @Test
    public void testCountByParameterizedSql() throws Exception {
        VariousDbTestHelper.setUpTable(
                          TestEntity.create("id01", "11111","22222")
                        , TestEntity.create("id02", "11111","33332")
                        , TestEntity.create("id03", "11112","44442"));

        ParamObject paramObject = new ParamObject();
        paramObject.col1 = "11111";

        assertThat(super.countByParameterizedSql("SQL001", paramObject), is(2));

        paramObject = new ParamObject();
        paramObject.col1 = "11111";
        paramObject.col2 = "33332";

        DbAccessSupport support = new DbAccessSupport(getClass()) {};
        assertThat(support.countByParameterizedSql("SQL002", paramObject), is(1));

        paramObject = new ParamObject();
        paramObject.col1 = "11111";
        paramObject.col2 = "44442";
        assertThat(support.countByParameterizedSql("SQL002", paramObject), is(0));
    }

    @Test
    public void testCountByParameterizedSqlByMap() throws Exception {
        VariousDbTestHelper.setUpTable(
                          TestEntity.create("id01", "11111","22222")
                        , TestEntity.create("id02", "11111","33332")
                        , TestEntity.create("id03", "11112","44442"));

        Map<String, String> condition = new HashMap<String, String>();
        condition.put("col1", "11111");
        assertThat(super.countByParameterizedSql("SQL001", condition), is(2));

        condition = new HashMap<String, String>();
        condition.put("col1", "11111");
        condition.put("col2", "33332");
        DbAccessSupport support = new DbAccessSupport(getClass()) {};
        assertThat(support.countByParameterizedSql("SQL002", condition), is(1));

        condition = new HashMap<String, String>();
        condition.put("col1", "11111");
        condition.put("col2", "44442");
        assertThat(support.countByParameterizedSql("SQL002", condition), is(0));
    }

    /**
     * 件数取得SQLの結果が取得出来ない場合。
     *
     * ※本来はありえないため、モックを使って実現する。
     */
    @Test(expected = IllegalStateException.class)
    public void testCountByParameterizedSql_recordNotFound() throws Exception {
        final AppDbConnection connection = DbConnectionContext.getConnection();
        new Expectations(connection) {{
            final ParameterizedSqlPStatement st = connection.prepareParameterizedCountSqlStatementBySqlId(
                    anyString, any);

            final ResultSetIterator rows = st.executeQueryByObject(any);
            rows.next(); result = false;
        }};

        DbAccessSupport support = new DbAccessSupport(getClass());
        support.countByParameterizedSql("SQL001", new Object());
    }

    public class ListSearchInfoImpl extends ListSearchInfo {

        private String col1 = "11111";

        @Override
        public String[] getSearchConditionProps() {
            return null;
        }

        public String getCol1() {
            return col1;
        }
    }


    @Test
    public void testSelectWithPaging() throws Exception {
        VariousDbTestHelper.setUpTable(
                          TestEntity.create("id01", "11111", "22201")
                        , TestEntity.create("id02", "11111", "22202")
                        , TestEntity.create("id03", "11111", "22203")
                        , TestEntity.create("id04", "11111", "22204")
                        , TestEntity.create("id05", "11111", "22205")
                        , TestEntity.create("id06", "11111", "22206")
                        , TestEntity.create("id07", "11111", "22207")
                        , TestEntity.create("id08", "11111", "22208")
                        , TestEntity.create("id09", "11111", "22209")
                        , TestEntity.create("id10", "11111", "22210")
                        , TestEntity.create("id11", "11111", "22211")
                        , TestEntity.create("id12", "11111", "22212")
                        , TestEntity.create("id13", "11111", "22212")
                        , TestEntity.create("id14", "11111", "22214")
                        , TestEntity.create("id15", "11111", "22215")
                        , TestEntity.create("id16", "11111", "22216")
                        , TestEntity.create("id17", "11111", "22217")
                        , TestEntity.create("id18", "11111", "22218")
                        , TestEntity.create("id19", "11111", "22219")
                        , TestEntity.create("id20", "11111", "22220")
                        , TestEntity.create("id21", "11111", "22221")
                        , TestEntity.create("id22", "11111", "22222")
                        , TestEntity.create("id23", "11112", "44442"));

        // デフォルト(max=20、startPageNumber=1)の場合
        ListSearchInfoImpl cond = new ListSearchInfoImpl();
        SqlResultSet rs = super.search("SQL004", cond);
        assertThat(rs.size(), is(20));
        assertThat(rs.get(0).getString("col2"), is("22201"));
        assertThat(rs.get(19).getString("col2"), is("22220"));
        assertThat(cond.getResultCount(), is(22));

        // max=5、startPageNumber=1に設定した場合
        cond.setMax(5);
        cond.setPageNumber(1);
        rs = super.search("SQL004", cond);
        assertThat(rs.size(), is(5));
        assertThat(rs.get(0).getString("col2"), is("22201"));
        assertThat(rs.get(4).getString("col2"), is("22205"));
        assertThat(cond.getResultCount(), is(22));

        // max=5、startPageNumber=5に設定した場合
        cond.setMax(5);
        cond.setPageNumber(5);
        rs = super.search("SQL004", cond);
        assertThat(rs.size(), is(2));
        assertThat(rs.get(0).getString("col2"), is("22221"));
        assertThat(rs.get(1).getString("col2"), is("22222"));
        assertThat(cond.getResultCount(), is(22));

        // maxResultCount=22に設定した場合
        cond.setMaxResultCount(22);
        cond.setMax(5);
        cond.setPageNumber(5);
        rs = super.search("SQL004", cond);
        assertThat(rs.size(), is(2));
        assertThat(rs.get(0).getString("col2"), is("22221"));
        assertThat(rs.get(1).getString("col2"), is("22222"));
        assertThat(cond.getResultCount(), is(22));

        // maxResultCount=21に設定した場合
        cond.setMaxResultCount(21);
        cond.setMax(5);
        cond.setPageNumber(5);
        try {
            super.search("SQL004", cond);
            fail("ここはとおらない");
        } catch (TooManyResultException e) {
            assertThat(e.getMaxResultCount(), is(21));
            assertThat(e.getResultCount(), is(22));
        }

        // 検索結果0件の場合
        cond.col1 = "11113";
        rs = super.search("SQL004", cond);
        assertThat(rs.size(), is(0));
        assertThat(cond.getResultCount(), is(0));
    }

    @Test
    public void testGetSqlPStatement() throws Exception {
        VariousDbTestHelper.setUpTable(
                TestEntity.create("id01", "11111", "22222")
                , TestEntity.create("id02", "11111", "33332")
                , TestEntity.create("id03", "11112", "44442"));

        SqlPStatement sql003 = super.getSqlPStatement("SQL003");
        sql003.setString(1, "11111");
        SqlResultSet retrieve = sql003.retrieve();
        assertThat(retrieve.size(), is(2));
    }

    /**
     * {@link DbAccessSupport#countByStatementSql(String)}のテスト。
     * @throws Exception
     */
    @Test
    public void testCountByStatementSql() throws Exception {
        VariousDbTestHelper.setUpTable(
                          TestEntity.create("id01", "11111","22222")
                        , TestEntity.create("id02", "11111","33332")
                        , TestEntity.create("id03", "11112","44442"));

        int count1 = countByStatementSql("SQL005");
        assertThat(count1, is(3));

        int count2 = countByStatementSql("SQL006");
        assertThat(count2, is(2));
    }

    /**
     * サポート経由でストアドの実行ができること。
     * @throws Exception
     */
    @Test
    @TargetDb(include = TargetDb.Db.ORACLE)
    public void getSqlCStatement() throws Exception {
        final SqlCStatement statement = getSqlCStatement("SQL007");
        statement.registerOutParameter(1, Types.CHAR);
        statement.registerOutParameter(2, Types.NUMERIC);
        statement.execute();
        assertThat("文字列の値が取得できる", statement.getString(1), is("12345"));
        assertThat("数値の値が取得できる", statement.getInteger(2), is(100));
    }

    /**
     * サポート経由で更新有りのストアドが実行出来ること。
     * @throws Exception
     */
    @Test
    @TargetDb(include = TargetDb.Db.ORACLE)
    public void getSqlCStatementWithTransaction() throws Exception {
        VariousDbTestHelper.delete(TestEntity.class);
        final SqlCStatement statement = getSqlCStatement("SQL008");
        statement.setString(1, "0001");
        statement.setString(2, "0002");
        statement.setString(3, "0003");
        statement.setString(4, "VAL1");
        statement.setString(5, "VAL2");
        statement.registerOutParameter(6, Types.INTEGER);
        statement.executeUpdate();

        assertThat("登録件数は3", statement.getInteger(6), is(3));
        tranManager.commitTransaction();

        List<TestEntity> results = VariousDbTestHelper.findAll(TestEntity.class, "id");
        assertThat("ストアドで3件登録出来ていること", results.size(), is(3));
        assertThat("登録内容の確認", results.get(0).id, is("0001"));
        assertThat("登録内容の確認", results.get(1).id, is("0002"));
        assertThat("登録内容の確認", results.get(2).id, is("0003"));
        for (TestEntity entity : results) {
            assertThat(entity.col1, is("VAL1"));
            assertThat(entity.col2, is("VAL2"));
        }
    }

    @Entity
    @Table(name="test")
    public static class TestEntity {
        @Id
        @Column(name="id", length=4)
        public String id;
        @Column(name="col1", length=5)
        public String col1;
        @Column(name="col2", length=5)
        public String col2;
        private static TestEntity create(String id, String col1, String col2) {
            TestEntity entity = new TestEntity();
            entity.id = id;
            entity.col1 = col1;
            entity.col2 = col2;
            return entity;
        }
    }
}
