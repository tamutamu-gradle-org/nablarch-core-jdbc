package nablarch.core.db.cache.statement;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import nablarch.core.ThreadContext;
import nablarch.core.cache.expirable.BasicExpirationSetting;
import nablarch.core.db.DbExecutionContext;
import nablarch.core.db.cache.InMemoryResultSetCache;
import nablarch.core.db.cache.ResultSetCache;
import nablarch.core.db.cache.ResultSetCacheKey;
import nablarch.core.db.cache.ResultSetCacheKeyBuilder;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.dialect.DefaultDialect;
import nablarch.core.db.statement.ParameterizedSqlPStatement;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.db.statement.autoproperty.RequestId;
import nablarch.core.db.statement.autoproperty.UserId;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.util.Builder;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import mockit.Mocked;


/**
 * {@link CacheableStatementFactory}のテストクラス。
 *
 * @author T.Kawasaki
 */
@RunWith(DatabaseTestRunner.class)
public class CacheableStatementFactoryTest {

    @ClassRule
    public static SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/core/db/cache/statement/CacheableStatementFactoryTest.xml");

    /** SQLIDのプレフィックス */
    private static final String PREFIX = "nablarch/core/db/cache/statement/CacheableStatementFactoryTest#";

    /** テスト対象 */
    private CacheableStatementFactory sut;

    /** キャッシュ */
    private ResultSetCache cache;

    private TransactionManagerConnection conn;

    private DbExecutionContext context;

    @BeforeClass
    public static void setUpClass() {
        VariousDbTestHelper.createTable(CacheStatementFactoryTestEntity.class);
    }

    /** 準備 */
    @Before
    public void setUp() {
        sut = repositoryResource.getComponent("statementFactory");
        cache = repositoryResource.getComponent("resultSetCache");
        ConnectionFactory factory = repositoryResource.getComponent("connectionFactory");
        conn = factory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        context = new DbExecutionContext(conn, repositoryResource.getComponentByType(DefaultDialect.class),
                                         TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        cache.clear();
        prepareTestTable();
    }

    @After
    public void tearDown() {
        conn.terminate();
    }

    /** パラメータ無しステートメントの結果セットがキャッシュできること。 */
    @Test
    public void testNoParamCached() throws SQLException {
        String sqlId = PREFIX + "TEST_NO_PARAM";

        SqlPStatement stmt = conn.prepareStatementBySqlId(sqlId);
        SqlResultSet rs = stmt.retrieve();
        assertThat(rs.size(), is(5));
        assertThat(rs.get(0).getString("MSG"), is("HELLO"));

        // キャッシュにのる。
        ResultSetCacheKey key = new ResultSetCacheKeyBuilder(sqlId).build();
        SqlResultSet cached = cache.getIfNotExpired(key);
        assertThat("等価な結果セットが返却される",
                   rs, is(cached));

        SqlResultSet second = stmt.retrieve();
        assertThat("２回目も同じ",
                   rs, is(second));

    }

    /** キャッシュ対象でないSQLの場合、キャッシュされないこと。（パラメータ無しステートメント） */
    @Test
    public void testNoParamNoCache() throws SQLException {
        String sqlId = PREFIX + "TEST_NO_PARAM_NO_CACHE";
        SqlPStatement stmt = conn.prepareStatementBySqlId(sqlId);
        SqlResultSet rs = stmt.retrieve();
        assertThat(rs.size(), is(5));
        assertThat(rs.get(0).getString("MSG"), is("HELLO"));

        // キャッシュにのらない。
        ResultSetCacheKey key = new ResultSetCacheKeyBuilder(sqlId).build();
        assertThat("キャッシュされていない",
                   cache.getIfNotExpired(key), is(nullValue()));
    }

    /** パラメータ無しステートメントの結果セットがキャッシュできること。（Parameterized） */
    @Test
    public void testNoParamParameterizedCached() throws SQLException {

        String sqlId = PREFIX + "TEST_NO_PARAM";
        ParameterizedSqlPStatement stmt = conn.prepareParameterizedSqlStatementBySqlId(sqlId);
        ImmutableSqlResultSet rs
                = (ImmutableSqlResultSet) stmt.retrieve(new HashMap<String, Object>());
        assertThat(rs.size(), is(5));
        assertThat(rs.get(0).getString("MSG"), is("HELLO"));

        // キャッシュにのる。
        ResultSetCacheKey key = new ResultSetCacheKeyBuilder(sqlId).build();
        SqlResultSet cached = cache.getIfNotExpired(key);
        assertThat(rs, is(cached));
    }

    /** パラメータありステートメントの結果セットがキャッシュできること。 */
    @Test
    public void testWithParamsCached() throws SQLException {

        String sqlId = PREFIX + "TEST_WITH_PARAM";
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("colName1", "10002");

        ParameterizedSqlPStatement stmt
                = conn.prepareParameterizedSqlStatementBySqlId(sqlId, params);
        ImmutableSqlResultSet rs
                = (ImmutableSqlResultSet) stmt.retrieve(params);
        assertThat(rs.size(), is(1));
        assertThat(rs.get(0).getString("colName1"), is("10002"));

        // キャッシュにのる。
        ResultSetCacheKey key = new ResultSetCacheKeyBuilder(sqlId).addParam("colName1", "10002")
                                                                   .build();
        SqlResultSet cached = cache.getIfNotExpired(key);
        assertThat(rs, is(cached));
    }

    /** キャッシュ対象でないSQLの場合、キャッシュされないこと。（パラメータありステートメント） */
    @Test
    public void testWithParamsNoCache() throws SQLException {

        String sqlId = PREFIX + "TEST_WITH_PARAM_NO_CACHE";
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("colName1", "10002");


        ParameterizedSqlPStatement stmt
                = conn.prepareParameterizedSqlStatementBySqlId(sqlId, params);

        SqlResultSet rs = stmt.retrieve(params);
        assertThat(rs, is(not(instanceOf(ImmutableSqlResultSet.class))));
        assertThat(rs.size(), is(1));
        assertThat(rs.get(0).getString("colName1"), is("10002"));

        // キャッシュにのらない。
        ResultSetCacheKey key = new ResultSetCacheKeyBuilder(sqlId).addParam("colName1", "10002")
                                                                   .build();
        assertThat(cache.getIfNotExpired(key), is(nullValue()));

    }

    /** パラメータありステートメントの結果セットがキャッシュできること。（可変条件） */
    @Test
    public void testVariableConditionCached() throws SQLException {

        String sqlId = PREFIX + "TEST_VARIABLE";
        Params params = new Params();
        params.colName3 = 30000L; // 可変条件colName1とcolName3のうち、3だけ設定
        CacheableSqlPStatement stmt
                = (CacheableSqlPStatement)
                conn.prepareParameterizedSqlStatementBySqlId(sqlId, params);

        SqlResultSet rs = stmt.retrieve(params);
        assertThat(rs, instanceOf(ImmutableSqlResultSet.class));
        assertThat(rs.size(), is(1));
        assertThat(rs.get(0).getString("colName1"), is("10003"));

        // キャッシュにのる。
        ResultSetCacheKey key = new ResultSetCacheKeyBuilder(sqlId)
                .addParam("colName1", (Object) null)   // 可変条件で設定していないパラメータ
                .addParam("colName3", 30000L)
                .build();
        SqlResultSet cached = cache.getIfNotExpired(key);
        assertThat(rs, is(cached));
    }

    /**
     * キャッシュ対象でないSQLの場合、パラメータありステートメントの結果セットが
     * キャッシュされないこと。（可変条件）
     */
    @Test
    public void testVariableConditionNoCache() throws SQLException {
        String sqlId = PREFIX + "TEST_VARIABLE_NO_CACHE";
        Params params = new Params();
        params.colName3 = 30000L; // 可変条件colName1とcolName3のうち、3だけ設定

        ParameterizedSqlPStatement stmt
                = conn.prepareParameterizedSqlStatementBySqlId(sqlId, params);

        SqlResultSet rs = stmt.retrieve(params);
        assertThat(rs, not(instanceOf(ImmutableSqlResultSet.class)));
        assertThat(rs.size(), is(1));
        assertThat(rs.get(0).getString("colName1"), is("10003"));

        // キャッシュにのらない。
        ResultSetCacheKey key = new ResultSetCacheKeyBuilder(sqlId)
                .addParam("colName1", (Object) null)   // 可変条件で設定していないパラメータ
                .addParam("colName3", "30000")
                .build();
        assertThat(cache.getIfNotExpired(key), is(nullValue()));

    }


    /** LIKE条件を使用した場合でもキャッシュにヒットすること。(prepare時にパラメータ指定なし）*/
    @Test
    public void testLikeCondition() {
        String sqlId = PREFIX + "TEST_LIKE";  // LIKE条件付きのSQL
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("colName1", "002");    // 後方一致

        ParameterizedSqlPStatement stmt
                = conn.prepareParameterizedSqlStatementBySqlId(sqlId);
        ImmutableSqlResultSet rs
                = (ImmutableSqlResultSet) stmt.retrieve(params);
        assertThat(rs.size(), is(1));
        assertThat(rs.get(0).getString("colName1"), is("10002"));

        // キャッシュにのる。
        ResultSetCacheKey key = new ResultSetCacheKeyBuilder(sqlId).addParam("colName1", "%002")
                                                                   .build();
        SqlResultSet cached = cache.getIfNotExpired(key);
        assertThat(rs, is(cached));
    }

    /** LIKE条件を使用した場合でもキャッシュにヒットすること。(prepare時にパラメータ指定あり）*/
    @Test
    public void testLikeConditionWithParam() {
        String sqlId = PREFIX + "TEST_LIKE";  // LIKE条件付きのSQL
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("colName1", "002");    // 後方一致

        ParameterizedSqlPStatement stmt
                = conn.prepareParameterizedSqlStatementBySqlId(sqlId, params);
        ImmutableSqlResultSet rs
                = (ImmutableSqlResultSet) stmt.retrieve(params);
        assertThat(rs.size(), is(1));
        assertThat(rs.get(0).getString("colName1"), is("10002"));

        // キャッシュにのる。
        ResultSetCacheKey key = new ResultSetCacheKeyBuilder(sqlId).addParam("colName1", "%002")
                                                                   .build();
        SqlResultSet cached = cache.getIfNotExpired(key);
        assertThat(rs, is(cached));
    }

    /** プロパティresultSetCacheが設定されていない場合、例外が発生すること。 */
    @Test(expected = IllegalStateException.class)
    public void testResultSetCacheNotSet(@Mocked Connection mockConnection) throws SQLException {
        CacheableStatementFactory target = new CacheableStatementFactory();
        target.setExpirationSetting(new BasicExpirationSetting());
        target.getParameterizedSqlPStatementBySqlId("SQL", mockConnection, context);
    }

    /** プロパティexpirationSettingが設定されていない場合、例外が発生すること。 */
    @Test(expected = IllegalStateException.class)
    public void testExpirationSettingNotSet(@Mocked Connection mockConnection) throws SQLException {
        CacheableStatementFactory target = new CacheableStatementFactory();
        target.setResultSetCache(new InMemoryResultSetCache());
        target.getParameterizedSqlPStatementBySqlId("SQL", mockConnection, context);
    }

    /** AutoPropertyを使用した場合でも、キャッシュできること。 */
    @Test
    public void testAutoProps() throws SQLException {

        String sqlId = PREFIX + "TEST_AUTO_PROP";

        String sql = Builder.lines(
                "SELECT * FROM TEST_TABLE",
                "WHERE",
                "COL_NAME_1 = :userId AND",
                "COL_NAME_2 = :requestId"
        );

        final AutoPropCondition autoProp = new AutoPropCondition();
        ParameterizedSqlPStatement stmt = conn.prepareParameterizedSqlStatementBySqlId(sqlId, autoProp);

        ThreadContext.setUserId("10003");
        ThreadContext.setRequestId("abc");

        ImmutableSqlResultSet rs
                = (ImmutableSqlResultSet) stmt.retrieve(autoProp);
        assertThat(rs.size(), is(1));
        assertThat(rs.get(0).getString("COL_NAME_1"), is("10003"));

        // キャッシュにのる。
        ResultSetCacheKey key = new ResultSetCacheKeyBuilder(sqlId).addParam("userId", "10003")
                                                                   .addParam("requestId", "abc")
                                                                   .build();
        SqlResultSet cached = cache.getIfNotExpired(key);
        assertThat(rs, is(cached));
    }


    /**
     * プロパティ情報保持オブジェクトがファクトリからステートメントへ移送されていること。
     *
     * @throws SQLException 予期しない例外
     */
    @Test
    public void testObjectFieldCacheSet() throws SQLException {
        String sqlId = PREFIX + "TEST_WITH_PARAM";
        Param params = new Param();

        ParameterizedSqlPStatement statement = sut.getParameterizedSqlPStatementBySqlId
                (sqlId, conn.getConnection(), context);
        SqlResultSet rs = statement.retrieve(params);
        assertThat(rs.size(), is(1));
        assertThat(rs.get(0).getString("COL_NAME_1"), is("10002"));
    }

    public static class Param {
        public String colName1 = "10002";

        public String getColName1() {
            return colName1;
        }
    }

    public static class AutoPropCondition {

        @UserId
        private String userId;

        @RequestId
        private String requestId;

        public String getUserId() {
            return userId;
        }

        public String getRequestId() {
            return requestId;
        }
    }

    public static class Params {
        String colName1;
        String colName2;
        Long colName3;
        String colName4;
        String colName5;

        public String getColName1() {
            return colName1;
        }

        public String getColName2() {
            return colName2;
        }

        public Long getColName3() {
            return colName3;
        }

        public String getColName4() {
            return colName4;
        }

        public String getColName5() {
            return colName5;
        }
    }

    private void prepareTestTable() {
        VariousDbTestHelper.setUpTable(
                new CacheStatementFactoryTestEntity(
                        "10001", null, 11111L, java.sql.Date.valueOf("2010-01-01"), Timestamp.valueOf(
                        "2010-11-01 11:28:00.0"), new BigDecimal("9999999999.12345")),
                new CacheStatementFactoryTestEntity(
                        "10002", "abc", 20000L, java.sql.Date.valueOf("2010-02-01"), Timestamp.valueOf(
                        "2010-11-01 11:28:01.0"), new BigDecimal("99999999999999.011")),
                new CacheStatementFactoryTestEntity(
                        "10003", "abc", 30000L, java.sql.Date.valueOf("2010-03-01"), Timestamp.valueOf(
                        "2010-11-01 11:28:02.0"), new BigDecimal("100")),
                new CacheStatementFactoryTestEntity(
                        "10004", "abc", 40000L, java.sql.Date.valueOf("2010-04-01"), Timestamp.valueOf(
                        "2010-11-01 11:28:03.0"), new BigDecimal("200")),
                new CacheStatementFactoryTestEntity(
                        "10005", "abc", 50000L, java.sql.Date.valueOf("2010-05-01"), Timestamp.valueOf(
                        "2010-11-01 11:28:04.0"), new BigDecimal("30"))
        );

    }

    @Entity
    @Table(name = "CACHE_STATEMENT_TEST_TABLE")
    public static class CacheStatementFactoryTestEntity {

        @Id
        @Column(name = "col_name_1")
        public String colName1;

        @Column(name = "col_name_2", length = 20)
        public String colName2;

        @Column(name = "col_name_3", length = 15)
        public Long colName3;

        @Column(name = "col_name_4", columnDefinition = "date")
        @Temporal(TemporalType.DATE)
        public Date colName4;

        @Column(name = "col_name_5")
        public Timestamp colName5;

        @Column(name = "col_name_6", precision = 20, scale = 5)
        public BigDecimal colName6;

        public CacheStatementFactoryTestEntity() {
        }

        public CacheStatementFactoryTestEntity(
                String colName1, String colName2, Long colName3, Date colName4, Timestamp colName5, BigDecimal colName6) {
            this.colName1 = colName1;
            this.colName2 = colName2;
            this.colName3 = colName3;
            this.colName4 = colName4;
            this.colName5 = colName5;
            this.colName6 = colName6;
        }
    }
}

