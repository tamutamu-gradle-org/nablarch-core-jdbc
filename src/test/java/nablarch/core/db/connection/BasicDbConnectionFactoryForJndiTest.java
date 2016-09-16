package nablarch.core.db.connection;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import nablarch.core.db.DbExecutionContext;
import nablarch.core.db.connection.exception.DbConnectionException;
import nablarch.core.db.statement.BasicStatementFactory;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.transaction.TransactionContext;

import org.junit.Test;

import mockit.Expectations;
import mockit.Mocked;


/**
 * {@link BasicDbConnectionFactoryForJndi}のテストクラス。
 *
 * @author Hisaaki Sioiri
 */
public class BasicDbConnectionFactoryForJndiTest {

    @Mocked
    private InitialContext jndiContext;

    @Mocked
    private DataSource dataSource;

    @Mocked
    private Connection con;

    @Mocked
    private DbExecutionContext context;

    @Mocked
    private DbAccessExceptionFactory exceptionFactory;

    private static final String CONNECTION_NAME = TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY;

    /**
     * {@link BasicDbConnectionFactoryForJndi#getConnection()}のテスト。
     * <br/>
     * <p/>
     * JNDIからlookupしたdatasourceから取得したconnectionを使用して処理することを確認する。
     *
     * @throws Exception
     */
    @Test
    public void testGetConnectionDefaultInitialContext() throws Exception {
        // jndiから取得されるdataSource、connectionの設定を行う。
        final String lookupName = "nablarch_test";
        new Expectations() {{
            jndiContext.lookup(lookupName);
            result = dataSource;
        }};
        new Expectations() {{
            dataSource.getConnection();
            result = con;
        }};

        BasicDbConnectionFactoryForJndi forJndi = new BasicDbConnectionFactoryForJndi();
        forJndi.setJndiResourceName(lookupName);

        TransactionManagerConnection connection = forJndi.getConnection(CONNECTION_NAME);
        assertThat(connection, instanceOf(BasicDbConnection.class));
        assertThat(((BasicDbConnection) connection).getConnection(), is(con));
    }

    /**
     * {@link BasicDbConnectionFactoryForJndi#getConnection()}のテスト。
     * <p />
     *  指定したプロパティを利用してlookupすることを検証するためにInitialContextに指定したインスタンスを渡すことを確認する。
     *
     * @throws Exception
     */
    @Test
    public void testGetConnection() throws Exception {
        // 引数で渡したパラメータを元にInitialContextが生成されること。
        Map<String, String> property = new HashMap<String, String>();
        property.put(InitialContext.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.fscontext.RefFSContextFactory");
        property.put(InitialContext.PROVIDER_URL, "file:./java/nablarch/core/db/connection");
        final String lookupName = "test";
        final Properties jndiProp = new Properties();
        jndiProp.putAll(property);
        new Expectations() {{
            new InitialContext(jndiProp);
        }};
        new Expectations() {{
            jndiContext.lookup(lookupName);
            result = dataSource;
        }};
        new Expectations(){{
            dataSource.getConnection();
            result = con;
        }};

        BasicDbConnectionFactoryForJndi forJndi = new BasicDbConnectionFactoryForJndi();
        forJndi.setJndiResourceName(lookupName);
        forJndi.setJndiProperties(property);

        TransactionManagerConnection connection = forJndi.getConnection(CONNECTION_NAME);
        assertThat(connection, instanceOf(BasicDbConnection.class));
        assertThat(((BasicDbConnection) connection).getConnection(), is(con));
    }

    /**
     *  statementReuseのデフォルト設定でSqlPStatementが同じものが取得できる。
     *
     * @throws Exception
     */
    @Test
    public void testStatementReuseDefault() throws Exception {
        final String lookupName = "nablarch_test";
        new Expectations() {{
            jndiContext.lookup(lookupName);
            result = dataSource;
        }};
        new Expectations() {{
            dataSource.getConnection();
            result = con;
        }};

        BasicDbConnectionFactoryForJndi forJndi = new BasicDbConnectionFactoryForJndi();
        forJndi.setJndiResourceName(lookupName);
        forJndi.setStatementFactory(new BasicStatementFactory());
        TransactionManagerConnection connection = forJndi.getConnection(CONNECTION_NAME);

        String sql = "SELECT * FROM TEST_TABLE";
        SqlPStatement statement1 = connection.prepareStatement(sql);
        SqlPStatement statement2 = connection.prepareStatement(sql);
        assertThat("デフォルト設定でSqlPStatementが同一のものが返ってくる。",
                statement1, is(statement2));

    }

    /**
     * statementReuseの値をfalseに設定することでSqlPStatementが異なるものが返ってくる。
     *
     * @throws Exception
     */
    @Test
    public void testStatementReuseFalse() throws Exception {
        final String lookupName = "nablarch_test";
        new Expectations() {{
            jndiContext.lookup(lookupName);
            result = dataSource;
        }};
        new Expectations() {{
            dataSource.getConnection();
            result = con;
        }};

        BasicDbConnectionFactoryForJndi forJndi = new BasicDbConnectionFactoryForJndi();
        forJndi.setJndiResourceName(lookupName);
        forJndi.setStatementFactory(new BasicStatementFactory());
        forJndi.setStatementReuse(false);
        TransactionManagerConnection connection = forJndi.getConnection(CONNECTION_NAME);

        String sql = "SELECT * FROM TEST_TABLE";
        SqlPStatement statement1 = connection.prepareStatement(sql);
        SqlPStatement statement2 = connection.prepareStatement(sql);
        assertThat("statementReuseの値をfalseに設定することでSqlPStatementが異なるものが返ってくる。",
                statement1, not(is(statement2)));

    }

    /**
     * {@link BasicDbConnectionFactoryForJndi#getConnection()}の異常系テスト。
     * <p/>
     * JNDIのプロバイダが存在しない場合のテスト。
     *
     * @throws Exception
     */
    @Test
    public void testGetConnectionError() throws Exception {
        // 想定する振る舞いを設定。
        Map<String, String> property = new HashMap<String, String>();
        property.put(InitialContext.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.fscontext.RefFSContextFactory");
        property.put(InitialContext.PROVIDER_URL, "file:./hoge/hoge");
        final String lookupName = "test";
        final Properties jndiProp = new Properties();
        jndiProp.putAll(property);
        // 設定されたMAPをベースにコンテキストを作成し、lookup時にNamingExceptionが発生する。
        new Expectations() {{
            new InitialContext(jndiProp);
        }};
        new Expectations() {{
            jndiContext.lookup(lookupName);
            result = new NamingException("リソースがない");
        }};

        BasicDbConnectionFactoryForJndi forJndi = new BasicDbConnectionFactoryForJndi();
        forJndi.setJndiResourceName(lookupName);
        forJndi.setJndiProperties(property);

        try {
            forJndi.getConnection(CONNECTION_NAME);
            fail("例外が発生しないといけない.");
        } catch (Exception e) {
            assertThat(e.getMessage(), is("failed to DataSource lookup. jndiResourceName = [test]"));
            assertThat(e.getCause(), not(nullValue()));
            assertThat(e.getCause().getMessage(), is("リソースがない"));
        }
    }

    /**
     * {@link BasicDbConnectionFactoryForJndi#getConnection()}の異常系テスト。
     * <p />
     * JNDIでlookupしたdataSourceからconnectionを取得した際に例外が発生した場合。
     *
     * @throws Exception テスト実行時の例外
     */
    @Test
    public void testGetConnectionFailedToConnetion() throws Exception {
        // 想定する振る舞いを設定。
        Map<String, String> property = new HashMap<String, String>();
        property.put(InitialContext.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.fscontext.RefFSContextFactory");
        property.put(InitialContext.PROVIDER_URL, "file:./test/java/nablarch/core/db/connection");
        final String lookupName = "test_error";
        final Properties jndiProp = new Properties();
        jndiProp.putAll(property);
        final SQLException nativeException = new SQLException("connection取得時にエラー");
        final String message = "failed to get database connection. jndiResourceName = [test_error]";
        final DbConnectionException expected = new DbConnectionException(message, nativeException);
        // 設定されたMAPをベースにコンテキストを作成し、取得したDataSourceがSQL例外を投げた場合、SQL例外を元に例外ファクトリが生成した例外を投げる。
        new Expectations() {{
            new InitialContext(jndiProp);
        }};
        new Expectations() {{
            jndiContext.lookup(lookupName);
            result = dataSource;
        }};
        new Expectations() {{
            dataSource.getConnection();
            result = nativeException;
        }};
        // DataSourceが送出した例外を渡すことをverifyする。
        new Expectations() {{
            exceptionFactory.createDbAccessException(message, nativeException, null);
            returns(expected);
        }};

        BasicDbConnectionFactoryForJndi forJndi = new BasicDbConnectionFactoryForJndi();
        forJndi.setJndiResourceName(lookupName);
        forJndi.setJndiProperties(property);
        forJndi.setDbAccessExceptionFactory(exceptionFactory);

        try {
            forJndi.getConnection(CONNECTION_NAME);
            fail("例外が発生しないといけない.");
        } catch (DbConnectionException e) {
            assertThat(e, is(expected));
        }
    }

    /**
     * {@link BasicDbConnectionFactoryForJndi#getConnection()}のテスト。
     * <br/>
     * <p/>
     * JNDIのルックアップ先のプールサイズを超えてデータベース接続の取得要求をした場合
     *
     * @throws Exception
     */
    @Test
    public void testConnectionPoolSizeOver() throws Exception {
        // 想定した振る舞いを設定
        Map<String, String> property = new HashMap<String, String>();
        property.put(InitialContext.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.fscontext.RefFSContextFactory");
        property.put(InitialContext.PROVIDER_URL, "file:./test/java/nablarch/core/db/connection");
        // 設定したMAPをベースにコンテキストを生成し、lookupしたDataSourceがnullを返却した場合
        final String lookupName = "empty";
        final Properties jndiProp = new Properties();
        jndiProp.putAll(property);
        new Expectations() {{
            new InitialContext(jndiProp);
        }};
        new Expectations() {{
            jndiContext.lookup(lookupName);
            result = dataSource;
        }};
        new Expectations() {{
            dataSource.getConnection();
            result = null;
        }};
        BasicDbConnectionFactoryForJndi forJndi = new BasicDbConnectionFactoryForJndi();
        forJndi.setJndiResourceName(lookupName);
        forJndi.setJndiProperties(property);

        try {
            forJndi.getConnection(CONNECTION_NAME);
            fail("例外が発生しないといけない.");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is("database connection lookup result was null. JNDI resource name = [empty]"));
        }
    }
}

