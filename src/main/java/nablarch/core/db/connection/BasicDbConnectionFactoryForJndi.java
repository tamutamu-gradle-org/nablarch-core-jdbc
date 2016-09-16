package nablarch.core.db.connection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

/**
 * JNDI経由で取得した{@link DataSource}からデータベース接続({@link Connection})を取得し、BasicDbConnectionを生成するクラス。
 * <p/>
 * JNDIから{@link DataSource}を取得するための情報は、#setJndiProperties(Map)及び、#setJndiResourceName(String)を使用して設定すること。
 *
 * @author Hisaaki Sioiri
 */
public class BasicDbConnectionFactoryForJndi extends ConnectionFactorySupport {

    /** JNDI properties */
    private Properties jndiProperties;

    /** JNDI resource name */
    private String jndiResourceName;

    /**
     * データベース接続オブジェクトを取得する。
     *
     * @return 指定されたリソース名に対応するデータベース接続オブジェクト
     */
    public TransactionManagerConnection getConnection(String connectionName) {

        try {
            InitialContext context;
            if (jndiProperties == null) {
                // jndiPropertiesが設定されていない場合は、
                // クラスパス配下のjndi.propertiesを使用する。
                context = new InitialContext();
            } else {
                context = new InitialContext(jndiProperties);
            }

            // JNDIリソース名に紐づくデータソースを取得する。
            DataSource dataSource = (DataSource) context.lookup(jndiResourceName);
            Connection connection = dataSource.getConnection();
            if (connection == null) {
                throw new IllegalStateException(
                        "database connection lookup result was null. JNDI resource name = [" + jndiResourceName + ']');
            }

            BasicDbConnection dbConnection = new BasicDbConnection(connection);
            initConnection(dbConnection, connectionName);
            return dbConnection;
        } catch (SQLException e) {
            throw dbAccessExceptionFactory.createDbAccessException(String.format(
                    "failed to get database connection. jndiResourceName = [%s]", jndiResourceName), e, null);
        } catch (NamingException e) {
            throw new IllegalStateException(String.format("failed to DataSource lookup. jndiResourceName = [%s]",
                    jndiResourceName), e);
        }
    }

    /**
     * JNDIプロパティを設定する。
     *
     * @param jndiProperties JNDIプロパティ
     */
    public void setJndiProperties(Map<String, String> jndiProperties) {
        this.jndiProperties = new Properties();
        this.jndiProperties.putAll(jndiProperties);
    }

    /**
     * JNDIリソース名を設定する。
     *
     * @param jndiResourceName JNDIリソース名
     */
    public void setJndiResourceName(String jndiResourceName) {
        this.jndiResourceName = jndiResourceName;
    }
}

