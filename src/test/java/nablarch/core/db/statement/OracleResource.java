package nablarch.core.db.statement;

import java.sql.SQLException;
import java.util.ResourceBundle;

import javax.sql.DataSource;

import oracle.jdbc.pool.OracleConnectionPoolDataSource;
import oracle.jdbc.pool.OracleDataSource;

/**
 * {@link DataSource}を提供するクラス。
 * db-configファイルをクラスパスから取得し、その情報でデータソースを生成する。
 * {@link org.junit.Rule}アノテーションを付与して使用する。
 * <pre>
 * {@literal @Rule public DataSourceResource dsResource = new DataSourceResource();}
 * </pre>
 * テスト終了時には、リソースの解放が自動的に呼び出される。
 */
public class OracleResource extends DBResource {

    /**
     * データソースを取得する。
     *
     * @return データソース
     */
    @Override
    public DataSource getDataSource() {
        return DataSourceInitializer.ds;
    }

    private static class DataSourceInitializer {

        private static DataSource ds = createDataSource();

        private static DataSource createDataSource() {
            OracleDataSource ds;
            try {
                ds = new OracleConnectionPoolDataSource();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            ResourceBundle conf = ResourceBundle.getBundle("db-config");
            ds.setURL(conf.getString("db.url"));
            ds.setUser(conf.getString("db.user"));
            ds.setPassword(conf.getString("db.password"));
            return ds;
        }

    }

}
