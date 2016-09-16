package nablarch.core.db.connection;

import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * {@link DataSource}からデータベース接続を取得し、BasicDbConnectionを生成すクラス。
 * <p/>
 * {@link DataSource}は、データベースへの接続情報を設定の上、#setDataSource(javax.sql.DataSource)を使用して本オブジェクトに設定すること。
 * <p/>
 * ※{@link DataSource}へのデータベース接続情報の設定方法は、各データベースベンダーのJDBCマニュアルを参照の上実施すること。
 *
 * @author Hisaaki Sioiri
 */
public class BasicDbConnectionFactoryForDataSource extends ConnectionFactorySupport {

    /** データソースオブジェクト */
    private DataSource dataSource = null;

    /**
     * データベース接続オブジェクトを取得する。
     *
     * @return データベース接続オブジェクト
     */
    @Override
    public TransactionManagerConnection getConnection(String connectionName) {

        try {
            BasicDbConnection dbConnection = new BasicDbConnection(dataSource.getConnection());
            initConnection(dbConnection, connectionName);
            return dbConnection;
        } catch (SQLException e) {
            throw dbAccessExceptionFactory.createDbAccessException("failed to get database connection.", e, null);
        }
    }

    /**
     * {@link DataSource}を設定する。
     *
     * @param dataSource データソース
     */
    public void setDataSource(final DataSource dataSource) {
        this.dataSource = dataSource;
    }
}

