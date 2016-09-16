package nablarch.core.db.statement;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.sql.DataSource;

import org.junit.rules.ExternalResource;

/**
 * データベースコネクションを提供するクラス。
 *
 * @author T.Kawasaki
 */
public abstract class DBResource extends ExternalResource {

    /** 使用中のコネクション */
    private final Set<Connection> connInUse = Collections.synchronizedSet(new HashSet<Connection>());;

    private volatile boolean testFinished = false;


    /**
     * コネクションを取得する。
     *
     * @return コネクション
     */
    public Connection getConnection() {
        if (testFinished) {
            throw new IllegalStateException("test finished.");
        }
        try {
            Connection conn = doGetConnection();
            connInUse.add(conn);
            return conn;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection doGetConnection() throws SQLException {
        return getDataSource().getConnection();
    }

    /**
     * データソースを取得する。
     *
     * @return データソース
     */
    public abstract DataSource getDataSource();


    /**
     * テスト終了時の処理を行う。データソースのクローズを行う。
     */
    @Override
    protected void after() {
        closeAllConnection();
    }

    protected void closeAllConnection() {
        for (Connection conn : connInUse) {
            closeQuietly(conn);
        }
    }

    public void closeQuietly(Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.close();
        } catch (SQLException ignored) {
        } catch (RuntimeException ignored) {
        }
    }

}
