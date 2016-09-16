package nablarch.core.db;

import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.dialect.Dialect;

/**
 * DBアクセス時のインスタンスへの参照を保持するコンテキストクラス。
 * <p />
 *
 * @author tani takanori
 */
public class DbExecutionContext {

    /** データベース接続オブジェクト */
    private final TransactionManagerConnection connection;

    /** SQL方言 */
    private final Dialect dialect;

    /** 接続名 */
    private final String connectionName;

    /**
     * コンストラクタ
     *
     * @param connection データベース接続オブジェクト
     * @param dialect SQL方言
     * @param connectionName 接続名
     */
    public DbExecutionContext(TransactionManagerConnection connection, Dialect dialect, String connectionName) {
        this.connection = connection;
        this.dialect = dialect;
        this.connectionName = connectionName;
    }

    /**
     * データベース接続を行うオブジェクトを取得する。
     *
     * @return データベース接続を行うオブジェクト
     */
    public TransactionManagerConnection getConnection() {
        return connection;
    }

    /**
     * SQL方言を取得する。
     *
     * @return SQL方言
     */
    public Dialect getDialect() {
        return dialect;
    }

    /**
     * 接続名を取得する。
     * <p />
     *
     * @return 接続名
     *
     */
    public String getConnectionName() {
        return connectionName;
    }
}
