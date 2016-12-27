package nablarch.core.db.connection;

import java.util.HashMap;
import java.util.Map;

import nablarch.core.transaction.TransactionContext;
import nablarch.core.util.annotation.Published;


/**
 * データベース接続({@link AppDbConnection})をスレッド単位に管理するクラス。
 * <p/>
 * 設定されたデータベース接続をスレッドに紐付けて管理する。<br/>
 * データベース接続の取得要求があった場合は、スレッドに紐付いているデータベース接続を返す。<br/>
 *
 * @author Koichi Asano
 */
public final class DbConnectionContext {

    /** 隠蔽コンストラクタ。 */
    private DbConnectionContext() {

    }

    /** スレッドに紐付けたDB接続 */
    private static final ThreadLocal<Map<String, AppDbConnection>> connection =
            new ThreadLocal<Map<String, AppDbConnection>>() {
                @Override
                protected Map<String, AppDbConnection> initialValue() {
                    return new HashMap<String, AppDbConnection>();
                }
            };

    /**
     * データベース接続をデフォルトの名前でスレッドに設定する。
     * <p/>
     * データベース接続の設定には、"transaction"という名前が使用される。
     * 設定できるデフォルトのデータベース接続はカレントスレッドに対して一つまでである。
     *
     * @param con データベース接続
     * @throws IllegalArgumentException カレントスレッドに対してデフォルトのデータベース接続を複数設定した場合
     */
    @Published(tag = "architect")
    public static void setConnection(AppDbConnection con) {
        setConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY, con);
    }

    /**
     * データベース接続を指定した名前でスレッドに設定する。
     * <p/>
     * 名前はスレッド内でユニークでなければならない。
     *
     * @param connectionName データベース接続名
     * @param con データベース接続
     * @throws IllegalArgumentException カレントスレッドに対して同じ名前のデータベース接続が設定されている場合
     */
    @Published(tag = "architect")
    public static void setConnection(String connectionName, AppDbConnection con) {
        Map<String, AppDbConnection> localMap = connection.get();
        if (localMap.containsKey(connectionName)) {
            throw new IllegalArgumentException(
                    String.format(
                            "specified database connection name was duplication in thread local. connection name = [%s]",
                            connectionName));
        }
        localMap.put(connectionName, con);
    }

    /**
     * 現在のスレッドに紐付けられたデフォルトのデータベース接続を取得する。
     * <p/>
     * データベース接続の取得には、"transaction"という名前が使用される。
     *
     * @return データベース接続
     */
    @Published(tag = "architect")
    public static AppDbConnection getConnection() {
        return getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
    }

    /**
     * 現在のスレッドに紐付けられた指定した名前のデータベース接続を取得する。
     *
     * @param connectionName データベース接続名
     * @return データベース接続
     * @throws IllegalArgumentException データベース接続が見つからなかった場合
     */
    @Published(tag = "architect")
    public static AppDbConnection getConnection(String connectionName) {
        Map<String, AppDbConnection> localMap = connection.get();
        AppDbConnection con = localMap.get(connectionName);
        if (con == null) {
            throw new IllegalArgumentException(String
                    .format("specified database connection name is not register in thread local. connection name = [%s]",
                            connectionName));
        }
        return con;
    }

    /**
     * 現在のスレッドに指定した名前のデータベース接続が保持されているか判定する。
     *
     * @param connectionName データベース接続名
     * @return データベース接続が保持されていれば{@code true}
     */
    public static boolean containConnection(String connectionName) {
        Map<String, AppDbConnection> localMap = connection.get();
        return localMap.containsKey(connectionName);
    }

    /**
     * 現在のスレッドに紐付いたデフォルトのデータベース接続を削除する。
     * <p/>
     * データベース接続の取得には"transaction"という名前が使用される。
     */
    @Published(tag = "architect")
    public static void removeConnection() {
        removeConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
    }

    /**
     * 現在のスレッドに紐付いた指定した名前のデータベース接続を削除する。
     *
     * @param connectionName データベース接続名
     */
    @Published(tag = "architect")
    public static void removeConnection(String connectionName) {
        Map<String, AppDbConnection> localMap = connection.get();
        localMap.remove(connectionName);
        if (localMap.isEmpty()) {
            connection.remove();
        }
    }

    /**
     * 現在のスレッドに紐付いたデフォルトのトランザクション制御を取得する。
     * <p/>
     * トランザクション制御の取得には、"transaction"という名前が使用される。
     *
     * @return トランザクション制御
     * @throws ClassCastException データベース接続の実体が{@link TransactionManagerConnection}を実装していない場合
     * @throws IllegalArgumentException データベース接続が見つからなかった場合
     */
    @Published(tag = "architect")
    public static TransactionManagerConnection getTransactionManagerConnection() {
        return getTransactionManagerConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
    }

    /**
     * 現在のスレッドから指定した名前のトランザクション制御を取得する。
     *
     * @param connectionName データベース接続名
     * @return トランザクション制御
     * @throws ClassCastException データベース接続の実体が{@link TransactionManagerConnection}を実装していない場合
     * @throws IllegalArgumentException データベース接続が見つからなかった場合
     */
    @Published(tag = "architect")
    public static TransactionManagerConnection getTransactionManagerConnection(
            String connectionName) {
        return (TransactionManagerConnection) getConnection(connectionName);
    }

}
