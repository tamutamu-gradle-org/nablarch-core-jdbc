package nablarch.core.db.connection;

import nablarch.core.util.annotation.Published;

/**
 * データベース接続({@link TransactionManagerConnection})を生成するインタフェース。
 *
 * @author Hisaaki Sioiri
 */
@Published(tag = "architect")
public interface ConnectionFactory {

    /**
     * データベース接続を取得する。
     *
     * @param connectionName コネクション名
     * @return データベース接続オブジェクト
     * @see TransactionManagerConnection
     */
    TransactionManagerConnection getConnection(String connectionName);

}

