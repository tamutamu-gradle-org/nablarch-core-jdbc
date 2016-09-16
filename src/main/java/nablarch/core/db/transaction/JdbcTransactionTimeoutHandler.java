package nablarch.core.db.transaction;

import java.sql.SQLException;

import nablarch.core.db.dialect.Dialect;
import nablarch.core.transaction.TransactionTimeoutException;

/**
 * JDBC経由でデータベースアクセスをした際にトランザクションタイムアウトを実現するクラス。
 *
 * @author hisaaki sioiri
 */
public class JdbcTransactionTimeoutHandler {

    /** ミリ秒 */
    private static final double MILLI_SEC = 1000D;

    /** トランザクションタイムアウト時間（秒） */
    private final int transactionTimeoutSec;

    /** トランザクション有効期限 */
    private int expiryTime;

    /** トランザクション開始時間 */
    private long transactionStartTime;

    /**
     * コンストラクタ。
     * <p/>
     * トランザクションタイムアウト秒数を元にインスタンスを生成する。
     *
     * @param transactionTimeoutSec トランザクションタイムアウト秒数
     */
    public JdbcTransactionTimeoutHandler(int transactionTimeoutSec) {
        this.transactionTimeoutSec = transactionTimeoutSec;
        if (transactionTimeoutSec <= 0) {
            throw new IllegalArgumentException("must be greater than 0.");
        }
    }

    /**
     * トランザクション有効期限をリセットする。
     * <p/>
     * 現在時間を元に、トランザクションの有効期限を算出する。
     */
    public void begin() {
        transactionStartTime = System.currentTimeMillis();
        expiryTime = (int) Math.ceil(transactionStartTime / MILLI_SEC) + transactionTimeoutSec;
    }

    /**
     * トランザクションタイムアウトしているか否かをチェックする。
     * <p/>
     * トランザクションタイムアウトが発生していた場合には、{@link TransactionTimeoutException}を送出する。
     * トランザクションタイムアウトをしているか否かは有効期限({@link #getExpiryTimeSec()}を経過しているかで判断する。
     *
     * @throws TransactionTimeoutException トランザクションタイムアウトしている場合
     */
    public void checkTransactionTimeout() throws TransactionTimeoutException {
        if (isTransactionTimeout()) {
            // 有効期限までの残り時間が0以下の場合には、トランザクションタイムアウト例外を送出する。
            throw new TransactionTimeoutException(System.currentTimeMillis() - transactionStartTime);
        }
    }

    /**
     * トランザクションタイムアウトしているか否かをチェックする。
     * <p/>
     * SQL実行時例外が、トランザクションタイムアウト対象か否かをデータベース方言を用いて判定する。
     * トランザクションタイムアウト対象の例外で、トランザクションの有効期限を超過している場合には、
     * {@link TransactionTimeoutException}を送出する。
     *
     * @param sqle SQL実行時に発生した{@link SQLException}
     * @param dialect データベース方言
     * @throws TransactionTimeoutException トランザクションタイムアウトしている場合
     */
    public void checkTransactionTimeout(SQLException sqle, Dialect dialect) throws TransactionTimeoutException {
        if (!dialect.isTransactionTimeoutError(sqle)) {
            return;
        }
        if (isTransactionTimeout()) {
            throw new TransactionTimeoutException(System.currentTimeMillis() - transactionStartTime, sqle);
        }
    }

    /**
     * トランザクションタイムアウトをしているか否か。
     *
     * @return トランザクションタイムアウトしている場合はtrue
     */
    private boolean isTransactionTimeout() {
        return getExpiryTimeSec() <= 0;
    }

    /**
     * トランザクションタイムアウト時間までの残り秒数を取得する。
     * <p/>
     *
     * @return トランザクションタイムアウト時間までの残り秒数
     */
    public int getExpiryTimeSec() {
        if (expiryTime <= 0L) {
            throw new IllegalStateException(
                    "transaction status is abnormal. must call #begin()");
        }
        return expiryTime - (int) Math.floor(System.currentTimeMillis() / MILLI_SEC);
    }
}

