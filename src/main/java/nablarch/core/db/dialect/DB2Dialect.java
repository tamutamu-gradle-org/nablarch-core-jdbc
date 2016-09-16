package nablarch.core.db.dialect;

import java.sql.SQLException;

import nablarch.core.util.annotation.Published;

/**
 * DB2用の方言クラス。
 *
 * @author hisaaki sioiri
 */
@Published(tag = "architect")
public class DB2Dialect extends DefaultDialect {

    /** 一意制約違反を表すSQLState */
    private static final String UNIQUE_ERROR_SQL_STATE = "23505";

    /** Query Timeアウト時に発生する例外のエラーコード */
    private static final String QUERY_CANCEL_SQL_STATE = "57014";

    /**
     * {@inheritDoc}
     * <p/>
     * DB2では、IDENTITY属性が使えるので{@code true}を返す。
     */
    @Override
    public boolean supportsIdentity() {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * DB2では、シーケンスオブジェクトが使えるので{@code true}を返す。
     */
    @Override
    public boolean supportsSequence() {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * {@link SQLException#getSQLState()}が23505(一意制約違反)の場合、一意制約違反とする。
     */
    @Override
    public boolean isDuplicateException(SQLException sqlException) {
        return UNIQUE_ERROR_SQL_STATE.equals(sqlException.getSQLState());
    }

    /**
     * {@inheritDoc}
     * <p/>
     * DB2では、以下のSQLStateの場合にトランザクションタイムアウト対象の例外と判断する。
     * <ul>
     * <li>57014:(SQL0952N:割り込みによる処理の取消)</li>
     * </ul>
     */
    @Override
    public boolean isTransactionTimeoutError(SQLException sqlException) {
        return QUERY_CANCEL_SQL_STATE.equals(sqlException.getSQLState());
    }

    /**
     * {@inheritDoc}
     * <p/>
     * 「VALUES NEXTVAL FOR」を使用して次の値を取得するSQL文を構築する。
     */
    @Override
    public String buildSequenceGeneratorSql(String sequenceName) {
        return "VALUES NEXTVAL FOR " + sequenceName;
    }

    @Override
    public String getPingSql() {
        return "select 1 from SYSIBM.DUAL";
    }
}

