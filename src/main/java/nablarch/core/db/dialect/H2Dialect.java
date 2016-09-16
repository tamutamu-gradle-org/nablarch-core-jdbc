package nablarch.core.db.dialect;

import nablarch.core.db.statement.SelectOption;

import java.sql.SQLException;

/**
 * H2用のSQL方言クラス。
 *
 * @author Masaya Seko
 *
 */
public class H2Dialect extends DefaultDialect {

    /** 一意制約違反を表すSQLState */
    private static final String UNIQUE_ERROR_SQL_STATE = "23505";

    /** Query Timeアウト時に発生する例外のエラーコード */
    private static final String QUERY_CANCEL_SQL_STATE = "57014";

    /**
     * {@inheritDoc}
     * <p/>
     * H2では、IDENTITYカラムを使用できるため、 {@code true}を返す。
     */
    @Override
    public boolean supportsIdentity() {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * H2では、シーケンスオブジェクトが使用できるので、 {@code true}を返す。
     */
    @Override
    public boolean supportsSequence() {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * H2では、{@code offset}がサポートされるので{@code true}を返す。
     */
    @Override
    public boolean supportsOffset() {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * {@link SQLException#getSQLState()}が23505(unique_violation:一意制約違反)の場合、一意制約違反とする。
     */
    @Override
    public boolean isDuplicateException(SQLException sqlException) {
        return UNIQUE_ERROR_SQL_STATE.equals(sqlException.getSQLState());
    }

    /**
     * {@inheritDoc}
     * <p/>
     * H2の場合、以下例外の場合タイムアウト対象の例外として扱う。
     * <ul>
     * <li>SQLState:57014(query_canceled:クエリタイムアウト時に送出される例外)</li>
     * </ul>
     */
    @Override
    public boolean isTransactionTimeoutError(SQLException sqlException) {
        final String sqlState = sqlException.getSQLState();
        return QUERY_CANCEL_SQL_STATE.equals(sqlState);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * {@code nextval}関数を使用して、次の順序を取得するSQL文を構築する。
     */
    @Override
    public String buildSequenceGeneratorSql(String sequenceName) {
        return "select nextval('" + sequenceName + "')";
    }

    /**
     * {@inheritDoc}
     * <p/>
     * {@code offset}と{@code limit}を使用したSQL文に変換する。
     */
    @Override
    public String convertPaginationSql(String sql, SelectOption selectOption) {
        final StringBuilder result = new StringBuilder(256);
        result.append(sql);
        if (selectOption.getLimit() > 0) {
            result.append(" limit ")
                    .append(selectOption.getLimit());
        }
        if (selectOption.getOffset() > 0) {
            result.append(" offset ")
                    .append(selectOption.getOffset());
        }
        return result.toString();
    }

    @Override
    public String getPingSql() {
        return "select 1";
    }
}
