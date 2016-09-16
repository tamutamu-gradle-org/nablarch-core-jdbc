package nablarch.core.db.dialect;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import nablarch.core.db.statement.ResultSetConvertor;
import nablarch.core.db.statement.SelectOption;
import nablarch.core.util.annotation.Published;

/**
 * デフォルトの{@link Dialect}実装クラス。
 * <p/>
 * 本実装では、全ての方言が無効化される。
 *
 * @author hisaaki sioiri
 */
@Published(tag = "architect")
public class DefaultDialect implements Dialect {

    /** {@link ResultSet}から値を取得するクラス */
    private static final ResultSetConvertor RESULT_SET_CONVERTOR = new DefaultResultSetConvertor();

    /**
     * @return {@code false}を返す。
     */
    @Override
    public boolean supportsIdentity() {
        return false;
    }

    /**
     * @return {@code false}を返す。
     */
    @Override
    public boolean supportsSequence() {
        return false;
    }

    /**
     * @return {@code false}を返す。
     */
    @Override
    public boolean supportsOffset() {
        return false;
    }

    /**
     * @return {@code false}を返す。
     */
    @Override
    public boolean isTransactionTimeoutError(SQLException sqlException) {
        return false;
    }

    /**
     * @return {@code false}を返す。
     */
    @Override
    public boolean isDuplicateException(SQLException sqlException) {
        return false;
    }

    /**
     * 全てのカラムを{@link ResultSet#getObject(int)}で取得するコンバータを返す。
     *
     * @return {@inheritDoc}
     */
    @Override
    public ResultSetConvertor getResultSetConvertor() {
        return RESULT_SET_CONVERTOR;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * シーケンス採番はサポートしない。
     *
     * @throws UnsupportedOperationException 呼び出された場合
     */
    @Override
    public String buildSequenceGeneratorSql(final String sequenceName) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("sequence generator is unsupported.");
    }

    /**
     * SQL文を変換せずに返す。
     */
    @Override
    public String convertPaginationSql(String sql, SelectOption selectOption) {
        return sql;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * 以下形式のCOUNT文取得用SQL文に変換する。<br/>
     * {@code SELECT COUNT(*) COUNT_ FROM ('引数のSQL') SUB_}
     */
    @Override
    public String convertCountSql(String sql) {
        return "SELECT COUNT(*) COUNT_ FROM (" + sql + ") SUB_";
    }

    /**
     * {@inheritDoc}
     *
     * デフォルト実装では、本メソッドはサポートしない。
     */
    @Override
    public String getPingSql() {
        throw new UnsupportedOperationException("unsupported getPingSql.");
    }

    /**
     * 全て{@link ResultSet#getObject(int)}で値を取得する{@link ResultSetConvertor}の実装クラス。
     */
    private static class DefaultResultSetConvertor implements ResultSetConvertor {

        @Override
        public Object convert(ResultSet rs, ResultSetMetaData rsmd, int columnIndex) throws SQLException {
            return rs.getObject(columnIndex);
        }

        @Override
        public boolean isConvertible(ResultSetMetaData rsmd, int columnIndex) throws SQLException {
            return true;
        }
    }
}

