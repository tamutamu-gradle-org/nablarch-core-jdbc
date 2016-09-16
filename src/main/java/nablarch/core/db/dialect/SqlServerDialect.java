package nablarch.core.db.dialect;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import nablarch.core.db.statement.ResultSetConvertor;
import nablarch.core.util.annotation.Published;

/**
 * SqlServer用のSQL方言クラス。
 *
 * @author tani takanori
 */
@Published(tag = "architect")
public class SqlServerDialect extends DefaultDialect {

    /** 重複エラーのリスト */
    private static final int[] DUPLICATE_ERROR_CODE_LIST = {2627, 2601};

    /** クエリーキャンセルのSQLState */
    private static final String QUERY_CANCEL_STATE_CODE = "HY008";

    /** SQLServer用のResultSet変換クラス */
    private static final SqlServerResultSetConvertor RESULT_SET_CONVERTOR = new SqlServerResultSetConvertor();


    /**
     * {@inheritDoc}
     * <p/>
     * SQLServerは、identityカラムが定義できるので{@code true}を返す。
     */
    @Override
    public boolean supportsIdentity() {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * キー重複(エラーコード:2627)及びユニークインデックスの値重複(エラーコード:2601)の場合に、
     * 重複エラーとする。
     */
    @Override
    public boolean isDuplicateException(SQLException sqlException) {
        final int actualErrorCode = sqlException.getErrorCode();

        for (int expectedErrorCode : DUPLICATE_ERROR_CODE_LIST) {
            if (expectedErrorCode == actualErrorCode) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * クエリーの実行キャンセルを表すSQLState(HY008)の場合のみトランザクションタイムアウト対象の例外とする。
     */
    @Override
    public boolean isTransactionTimeoutError(SQLException sqlException) {
        return QUERY_CANCEL_STATE_CODE.equals(sqlException.getSQLState());
    }

    /**
     * {@inheritDoc}
     * <p/>
     * {@code varbinary(max)}で定義されたカラムの場合は、{@link ResultSet#getBinaryStream(int)}の結果を返す、
     * ResultSetの変換クラスを返却する。
     */
    @Override
    public ResultSetConvertor getResultSetConvertor() {
        return RESULT_SET_CONVERTOR;
    }

    /**
     * {@inheritDoc}
     *
     * SQLServerは、インラインビュー内にORDER BYを記述することが出来ない。
     * このため、レコード数取得のSQLに変換する際にSQLの一番最後に存在しているORDER BYを削除後に
     * 件数取得用のSQLに変換する。
     */
    @Override
    public String convertCountSql(String sql) {

        final String lowerSql = sql.toLowerCase();
        final int index = lowerSql.lastIndexOf("order by");

        StringBuilder countSql = new StringBuilder(256);
        countSql.append("SELECT COUNT(*) COUNT_ FROM (");

        if (index == -1) {
            countSql.append(sql);
        } else {
            countSql.append(sql.substring(0, index));
        }
        countSql.append(") SUB_");

        return countSql.toString();
    }

    /**
     * SQLServer用のResultSet変換クラス。
     * <p/>
     * このクラスでは、{@code varbinary(max)}で定義されたカラムの場合、
     * ヒープを圧迫しないように{@link ResultSet#getBinaryStream(int)}の結果を返す。
     */
    protected static class SqlServerResultSetConvertor implements ResultSetConvertor {

        @Override
        public Object convert(ResultSet rs, ResultSetMetaData rsmd, int columnIndex) throws SQLException {
            if (rsmd.getColumnType(columnIndex) == Types.LONGVARBINARY) {
                // varbinary(max)は、非常に大きいバイト数のデータが格納される可能性があるため、
                // InputStreamオブジェクトを返す。
                return rs.getBinaryStream(columnIndex);
            }
            return rs.getObject(columnIndex);
        }

        @Override
        public boolean isConvertible(ResultSetMetaData rsmd, int columnIndex) throws SQLException {
            return true;
        }
    }

    @Override
    public String getPingSql() {
        return "select 1";
    }
}
