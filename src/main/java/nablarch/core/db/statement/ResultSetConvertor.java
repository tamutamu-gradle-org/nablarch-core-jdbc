package nablarch.core.db.statement;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import nablarch.core.util.annotation.Published;

/**
 * {@link java.sql.ResultSet}から1カラムのデータを取得するインタフェース。<br>
 * <br>
 * {@link java.sql.ResultSet#getObject(int)} 以外を使用して、値を取得する必要がある場合には、
 * 本クラスのサブクラスを作成しgetObject(int)以外を使用してデータの取得を行うこと。<br>
 * <br>
 * 主に、getObject(int)を使用した場合にアプリケーションで処理する際に不都合なデータ型が返却される場合に、
 * 本インタフェースの実装クラスが必要となる。<br>
 * <br>
 * 例えば、getObject(int)ではdoubleが返却されるため、{@link java.sql.ResultSet#getBigDecimal(int)}を使用して、明示的に{@link java.math.BigDecimal}を取得したい場合が該当する。
 *
 * @author Hisaaki Sioiri
 */
@Published(tag = "architect")
public interface ResultSetConvertor {

    /**
     * {@link java.sql.ResultSet}から指定されたカラムのデータを取得する。<br>
     *
     * @param rs ResultSet
     * @param rsmd ResultSetMetaData
     * @param columnIndex カラムインデックス
     * @return ResultSetから取得した対象カラムのデータ
     * @throws java.sql.SQLException SQL例外発生時
     */
    Object convert(ResultSet rs, ResultSetMetaData rsmd, int columnIndex) throws SQLException;

    /**
     * 指定されたカラムが変換対象のカラムかを返却する。<br>
     * 指定された、{@link java.sql.ResultSetMetaData}とカラムインデックスから、
     * {@link java.sql.ResultSet#getObject(int)}以外でデータを取得するか否かを返却する。<br>
     *
     * @param rsmd ResultSetMetaData
     * @param columnIndex カラムインデックス
     * @return {@link java.sql.ResultSet#getObject(int)}以外でデータを取得する必要がある場愛には、true
     * @throws SQLException SQL例外発生時
     */
    boolean isConvertible(ResultSetMetaData rsmd, int columnIndex) throws SQLException;

}
