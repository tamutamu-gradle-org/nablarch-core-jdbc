package nablarch.core.db.statement;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

/**
 * ストアドプロシージャを実行するインタフェース。
 *
 * @author hisaaki sioiri
 * @see java.sql.CallableStatement
 */
public interface SqlCStatement extends SqlPStatement {

    /**
     * {@link java.sql.CallableStatement#registerOutParameter(int, int)}.
     *
     * @param parameterIndex パラメータインデックス
     * @param sqlType {@link java.sql.Types}
     */
    void registerOutParameter(int parameterIndex, int sqlType);

    /**
     * {@link java.sql.CallableStatement#registerOutParameter(int, int, int)}.
     *
     * @param parameterIndex パラメータインデックス
     * @param sqlType {@link java.sql.Types}
     * @param scale 小数点以下の桁数(0以上であること)
     */
    void registerOutParameter(int parameterIndex, int sqlType, int scale);

    /**
     * {@link java.sql.CallableStatement#getObject(int)}.
     *
     * @param parameterIndex パラメータインデックス
     * @return パラメータインデックスに対応する値(値がnullの場合はnull);
     */
    Object getObject(int parameterIndex);

    /**
     * {@link java.sql.CallableStatement#getString(int)}.
     *
     * @param parameterIndex パラメータインデックス
     * @return パラメータインデックスに対応する値(値がnullの場合はnull)
     */
    String getString(int parameterIndex);

    /**
     * {@link java.sql.CallableStatement#getBigDecimal(int)}.
     *
     * @param parameterIndex パラメータインデックス
     * @return パラメータインデックスに対応する値(値がnullの場合はnull)
     */
    BigDecimal getBigDecimal(int parameterIndex);

    /**
     * {@link java.sql.CallableStatement#getInt(int)}.
     *
     * @param parameterIndex パラメータインデックス
     * @return パラメータインデックスに対応する値(値がnullの場合はnull);
     */
    Integer getInteger(int parameterIndex);

    /**
     * {@link java.sql.CallableStatement#getInt(int)}.
     *
     * @param parameterIndex パラメータインデックス
     * @return パラメータインデックスに対応する値(値がnullの場合はnull);
     */
    Long getLong(int parameterIndex);

    /**
     * {@link java.sql.CallableStatement#getShort(int)}.
     *
     * @param parameterIndex パラメータインデックス
     * @return パラメータインデックスに対応する値(値がnullの場合はnull);
     */
    Short getShort(int parameterIndex);

    /**
     * {@link java.sql.CallableStatement#getDate(int)}.
     *
     * @param parameterIndex パラメータインデックス
     * @return パラメータインデックスに対応する値(値がnullの場合はnull);
     */
    Date getDate(int parameterIndex);

    /**
     * {@link java.sql.CallableStatement#getTime(int)}.
     *
     * @param parameterIndex パラメータインデックス
     * @return パラメータインデックスに対応する値(値がnullの場合はnull);
     */
    Time getTime(int parameterIndex);

    /**
     * {@link java.sql.CallableStatement#getTimestamp(int)} (int)}.
     *
     * @param parameterIndex パラメータインデックス
     * @return パラメータインデックスに対応する値(値がnullの場合はnull);
     */
    Timestamp getTimestamp(int parameterIndex);

    /**
     * {@link java.sql.CallableStatement#getBoolean(int)}.
     *
     * @param parameterIndex パラメータインデックス
     * @return パラメータインデックスに対応する値(値がnullの場合はnull);
     */
    Boolean getBoolean(int parameterIndex);

    /**
     * {@link java.sql.CallableStatement#getByte(int)}.
     *
     * @param parameterIndex パラメータインデックス
     * @return パラメータインデックスに対応する値(値がnullの場合はnull);
     */
    byte[] getBytes(int parameterIndex);

    /**
     * {@link java.sql.CallableStatement#getBlob(int)}.
     *
     * @param parameterIndex パラメータインデックス
     * @return パラメータインデックスに対応する値(値がnullの場合はnull);
     */
    Blob getBlob(int parameterIndex);

    /**
     * {@link java.sql.CallableStatement#getClob(int)}.
     *
     * @param parameterIndex パラメータインデックス
     * @return パラメータインデックスに対応する値(値がnullの場合はnull);
     */
    Clob getClob(int parameterIndex);

}

