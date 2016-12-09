package nablarch.core.db.dialect;

import java.sql.Types;

/**
 * {@link java.sql.Types}とJavaクラスのマッピングを管理するインタフェース。
 * @author ryo asato
 */
public interface SqlTypeConverter {

    /**
     * SQL型に対応するJavaクラスを取得する。
     * @param sqlType {@link Types}
     * @return Javaのクラス
     */
    Class convertToJavaClass(int sqlType);
}
