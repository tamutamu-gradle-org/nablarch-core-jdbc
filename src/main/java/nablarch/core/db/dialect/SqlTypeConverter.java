package nablarch.core.db.dialect;

/**
 * {@link java.sql.Types}とJavaクラスのマッピングを管理するインタフェース。
 * @author ryo asato
 */
public interface SqlTypeConverter {

    /**
     * SQL型に対応するJavaクラスを取得する。
     * @param sqlType {@link java.sql.Types}
     * @return Javaのクラス
     */
    Class<?> convertToJavaClass(int sqlType);
}
