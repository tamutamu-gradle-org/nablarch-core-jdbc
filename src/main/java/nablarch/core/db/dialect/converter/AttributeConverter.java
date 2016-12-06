package nablarch.core.db.dialect.converter;

import nablarch.core.util.annotation.Published;

/**
 * 属性値の変換を行うインタフェース。
 *
 * @author siosio
 * @param <T> 属性の型
 */
@Published(tag = "architect")
public interface AttributeConverter<T> {

    /**
     * データベースのデータタイプに対応した値に変換する。
     *
     * @param javaAttribute 変換対象(Java)の値
     * @param databaseType データベースのデータタイプ
     * @param <DB> データタイプのデータ型
     * @return 変換した値
     */
    <DB> DB convertToDatabase(T javaAttribute, Class<DB> databaseType);

    /**
     * Javaのデータタイプに応じた値に変換する。
     * @param databaseAttribute 変換対象(データベース)の値
     * @return 変換した値
     */
    T convertFromDatabase(Object databaseAttribute);
}
