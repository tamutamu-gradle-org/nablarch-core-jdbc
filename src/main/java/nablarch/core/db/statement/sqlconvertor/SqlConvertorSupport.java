package nablarch.core.db.statement.sqlconvertor;

import java.util.Map;

import nablarch.core.beans.BeanUtil;
import nablarch.core.beans.BeansException;
import nablarch.core.db.statement.SqlConvertor;
import nablarch.core.util.annotation.Published;

/**
 * SQL文の変換を行うクラスをサポートするクラス。
 * @author Kiyohito Itoh
 */
@Published(tag = "architect")
public abstract class SqlConvertorSupport implements SqlConvertor {
    
    /**
     * バインド変数に対応するフィールドの値を取得する。
     * @param obj 検索条件をもつオブジェクト
     * @param fieldName フィールド名
     * @return バインド変数に対応するフィールドの値
     */
    protected Object getBindValue(Object obj, String fieldName) {
        final boolean isMap = (obj instanceof Map<?, ?>);

        if (isMap) {
            // バインド変数に対応するフィールドの値(Mapのvalue)を取得する。
            return getMapObject((Map<?, ?>) obj, fieldName);
        } else {
            try {
                return BeanUtil.getProperty(obj, fieldName);
            } catch (BeansException e) {
                throw new IllegalArgumentException("failed to get " + fieldName + " property.", e);
            }
        }
    }
    
    /**
     * MapのValue値を取得し返却する。
     *
     * @param map Map
     * @param key 取得対象のキー
     * @return 指定したキー値に対応するValue値
     */
    private Object getMapObject(Map<?, ?> map, String key) {
        if (!map.containsKey(key)) {
            throw new IllegalArgumentException(
                    String.format("there is not sql parameter '%s' in the key of the Map.", key));
        }
        return map.get(key);
    }
}
