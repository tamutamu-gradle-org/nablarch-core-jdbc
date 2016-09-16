package nablarch.core.db.statement.autoproperty;

import java.lang.annotation.Annotation;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nablarch.core.cache.StaticDataLoader;
import nablarch.core.util.annotation.Published;

/**
 * フィールド情報とフィールドに設定されたアノテーション情報をロードするクラス。<br>
 * 本クラスは、初期化時の一括ロード、インデックスによるデータロードをサポートしない。
 *
 * @author Hisaaki Sioiri
 * @deprecated フィールドではなくプロパティを参照するように機能改修を行ったため、フィールド情報をロードする本クラスは非推奨に変更
 */
@Published(tag = "architect")
@Deprecated
public class FieldAndAnnotationLoader implements StaticDataLoader<Map<String, Map<String, Object>>> {

    /**
     * Objectのフィールド、アノテーション情報をロードする。<br>
     * <br>
     * ロードしたフィールドとアノテーション情報は、下記のオブジェクト形式で返却する。
     * <pre>
     * key：フィールド名
     * value：{@link java.util.Map}
     *       key:"FIELD" value:フィールドのインスタンス({@link java.lang.reflect.Field}
     *       key:"ANNOTAION" value:フィールドに設定されているアノテーションの配列({@link java.lang.annotation.Annotation}
     * </pre>
     *
     * @param id ロード対象オブジェクトのClass
     * @return フィールドとアノテーション情報<br>
     */
    public Map<String, Map<String, Object>> getValue(Object id) {
        Map<String, Map<String, Object>> result = new HashMap<String, Map<String, Object>>();
        Class<?> clazz = (Class<?>) id;
        setFieldInfo(clazz, result);

        while (clazz.getSuperclass() != null) {
            clazz = clazz.getSuperclass();
            setFieldInfo(clazz, result);
        }
        return result;
    }

    /**
     * 指定されたクラスのフィールド情報を設定する。
     *
     * @param clazz クラス
     * @param info 設定対象のMap
     */
    private void setFieldInfo(Class<?> clazz, Map<String, Map<String, Object>> info) {
        Field[] fields = clazz.getDeclaredFields();
        AccessibleObject.setAccessible(fields, true);

        for (Field field : fields) {
            if (!info.containsKey(field.getName())) {
                Map<String, Object> fieldInfo = new HashMap<String, Object>();
                Annotation[] annotations = field.getAnnotations();
                fieldInfo.put("FIELD", field);
                fieldInfo.put("ANNOTATION", annotations);
                info.put(field.getName(), fieldInfo);
            }
        }
    }

    /**
     * インデックスに紐付くデータをロードする(本メソッドは、サポートしない)。
     *
     * @param indexName インデックス名
     * @param key 静的データのキー
     * @return インデックス名、キーに対応するデータのリスト
     */
    public List<Map<String, Map<String, Object>>> getValues(String indexName, Object key) {
        throw new UnsupportedOperationException("FieldAndAnnotationLoader#getValues is unsupported.");
    }

    /**
     * 全てのデータをロードする(本メソッドは、サポートしない)。
     *
     * @return 全てのデータ
     */
    public List<Map<String, Map<String, Object>>> loadAll() {
        throw new UnsupportedOperationException("FieldAndAnnotationLoader#loadAll is unsupported.");
    }

    /**
     * 作成する全てのインデックス名を取得する(本メソッドは、サポートしない)。
     *
     * @return 作成する全てのインデックス名
     */
    public List<String> getIndexNames() {
        throw new UnsupportedOperationException("FieldAndAnnotationLoader#getIndexNames is unsupported.");
    }

    /**
     * 静的データからIDを生成する(本メソッドは、サポートしない)。
     *
     * @param value 静的データ
     * @return 生成したID
     */
    public Object getId(Map<String, Map<String, Object>> value) {
        throw new UnsupportedOperationException("FieldAndAnnotationLoader#generateId is unsupported.");
    }

    /**
     * 静的データからインデックスのキーを生成する(本メソッドは、サポートしない)。
     *
     * @param indexName インデックス名
     * @param value 静的データ
     * @return 生成したインデックスのキー
     */
    public Object generateIndexKey(String indexName, Map<String, Map<String, Object>> value) {
        throw new UnsupportedOperationException("FieldAndAnnotationLoader#generateIndexKey is unsupported.");
    }
}
