package nablarch.core.db.statement.autoproperty;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nablarch.core.cache.StaticDataCache;
import nablarch.core.db.statement.AutoPropertyHandler;
import nablarch.core.repository.IgnoreProperty;
import nablarch.core.util.annotation.Published;


/**
 * フィールドのアノテーション情報を元に値を設定するクラスをサポートするクラス。
 *
 * @author Hisaaki Sioiri
 */
@Published(tag = "architect")
public abstract class FieldAnnotationHandlerSupport implements AutoPropertyHandler {

    /**
     * フィールドアノテーション保持クラスを設定する。
     *
     * @param fieldAnnotationCache フィールドアノテーション保持クラス
     * @deprecated 本プロパティは、仕様変更に伴い使用しなくなりました。(値を設定しても、意味が無い)
     */
    @IgnoreProperty("フィールドではなくプロパティアクセスするよう仕様変更を行ったため本プロパティは廃止")
    @Deprecated
    public void setFieldAnnotationCache(
            StaticDataCache<Map<String, Map<String, ?>>> fieldAnnotationCache) {
    }

    /**
     * 指定されたアノテーションが設定されているフィールド情報を取得する。<br>
     * 指定されたオブジェクトに、指定されたアノテーションが設定されているフィールドが存在しない場合は、空のリストを返す。
     *
     * @param obj 対象のオブジェクト
     * @param annotationType アノテーション
     * @return 指定されたアノテーションが設定されているフィールドの情報
     */
    protected <T extends Annotation> List<FieldHolder<T>> getFieldList(
            final Object obj, final Class<T> annotationType) {
        return findFieldsWithAnnotation(obj.getClass(), annotationType);
    }

    /**
     * 指定されたアノテーションが設定されたフィールドを取得する。
     * <p>
     * 親クラスが存在する場合は、再帰的に親クラスのフィールドも対象とする。
     *
     * @param clazz クラス
     * @param annotationType アノテーションクラス
     * @param <T> アノテーションの型
     * @return 指定されたアノテーションが設定されたフィールドのリスト
     */
    private <T extends Annotation> List<FieldHolder<T>> findFieldsWithAnnotation(
            final Class<?> clazz, final Class<T> annotationType) {
        final List<FieldHolder<T>> result = new ArrayList<FieldHolder<T>>();
        final Field[] fields = clazz.getDeclaredFields();

        for (Field field : fields) {
            final T annotation = field.getAnnotation(annotationType);
            if (annotation != null) {
                field.setAccessible(true);

                result.add(new FieldHolder<T>(field, annotation));
            }
        }
        final Class<?> superclass = clazz.getSuperclass();
        if (superclass != null) {
            result.addAll(findFieldsWithAnnotation(superclass, annotationType));
        }
        return result;
    }

    /**
     * フィールド情報を保持するクラス。
     *
     * このクラスでは、フィールドとフィールドに設定されたアノテーションの情報を保持する。
     *
     * @param <T> アノテーションの型
     */
    @Published(tag = "architect")
    public static class FieldHolder<T extends Annotation> {

        /** フィールド */
        private final Field field;

        /** アノテーション */
        private final T annotation;

        /**
         * フィールドとアノテーションを元に{@code FieldHolder}を構築する。
         *
         * @param field フィールド
         * @param annotation アノテーション
         */
        public FieldHolder(final Field field, final T annotation) {
            this.field = field;
            this.annotation = annotation;
        }

        /**
         * フィールドを取得する。
         *
         * @return フィールド
         */
        public Field getField() {
            return field;
        }

        /**
         * アノテーションを取得する。
         *
         * @return アノテーション
         */
        public T getAnnotation() {
            return annotation;
        }
    }
}

