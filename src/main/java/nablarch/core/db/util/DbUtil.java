package nablarch.core.db.util;

import java.lang.reflect.Array;
import java.util.Collection;

/**
 * データベースアクセス機能で利用するユーティリティクラス。
 *
 * @author hisaaki sioiri
 */
public final class DbUtil {

    /** コンストラクタ。 */
    private DbUtil() {
    }

    /**
     * オブジェクトが配列または、{@link Collection}か。
     *
     * @param object チェック対象のオブジェクト
     * @return 配列の場合は、true
     */
    public static boolean isArrayObject(Object object) {
        if (object == null) {
            // nullの場合は、配列要素として判断する。
            return true;
        }
        return isCollection(object) || isArray(object);
    }

    /**
     * 指定されたオブジェクトが配列か否か。
     * @param object オブジェクト
     * @return 配列の場合は、true
     */
    private static boolean isArray(Object object) {
        return object.getClass().isArray();
    }

    /**
     * 指定されたオブジェクトが{@link Collection}か否か。
     * @param object オブジェクト
     * @return {@link Collection}の場合は、true
     */
    private static boolean isCollection(Object object) {
        return (object instanceof Collection<?>);
    }

    /**
     * オブジェクトの配列サイズを取得する。<br/>
     * <p/>
     * オブジェクトが配列または、{@link java.util.Collection}以外の場合は、{@link IllegalArgumentException}。<br/>
     * オブジェクトがnullの場合は、0を返却する。
     *
     * @param object オブジェクト
     * @return 配列のサイズ
     */
    public static int getArraySize(Object object) {
        if (!isArrayObject(object)) {
            throw new IllegalArgumentException(String.format(
                    "object type is invalid. valid object type is Array or Collection. object class = [%s]",
                    object.getClass().getName()));
        }
        if (object == null) {
            return 0;
        }
        if (isCollection(object)) {
            return ((Collection<?>) object).size();
        } else {
            return Array.getLength(object);
        }
    }

    /**
     * 配列または、{@link java.util.Collection}オブジェクトから指定された要素の値を取得する。<br/>
     * <p/>
     * オブジェクトが配列または、Collection以外の場合は、{@link IllegalArgumentException}。<br/>
     * オブジェクトがnullの場合は、nullを返却する。
     *
     * @param object オブジェクト(配列または、Collection)
     * @param pos 要素
     * @return 取得した値。(オブジェクトがnullの場合は、null)
     */
    public static Object getArrayValue(Object object, int pos) {
        if (!isArrayObject(object)) {
            throw new IllegalArgumentException(
                    "object type is invalid. valid object type is Array or Collection.");
        }
        if (object == null) {
            return null;
        }
        int size = getArraySize(object);
        if (pos < 0 || pos >= size) {
            throw new IllegalArgumentException(String.format(
                    "specified position is out of range. actual size = [%d], specified position = [%d]",
                    size, pos));
        }
        if (isCollection(object)) {
            Object[] objects = ((Collection<?>) object).toArray();
            return objects[pos];
        } else {
            return Array.get(object, pos);
        }
    }
}
