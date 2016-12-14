package nablarch.core.db.dialect.converter;

import java.math.BigDecimal;

/**
 * {@link Boolean}をデータベースとの間で入出力するために変換するクラス。
 *
 * @author siosio
 */
public class BooleanAttributeConverter implements AttributeConverter<Boolean> {

    /**
     * 以下の型への変換をサポートする。
     *
     * <ul>
     *     <li>{@link Boolean}</li>
     *     <li>{@link BigDecimal}</li>
     *     <li>{@link Integer}</li>
     *     <li>{@link Long}</li>
     *     <li>{@link Short}</li>
     * </ul>
     *
     * 数値型に変換する場合、変換対象の値がtrueの場合は1を表すその型のオブジェクトを、
     * falseの場合は0を表すその型のオブジェクトを返す。
     *
     * 上記に以外の型への変換はサポートしないため{@link IllegalArgumentException}を送出する。
     */
    @SuppressWarnings("unchecked")
    @Override
    public <DB> DB convertToDatabase(final Boolean javaAttribute, final Class<DB> databaseType) {
        if (databaseType.isAssignableFrom(Boolean.class)) {
            return databaseType.cast(javaAttribute);
        } else if (databaseType.isAssignableFrom(BigDecimal.class)) {
            return (DB) (javaAttribute ? BigDecimal.ONE : BigDecimal.ZERO);
        } else if (databaseType.isAssignableFrom(Integer.class)) {
            return (DB) (javaAttribute ? Integer.valueOf(1) : Integer.valueOf(0));
        } else if (databaseType.isAssignableFrom(Long.class)) {
            return (DB) (javaAttribute ? Long.valueOf(1L) : Long.valueOf(0L));
        } else if (databaseType.isAssignableFrom(Short.class)) {
            return (DB) (javaAttribute ? Short.valueOf("1") : Short.valueOf("0"));
        }
        throw new IllegalArgumentException("unsupported database type:"
                + databaseType.getName());
    }

    /**
     * 以下のルールに従い{@link Boolean}に変換する。
     * <p>
     * 1. 変換対象が{@link String}の場合<br />
     * "1" or "on" or "true"の場合(大文字小文字は区別しない)に{@code true}に変換する。
     * <p>
     * 2. 変換対象が{@link Number}五感の場合<br />
     * 0の場合{@code false}、それ以外の場合{@code true}に変換する。
     * <p>
     * 3. 変換対象が{@link Boolean}の場合<br />
     * そのまま値を返す。
     * <p>
     * 4. 上記以外の場合<br />
     * サポートしない。({@link IllegalArgumentException}を送出する)
     * 
     * なお、{@code null}は変換せずに{@code null}を返却する。
     */
    @Override
    public Boolean convertFromDatabase(final Object databaseAttribute) {
        if (databaseAttribute == null) {
            return null;
        } else if (databaseAttribute instanceof Boolean) {
            return (Boolean) databaseAttribute;
        } else if (databaseAttribute instanceof String) {
            final String str = (String) databaseAttribute;
            return str.equals("1")
                    || str.equalsIgnoreCase("on")
                    || str.equalsIgnoreCase("true");
        } else if (databaseAttribute instanceof Number) {
            return ((Number) databaseAttribute).intValue() != 0;
        }
        throw new IllegalArgumentException(
                "unsupported data type:" + databaseAttribute.getClass()
                                                            .getName() + ", value:" + databaseAttribute);
    }

    /**
     * プリミティブ({@code boolean})を変換するクラス。
     * <p>
     * このクラスでは、データベースから変換する値がnullの場合に、{@code false}に置き換えて返す。
     */
    public static class Primitive implements AttributeConverter<Boolean> {

        /** 委譲先の{@link AttributeConverter}。 */
        private final BooleanAttributeConverter converter = new BooleanAttributeConverter();

        @Override
        public <DB> DB convertToDatabase(final Boolean javaAttribute, final Class<DB> databaseType) {
            return converter.convertToDatabase(javaAttribute, databaseType);
        }

        @Override
        public Boolean convertFromDatabase(final Object databaseAttribute) {
            final Boolean aBoolean = converter.convertFromDatabase(databaseAttribute);
            if (aBoolean == null) {
                return Boolean.FALSE;
            }
            return aBoolean;
        }
    }
}
