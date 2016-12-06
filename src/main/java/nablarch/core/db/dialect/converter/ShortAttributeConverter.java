package nablarch.core.db.dialect.converter;

import java.math.BigDecimal;

/**
 * {@link Short}をデータベースとの間で入出力するために変換するクラス。
 *
 * @author siosio
 */
public class ShortAttributeConverter implements AttributeConverter<Short> {

    /**
     * 以下の型への変換をサポートする。
     *
     * <ul>
     *     <li>{@link Short}</li>
     *     <li>{@link Integer}</li>
     *     <li>{@link Long}</li>
     *     <li>{@link BigDecimal}</li>
     *     <li>{@link String}</li>
     * </ul>
     *
     * 上記に以外の型への変換はサポートしないため{@link IllegalArgumentException}を送出する。
     * また、{@link null}もサポートしない。
     */
    @SuppressWarnings("unchecked")
    @Override
    public <DB> DB convertToDatabase(final Short javaAttribute, final Class<DB> databaseType) {
        if (databaseType.isAssignableFrom(Short.class)) {
            return databaseType.cast(javaAttribute);
        } else if (databaseType.isAssignableFrom(Integer.class)) {
            return databaseType.cast(javaAttribute.intValue());
        } else if (databaseType.isAssignableFrom(Long.class)) {
            return databaseType.cast(javaAttribute.longValue());
        } else if (databaseType.isAssignableFrom(BigDecimal.class)) {
            return (DB) BigDecimal.valueOf(javaAttribute);
        } else if (databaseType.isAssignableFrom(String.class)) {
            return (DB) String.valueOf(javaAttribute);
        }
        throw new IllegalArgumentException("unsupported database type:"
                + databaseType.getName());
    }

    /**
     * 以下の型からの変換をサポートする。
     *
     * <ul>
     *     <li>{@link Number}</li>
     *     <li>{@link String}</li>
     * </ul>
     *
     * 上記に以外の型からの変換はサポートしないため{@link IllegalArgumentException}を送出する。
     * なお、{@code null}は変換せずに{@code null}を返却する。
     */
    @Override
    public Short convertFromDatabase(final Object databaseAttribute) {
        if (databaseAttribute == null) {
            return null;
        } else if (databaseAttribute instanceof Short) {
            return Short.class.cast(databaseAttribute);
        } else if (databaseAttribute instanceof BigDecimal) {
            return ((BigDecimal) databaseAttribute).shortValueExact();
        } else if (databaseAttribute instanceof Number) {
            return Short.valueOf(databaseAttribute.toString());
        } else if (databaseAttribute instanceof String) {
            return Short.valueOf((String) databaseAttribute);
        }
        throw new IllegalArgumentException(
                "unsupported data type:" + databaseAttribute.getClass()
                                                            .getName() + ", value:" + databaseAttribute);
    }
    
    /**
     * プリミティブ({@code short})を変換するクラス。
     * <p>
     * このクラスでは、データベースから変換する値がnullの場合に、{@code 0}に置き換えて返す。
     */
    public static class Primitive implements AttributeConverter<Short> {

        /** 委譲先の{@link AttributeConverter}。 */
        private final ShortAttributeConverter converter = new ShortAttributeConverter();

        @Override
        public <DB> DB convertToDatabase(final Short javaAttribute, final Class<DB> databaseType) {
            return converter.convertToDatabase(javaAttribute, databaseType);
        }

        @Override
        public Short convertFromDatabase(final Object databaseAttribute) {
            final Short aShort = converter.convertFromDatabase(databaseAttribute);
            if (aShort == null) {
                return 0;
            }
            return aShort;
        }
    }
}
