package nablarch.core.db.dialect.converter;

import java.math.BigDecimal;

/**
 * {@link BigDecimal}をデータベースとの間で入出力するために変換するクラス。
 *
 * @author siosio
 */
public class BigDecimalAttributeConverter implements AttributeConverter<BigDecimal> {

    /**
     * 以下の型への変換をサポートする。
     * 
     * <ul>
     *     <li>{@link BigDecimal}</li>
     *     <li>{@link Integer}</li>
     *     <li>{@link Long}</li>
     *     <li>{@link Short}</li>
     *     <li>{@link String}</li>
     * </ul>
     * 
     * 上記に以外の型への変換はサポートしないため{@link IllegalArgumentException}を送出する。
     * また、{@link null}もサポートしない。
     */
    @SuppressWarnings("unchecked")
    @Override
    public <DB> DB convertToDatabase(final BigDecimal javaAttribute, final Class<DB> databaseType) {
        if (databaseType.isAssignableFrom(BigDecimal.class)) {
            return databaseType.cast(javaAttribute);
        } else if (databaseType.isAssignableFrom(Integer.class)) {
            return (DB) Integer.valueOf(javaAttribute.intValueExact());
        } else if (databaseType.isAssignableFrom(Long.class)) {
            return (DB) Long.valueOf(javaAttribute.longValueExact());
        } else if (databaseType.isAssignableFrom(Short.class)) {
            return (DB) Short.valueOf(javaAttribute.shortValueExact());
        } else if (databaseType.isAssignableFrom(String.class)) {
            return (DB) javaAttribute.toPlainString();
        }
        throw new IllegalArgumentException("unsupported database type:"
                + databaseType.getName());
    }

    /**
     * 以下の型からの変換をサポートする。
     * 
     * <ul>
     *     <li>{@link BigDecimal}</li>
     *     <li>{@link Integer}</li>
     *     <li>{@link Long}</li>
     *     <li>{@link Short}</li>
     *     <li>{@link String}</li>
     * </ul>
     * 
     * 上記に以外の型からの変換はサポートしないため{@link IllegalArgumentException}を送出する。
     * なお、{@code null}は変換せずに{@code null}を返却する。
     */
    @Override
    public BigDecimal convertFromDatabase(final Object databaseAttribute) {
        if (databaseAttribute == null) {
            return null;
        } else if (databaseAttribute instanceof BigDecimal) {
            return BigDecimal.class.cast(databaseAttribute);
        } else if (databaseAttribute instanceof Number) {
            return new BigDecimal(databaseAttribute.toString());
        } else if (databaseAttribute instanceof String) {
            return new BigDecimal((String) databaseAttribute);
        }
        throw new IllegalArgumentException(
                "unsupported data type:" + databaseAttribute.getClass()
                                                            .getName() + ", value:" + databaseAttribute);
    }
}
