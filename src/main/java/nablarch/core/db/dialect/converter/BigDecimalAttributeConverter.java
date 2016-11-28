package nablarch.core.db.dialect.converter;

import java.math.BigDecimal;

/**
 * {@link BigDecimal}をデータベースとの間で入出力するために変換するクラス。
 *
 * @author siosio
 */
public class BigDecimalAttributeConverter implements AttributeConverter<BigDecimal> {

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

    @Override
    public BigDecimal convertFromDatabase(final Object databaseAttribute) {
        if (databaseAttribute == null) {
            return null;
        } else if (databaseAttribute instanceof BigDecimal) {
            return (BigDecimal) databaseAttribute;
        } else if (databaseAttribute instanceof Integer) {
            return BigDecimal.valueOf((Integer) databaseAttribute);
        } else if (databaseAttribute instanceof Long) {
            return BigDecimal.valueOf((Long) databaseAttribute);
        } else if (databaseAttribute instanceof Short) {
            return BigDecimal.valueOf((Short) databaseAttribute);
        } else if (databaseAttribute instanceof String) {
            return new BigDecimal((String) databaseAttribute);
        }
        throw new IllegalArgumentException(
                "unsupported data type:" + databaseAttribute.getClass()
                                                            .getName() + ", value:" + databaseAttribute);
    }
}
