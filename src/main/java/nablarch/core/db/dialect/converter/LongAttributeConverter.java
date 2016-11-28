package nablarch.core.db.dialect.converter;

import java.math.BigDecimal;

/**
 * {@link Long}をデータベースとの間で入出力するために変換するクラス。
 *
 * @author siosio
 */
public class LongAttributeConverter implements AttributeConverter<Long> {

    @SuppressWarnings("unchecked")
    @Override
    public <DB> DB convertToDatabase(final Long javaAttribute, final Class<DB> databaseType) {
        if (databaseType.isAssignableFrom(Long.class)) {
            return databaseType.cast(javaAttribute);
        } else if (databaseType.isAssignableFrom(BigDecimal.class)) {
            return (DB) BigDecimal.valueOf(javaAttribute);
        } else if (databaseType.isAssignableFrom(String.class)) {
            return (DB) String.valueOf(javaAttribute);
        }
        throw new IllegalArgumentException("unsupported database type:"
                + databaseType.getName());
    }

    @Override
    public Long convertFromDatabase(final Object databaseAttribute) {
        if (databaseAttribute == null) {
            return null;
        } else if (databaseAttribute instanceof Long) {
            return Long.class.cast(databaseAttribute);
        } else if (databaseAttribute instanceof BigDecimal) {
            return ((BigDecimal) databaseAttribute).longValueExact();
        } else if (databaseAttribute instanceof Number) {
            return ((Number) databaseAttribute).longValue();
        } else if (databaseAttribute instanceof String) {
            return Long.valueOf((String) databaseAttribute);
        }
        throw new IllegalArgumentException(
                "unsupported data type:" + databaseAttribute.getClass()
                                                            .getName() + ", value:" + databaseAttribute);
    }
    
    /**
     * プリミティブ({@code long})を変換するクラス。
     * <p>
     * このクラスでは、データベースから変換する値がnullの場合に、{@code 0}に置き換えて返す。
     */
    public static class Primitive implements AttributeConverter<Long> {

        /** 委譲先の{@link AttributeConverter}。 */
        private final LongAttributeConverter converter = new LongAttributeConverter();

        @Override
        public <DB> DB convertToDatabase(final Long javaAttribute, final Class<DB> databaseType) {
            return converter.convertToDatabase(javaAttribute, databaseType);
        }

        @Override
        public Long convertFromDatabase(final Object databaseAttribute) {
            final Long aLong = converter.convertFromDatabase(databaseAttribute);
            if (aLong == null) {
                return 0L;
            }
            return aLong;
        }
    }
}
