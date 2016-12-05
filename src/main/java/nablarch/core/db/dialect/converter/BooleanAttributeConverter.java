package nablarch.core.db.dialect.converter;

/**
 * {@link Boolean}をデータベースとの間で入出力するために変換するクラス。
 *
 * @author siosio
 */
public class BooleanAttributeConverter implements AttributeConverter<Boolean> {

    @Override
    public <DB> DB convertToDatabase(final Boolean javaAttribute, final Class<DB> databaseType) {
        if (databaseType.isAssignableFrom(Boolean.class)) {
            return databaseType.cast(javaAttribute);
        }
        throw new IllegalArgumentException("unsupported database type:"
                + databaseType.getName());
    }

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
