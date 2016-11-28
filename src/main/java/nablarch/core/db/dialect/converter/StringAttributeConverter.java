package nablarch.core.db.dialect.converter;

/**
 * {@link String}をデータベースとの間で入出力するために変換するクラス。
 *
 * @author siosio
 */
public class StringAttributeConverter implements AttributeConverter<String> {

    @Override
    public <DB> DB convertToDatabase(final String javaAttribute, final Class<DB> databaseType) {
        if (databaseType.isAssignableFrom(String.class)) {
            return databaseType.cast(javaAttribute);
        }
        throw new IllegalArgumentException("unsupported database type:"
                + databaseType.getName());
    }

    @Override
    public String convertFromDatabase(final Object databaseAttribute) {
        return databaseAttribute == null ? null : String.valueOf(databaseAttribute);
    }
}
