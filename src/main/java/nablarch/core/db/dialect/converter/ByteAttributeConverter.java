package nablarch.core.db.dialect.converter;

/**
 * {@link Byte}をデータベースとの間で入出力するために変換するクラス。
 *
 * @author ryo asato
 */
public class ByteAttributeConverter implements AttributeConverter<Byte> {

    /**
     * 以下の型への変換をサポートする。
     *
     * <ul>
     *     <li>{@link Byte}</li>
     * </ul>
     *
     * 上記に以外の型への変換はサポートしないため{@link IllegalArgumentException}を送出する。
     */
    @Override
    public <DB> DB convertToDatabase(Byte javaAttribute, Class<DB> databaseType) {
        if (databaseType.isAssignableFrom(Byte.class)) {
            return databaseType.cast(javaAttribute);
        }
        throw new IllegalArgumentException("unsupported database type:"
                + databaseType.getName());
    }

    /**
     * 以下の型からの変換をサポートする。
     *
     * <ul>
     *     <li>{@link Byte}</li>
     * </ul>
     *
     * 上記に以外の型からの変換はサポートしないため{@link IllegalArgumentException}を送出する。
     * なお、{@code null}は変換せずに{@code null}を返却する。
     */
    @Override
    public Byte convertFromDatabase(Object databaseAttribute) {
        if (databaseAttribute == null) {
            return null;
        } else if (databaseAttribute instanceof Byte) {
            return Byte.class.cast(databaseAttribute);
        }
        throw new IllegalArgumentException(
                "unsupported data type:" + databaseAttribute.getClass()
                        .getName() + ", value:" + databaseAttribute);
    }

    /**
     * プリミティブ({@code byte})を変換するクラス。
     * <p>
     * このクラスでは、データベースから変換する値がnullの場合に、{@code false}に置き換えて返す。
     */
    public static class Primitive implements AttributeConverter<Byte> {

        /** 委譲先の{@link AttributeConverter}。 */
        private final ByteAttributeConverter converter = new ByteAttributeConverter();

        @Override
        public <DB> DB convertToDatabase(final Byte javaAttribute, final Class<DB> databaseType) {
            return converter.convertToDatabase(javaAttribute, databaseType);
        }

        @Override
        public Byte convertFromDatabase(final Object databaseAttribute) {
            Byte aByte = converter.convertFromDatabase(databaseAttribute);
            if (aByte == null) {
                return (byte) 0x00;
            }
            return aByte;
        }
    }
}
