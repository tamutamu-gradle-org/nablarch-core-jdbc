package nablarch.core.db.dialect.converter;

import java.sql.Date;
import java.sql.Timestamp;

import nablarch.core.db.util.DbUtil;

/**
 * {@link Date}をデータベースとの間で入出力するために変換するクラス。
 *
 * @author siosio
 */
public class SqlDateAttributeConverter implements AttributeConverter<Date> {

    /**
     * 以下の型への変換をサポートする。
     *
     * <ul>
     *     <li>{@link Date}</li>
     *     <li>{@link Timestamp}</li>
     *     <li>{@link String}</li>
     * </ul>
     *
     * 上記に以外の型への変換はサポートしないため{@link IllegalArgumentException}を送出する。
     * また、{@link null}もサポートしない。
     */
    @SuppressWarnings("unchecked")
    @Override
    public <DB> DB convertToDatabase(final Date javaAttribute, final Class<DB> databaseType) {
        if (databaseType.isAssignableFrom(Date.class)) {
            return databaseType.cast(javaAttribute);
        } else if (databaseType.isAssignableFrom(Timestamp.class)) {
            return (DB) new Timestamp(javaAttribute.getTime());
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
     *     <li>{@link Date}</li>
     *     <li>{@link java.util.Date}</li>
     *     <li>{@link String}</li>
     * </ul>
     *
     * 上記に以外の型からの変換はサポートしないため{@link IllegalArgumentException}を送出する。
     * なお、{@code null}は変換せずに{@code null}を返却する。
     */
    @Override
    public Date convertFromDatabase(final Object databaseAttribute) {
        if (databaseAttribute == null) {
            return null;
        } else if (databaseAttribute instanceof Date) {
            return Date.class.cast(databaseAttribute);
        } else if (databaseAttribute instanceof java.util.Date) {
            return new Date(DbUtil.trimTime((java.util.Date) databaseAttribute).getTimeInMillis());
        } else if (databaseAttribute instanceof String) {
            return Date.valueOf((String) databaseAttribute);
        }
        throw new IllegalArgumentException("unsupported data type:"
                + databaseAttribute.getClass()
                                   .getName() + ", value:" + databaseAttribute);
    }
}
