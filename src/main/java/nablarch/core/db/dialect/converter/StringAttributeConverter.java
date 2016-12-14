package nablarch.core.db.dialect.converter;

import nablarch.core.util.StringUtil;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * {@link String}をデータベースとの間で入出力するために変換するクラス。
 *
 * @author siosio
 */
public class StringAttributeConverter implements AttributeConverter<String> {

    /**
     * 以下の型への変換をサポートする。
     *
     * <ul>
     *     <li>{@link String}</li>
     *     <li>{@link BigDecimal}</li>
     *     <li>{@link Long}</li>
     *     <li>{@link Integer}</li>
     *     <li>{@link Short}</li>
     *     <li>{@link java.sql.Timestamp}</li>
     *     <li>{@link java.sql.Date}</li>
     * </ul>
     *
     * 上記に以外の型への変換はサポートしないため{@link IllegalArgumentException}を送出する。
     */
    @Override
    public <DB> DB convertToDatabase(final String javaAttribute, final Class<DB> databaseType) {
        if (databaseType.isAssignableFrom(String.class)) {
            return databaseType.cast(javaAttribute);
        } else if (databaseType.isAssignableFrom(Short.class)) {
            return (DB) Short.valueOf(javaAttribute);
        } else if (databaseType.isAssignableFrom(Integer.class)) {
            return (DB) Integer.valueOf(javaAttribute);
        } else if (databaseType.isAssignableFrom(Long.class)) {
            return (DB) Long.valueOf(javaAttribute);
        } else if (databaseType.isAssignableFrom(BigDecimal.class)) {
            return (DB) new BigDecimal(javaAttribute);
        } else if (databaseType.isAssignableFrom(Date.class)) {
            return (DB) Date.valueOf(javaAttribute);
        } else if (databaseType.isAssignableFrom(Timestamp.class)) {
            return (DB) Timestamp.valueOf(javaAttribute);
        };
        throw new IllegalArgumentException("unsupported database type:"
                + databaseType.getName());
    }

    /**
     * 変換対象の値の文字列表現を返す。
     * 
     * 変換対象が{@code null}の場合は、{@code null}を返す。
     */
    @Override
    public String convertFromDatabase(final Object databaseAttribute) {
        return databaseAttribute == null ? null : StringUtil.toString(databaseAttribute);
    }
}
