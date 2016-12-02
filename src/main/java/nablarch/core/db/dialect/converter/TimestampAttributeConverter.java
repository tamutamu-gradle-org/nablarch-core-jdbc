package nablarch.core.db.dialect.converter;

import java.sql.Date;
import java.sql.Timestamp;

import nablarch.core.db.util.DbUtil;

/**
 * {@link Integer}をデータベースとの間で入出力するために変換するクラス。
 *
 * @author siosio
 */
public class TimestampAttributeConverter implements AttributeConverter<Timestamp> {

    @SuppressWarnings("unchecked")
    @Override
    public <DB> DB convertToDatabase(final Timestamp javaAttribute, final Class<DB> databaseType) {
        if (databaseType.isAssignableFrom(Timestamp.class)) {
            return databaseType.cast(javaAttribute);
        } else if (databaseType.isAssignableFrom(Date.class)) {
            return (DB) new Date(DbUtil.trimTime(javaAttribute).getTimeInMillis());
        } else if (databaseType.isAssignableFrom(String.class)) {
            return (DB) javaAttribute.toString();
        }
        throw new IllegalArgumentException("unsupported database type:"
                + databaseType.getName());
    }

    @Override
    public Timestamp convertFromDatabase(final Object databaseAttribute) {
        if (databaseAttribute == null) {
            return null;
        } else if (databaseAttribute instanceof Timestamp) {
            return (Timestamp) databaseAttribute;
        } else if (databaseAttribute instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) databaseAttribute).getTime());
        } else if (databaseAttribute instanceof String) {
            return Timestamp.valueOf((String) databaseAttribute);
        }
        throw new IllegalArgumentException("unsupported data type:"
                + databaseAttribute.getClass()
                                   .getName() + ", value:" + databaseAttribute);
    }
}
