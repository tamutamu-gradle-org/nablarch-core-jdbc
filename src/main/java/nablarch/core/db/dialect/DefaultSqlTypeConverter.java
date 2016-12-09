package nablarch.core.db.dialect;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link SqlTypeConverter}のデフォルト実装
 * jdbc3.0で定義されているSQL型のうち、対応する型が明確なものを対象としている。
 * 変換後の型は{@link PreparedStatement}のsetXXで変換されるSQL型との対応に準じている。
 *
 * @author ryo asato
 */
public class DefaultSqlTypeConverter implements SqlTypeConverter {

    /**
     * SQL型に対応するJavaクラスのマッピング定義。
     */
    private static final Map<Integer, Class> CLASS_CONVERTER_MAP;

    static {
        final Map<Integer, Class> classConverterMap = new HashMap<Integer, Class>();
        classConverterMap.put(Types.BIT, Boolean.class);
        classConverterMap.put(Types.TINYINT, Byte.class);
        classConverterMap.put(Types.SMALLINT, Short.class);
        classConverterMap.put(Types.INTEGER, Integer.class);
        classConverterMap.put(Types.BIGINT, Long.class);
        classConverterMap.put(Types.FLOAT, Double.class);
        classConverterMap.put(Types.REAL, Float.class);
        classConverterMap.put(Types.DOUBLE, Double.class);
        classConverterMap.put(Types.NUMERIC, BigDecimal.class);
        classConverterMap.put(Types.DECIMAL, BigDecimal.class);
        classConverterMap.put(Types.CHAR, String.class);
        classConverterMap.put(Types.VARCHAR, String.class);
        classConverterMap.put(Types.LONGVARCHAR, String.class);
        classConverterMap.put(Types.DATE, java.sql.Date.class);
        classConverterMap.put(Types.TIME, java.sql.Time.class);
        classConverterMap.put(Types.TIMESTAMP, java.sql.Timestamp.class);
        classConverterMap.put(Types.BINARY, byte[].class);
        classConverterMap.put(Types.VARBINARY, byte[].class);
        classConverterMap.put(Types.LONGVARBINARY, byte[].class);
        classConverterMap.put(Types.STRUCT, Struct.class);
        classConverterMap.put(Types.ARRAY, Array.class);
        classConverterMap.put(Types.BLOB, Blob.class);
        classConverterMap.put(Types.CLOB, Clob.class);
        classConverterMap.put(Types.REF, Ref.class);
        classConverterMap.put(Types.DATALINK, URL.class);
        classConverterMap.put(Types.BOOLEAN, Boolean.class);

        CLASS_CONVERTER_MAP = Collections.unmodifiableMap(classConverterMap);
    }

    @Override
    public Class convertToJavaClass(int sqlType) {
        Class converted = CLASS_CONVERTER_MAP.get(sqlType);
        if (converted == null) {
            throw new IllegalArgumentException("unsupported sqlType: " + sqlType);
        }
        return converted;
    }
}
