package nablarch.core.db.dialect;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import nablarch.core.db.dialect.converter.AttributeConverter;
import nablarch.core.db.dialect.converter.BigDecimalAttributeConverter;
import nablarch.core.db.dialect.converter.BooleanAttributeConverter;
import nablarch.core.db.dialect.converter.ByteArrayAttributeConverter;
import nablarch.core.db.dialect.converter.IntegerAttributeConverter;
import nablarch.core.db.dialect.converter.LongAttributeConverter;
import nablarch.core.db.dialect.converter.ShortAttributeConverter;
import nablarch.core.db.dialect.converter.SqlDateAttributeConverter;
import nablarch.core.db.dialect.converter.StringAttributeConverter;
import nablarch.core.db.dialect.converter.TimestampAttributeConverter;
import nablarch.core.db.dialect.converter.UtilDateAttributeConverter;
import nablarch.core.db.statement.ResultSetConvertor;
import nablarch.core.db.statement.SelectOption;
import nablarch.core.util.annotation.Published;

/**
 * デフォルトの{@link Dialect}実装クラス。
 * <p/>
 * 本実装では、全ての方言が無効化される。
 *
 * @author hisaaki sioiri
 */
@Published(tag = "architect")
public class DefaultDialect implements Dialect {

    /** {@link ResultSet}から値を取得するクラス */
    private static final ResultSetConvertor RESULT_SET_CONVERTOR = new DefaultResultSetConvertor();
    
    /**
     * 型変換を行う{@link AttributeConverter}定義。
     */
    private static final Map<Class<?>, AttributeConverter<?>> ATTRIBUTE_CONVERTER_MAP;

    static {
        final Map<Class<?>, AttributeConverter<?>> attributeConverterMap = new HashMap<Class<?>, AttributeConverter<?>>();
        attributeConverterMap.put(String.class, new StringAttributeConverter());
        attributeConverterMap.put(Short.class, new ShortAttributeConverter());
        attributeConverterMap.put(short.class, new ShortAttributeConverter.Primitive());
        attributeConverterMap.put(Integer.class, new IntegerAttributeConverter());
        attributeConverterMap.put(int.class, new IntegerAttributeConverter.Primitive());
        attributeConverterMap.put(Long.class, new LongAttributeConverter());
        attributeConverterMap.put(long.class, new LongAttributeConverter.Primitive());
        attributeConverterMap.put(BigDecimal.class, new BigDecimalAttributeConverter());
        attributeConverterMap.put(java.sql.Date.class, new SqlDateAttributeConverter());
        attributeConverterMap.put(java.util.Date.class, new UtilDateAttributeConverter());
        attributeConverterMap.put(Timestamp.class, new TimestampAttributeConverter());
        attributeConverterMap.put(byte[].class, new ByteArrayAttributeConverter());
        attributeConverterMap.put(Boolean.class, new BooleanAttributeConverter());
        attributeConverterMap.put(boolean.class, new BooleanAttributeConverter.Primitive());
        ATTRIBUTE_CONVERTER_MAP = Collections.unmodifiableMap(attributeConverterMap);
    }

    /**
     * @return {@code false}を返す。
     */
    @Override
    public boolean supportsIdentity() {
        return false;
    }

    /**
     * @return {@code false}を返す。
     */
    @Override
    public boolean supportsSequence() {
        return false;
    }

    /**
     * @return {@code false}を返す。
     */
    @Override
    public boolean supportsOffset() {
        return false;
    }

    /**
     * @return {@code false}を返す。
     */
    @Override
    public boolean isTransactionTimeoutError(SQLException sqlException) {
        return false;
    }

    /**
     * @return {@code false}を返す。
     */
    @Override
    public boolean isDuplicateException(SQLException sqlException) {
        return false;
    }

    /**
     * 全てのカラムを{@link ResultSet#getObject(int)}で取得するコンバータを返す。
     *
     * @return {@inheritDoc}
     */
    @Override
    public ResultSetConvertor getResultSetConvertor() {
        return RESULT_SET_CONVERTOR;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * シーケンス採番はサポートしない。
     *
     * @throws UnsupportedOperationException 呼び出された場合
     */
    @Override
    public String buildSequenceGeneratorSql(final String sequenceName) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("sequence generator is unsupported.");
    }

    /**
     * SQL文を変換せずに返す。
     */
    @Override
    public String convertPaginationSql(String sql, SelectOption selectOption) {
        return sql;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * 以下形式のCOUNT文取得用SQL文に変換する。<br/>
     * {@code SELECT COUNT(*) COUNT_ FROM ('引数のSQL') SUB_}
     */
    @Override
    public String convertCountSql(String sql) {
        return "SELECT COUNT(*) COUNT_ FROM (" + sql + ") SUB_";
    }

    /**
     * {@inheritDoc}
     *
     * デフォルト実装では、本メソッドはサポートしない。
     */
    @Override
    public String getPingSql() {
        throw new UnsupportedOperationException("unsupported getPingSql.");
    }

    /**
     * 全て{@link ResultSet#getObject(int)}で値を取得する{@link ResultSetConvertor}の実装クラス。
     */
    private static class DefaultResultSetConvertor implements ResultSetConvertor {

        @Override
        public Object convert(ResultSet rs, ResultSetMetaData rsmd, int columnIndex) throws SQLException {
            return rs.getObject(columnIndex);
        }

        @Override
        public boolean isConvertible(ResultSetMetaData rsmd, int columnIndex) throws SQLException {
            return true;
        }
    }

    @Override
    public <T, DB> DB convertToDatabase(final T value, final Class<DB> dbType) {
        if (value == null) {
            return null;
        }
        @SuppressWarnings("unchecked")
        final AttributeConverter<T> converter = getAttributeConverter((Class<T>) value.getClass());
        return converter.convertToDatabase(value, dbType);
    }

    @Override
    public <T> T convertFromDatabase(final Object value, final Class<T> javaType) {
        final AttributeConverter<T> converter = getAttributeConverter(javaType);
        return converter.convertFromDatabase(value);
    }

    /**
     * 指定の型をデータベースの入出力で変換するためのコンバータを返す。
     *
     * @param javaType データベースへの入出力対象のクラス
     * @param <T> データベースへの入出力対象の型
     * @return 指定の型を変換するコンバータ
     */
    @SuppressWarnings("unchecked")
    protected <T> AttributeConverter<T> getAttributeConverter(Class<T> javaType) {
        AttributeConverter<T> converter = (AttributeConverter<T>) ATTRIBUTE_CONVERTER_MAP.get(javaType);
        if (converter == null) {
            throw new IllegalStateException("This dialect does not support [" + javaType.getSimpleName() + "] type.");
        }
        return converter;
    }
}

