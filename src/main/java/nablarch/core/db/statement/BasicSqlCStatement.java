package nablarch.core.db.statement;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.Date;

import nablarch.core.db.DbAccessException;

/**
 * {@link CallableStatement}のラッパークラス。
 *
 * @author hisaaki sioiri
 * @see CallableStatement
 */
public class BasicSqlCStatement extends BasicSqlPStatement implements SqlCStatement {

    /** {@link CallableStatement} */
    private final CallableStatement statement;

    /**
     * コンストラクタ。
     *
     * @param sql SQL文
     * @param statement {@link CallableStatement}
     */
    public BasicSqlCStatement(String sql, CallableStatement statement) {
        super(sql, statement);
        this.statement = statement;
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) {
        try {
            statement.registerOutParameter(parameterIndex, sqlType);
            paramHolder.add(parameterIndex + "(out param)", "type:" + sqlType);
        } catch (SQLException e) {
            throw new DbAccessException(
                    MessageFormat.format("failed to registerOutParameter({0}, {1})", parameterIndex, sqlType), e);
        }
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) {
        try {
            statement.registerOutParameter(parameterIndex, sqlType, scale);
            paramHolder.add(parameterIndex + "(out param)", "type:" + sqlType + ", scale:" + scale);
        } catch (SQLException e) {
            throw new DbAccessException(
                    MessageFormat.format("failed to registerOutParameter({0}, {1}, {2})",
                            parameterIndex, sqlType, scale),
                    e);
        }
    }

    @Override
    public Object getObject(int parameterIndex) {
        try {
            return statement.getObject(parameterIndex);
        } catch (SQLException e) {
            throw new DbAccessException(MessageFormat.format("failed to getObject({0})", parameterIndex), e);
        }
    }

    @Override
    public String getString(int parameterIndex) {
        try {
            return statement.getString(parameterIndex);
        } catch (SQLException e) {
            throw new DbAccessException(MessageFormat.format("failed to getString({0})", parameterIndex), e);
        }
    }

    @Override
    public Integer getInteger(int parameterIndex) {
        try {
            final Object o = statement.getObject(parameterIndex);
            if (o instanceof Integer) {
                return Integer.class.cast(o);
            }
            return o == null ? null : Integer.valueOf(o.toString());
        } catch (SQLException e) {
            throw new DbAccessException(MessageFormat.format("failed to getInteger({0})", parameterIndex), e);
        }
    }

    @Override
    public Long getLong(int parameterIndex) {
        try {
            final Object o = statement.getObject(parameterIndex);

            if (o instanceof Long) {
                return Long.class.cast(o);
            }
            return o == null ? null : Long.valueOf(o.toString());
        } catch (SQLException e) {
            throw new DbAccessException(MessageFormat.format("failed to getLong({0})", parameterIndex), e);
        }
    }

    @Override
    public Short getShort(int parameterIndex) {
        try {
            final Object o = statement.getObject(parameterIndex);
            if (o instanceof Short) {
                return Short.class.cast(o);
            }
            return o == null ? null : Short.valueOf(o.toString());
        } catch (SQLException e) {
            throw new DbAccessException(MessageFormat.format("failed to getShort({0})", parameterIndex), e);
        }
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) {
        try {
            return statement.getBigDecimal(parameterIndex);
        } catch (SQLException e) {
            throw new DbAccessException(MessageFormat.format("failed to getBigDecimal({0})", parameterIndex), e);
        }
    }

    @Override
    public Date getDate(int parameterIndex) {
        try {
            return statement.getDate(parameterIndex);
        } catch (SQLException e) {
            throw new DbAccessException(MessageFormat.format("failed to getDate({0})", parameterIndex), e);
        }
    }

    @Override
    public Time getTime(int parameterIndex) {
        try {
            return statement.getTime(parameterIndex);
        } catch (SQLException e) {
            throw new DbAccessException(MessageFormat.format("failed to getTime({0})", parameterIndex), e);
        }
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) {
        try {
            return statement.getTimestamp(parameterIndex);
        } catch (SQLException e) {
            throw new DbAccessException(MessageFormat.format("failed to getTimestamp({0})", parameterIndex), e);
        }
    }

    @Override
    public Boolean getBoolean(int parameterIndex) {
        try {
            final boolean result = statement.getBoolean(parameterIndex);
            if (statement.wasNull()) {
                return null;
            }
            return result;
        } catch (SQLException e) {
            throw new DbAccessException(MessageFormat.format("failed to getBoolean({0})", parameterIndex), e);
        }
    }

    @Override
    public byte[] getBytes(int parameterIndex) {
        try {
            return statement.getBytes(parameterIndex);
        } catch (SQLException e) {
            throw new DbAccessException(MessageFormat.format("failed to getBytes({0})", parameterIndex), e);
        }
    }

    @Override
    public Blob getBlob(int parameterIndex) {
        try {
            return statement.getBlob(parameterIndex);
        } catch (SQLException e) {
            throw new DbAccessException(MessageFormat.format("failed to getBlob({0})", parameterIndex), e);
        }
    }

    @Override
    public Clob getClob(int parameterIndex) {
        try {
            return statement.getClob(parameterIndex);
        } catch (SQLException e) {
            throw new DbAccessException(MessageFormat.format("failed to getClob({0})", parameterIndex), e);
        }
    }
}

