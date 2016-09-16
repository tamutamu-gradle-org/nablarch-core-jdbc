package nablarch.core.db.cache.statement;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;

import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.db.statement.SqlRow;

/**
 * 変更可能な値を変更から保護する{@link SqlResultSet}サブクラス。
 * 元の{@link SqlResultSet}の値をコピーして生成される。
 * また、変更可能な値に対して読み出し要求が発生した場合、値をコピーして返却する。
 * これにより元の値が変更されることを防ぐ。
 * <p/>
 * {@link SqlResultSet}インスタンスは変更可能であるため、そのままキャッシュ値として
 * 使用することができない。キャッシュした{@link SqlResultSet}を変更から保護するために
 * 本クラスを使用する。
 *
 * @author T.Kawasaki
 */
class ImmutableSqlResultSet extends SqlResultSet {

    /**
     * コンストラクタ。
     * @param original コピー元の{@link SqlResultSet}
     */
    ImmutableSqlResultSet(SqlResultSet original) {
        super(original.size());
        for (SqlRow origRow : original) {
            add(new ImmutableSqlRow(origRow));
        }
    }

    /**
     * 変更可能な値を変更から保護する{@link SqlRow}サブクラス。
     * 元の{@link SqlRow}の値をコピーして生成される。
     * また、変更可能な値に対して読み出し要求が発生した場合、値をコピーして返却する。
     * これにより元の値が変更されることを防ぐ。
     */
    static class ImmutableSqlRow extends SqlRow {

        /**
         * コンストラクタ。
         *
         * @param orig コピー元の{@link SqlRow}
         */
        ImmutableSqlRow(SqlRow orig) {
            super(orig);
        }

        /**
         * {@inheritDoc}
         * 変更可能な値が要求された場合は、そのコピーを返却する。
         */
        @Override
        public Object get(Object key) {

            Object value = super.get(key);
            if (value instanceof Timestamp) {
                return copy((Timestamp) value);
            }
            if (value instanceof Date) {
                return copy((Date) value);
            }
            if (value instanceof byte[]) {
                return copy((byte[]) value);
            }
            return value;
        }

        /** {@inheritDoc} */
        @Override
        public Date getDate(String colName) {
            Date orig = super.getDate(colName);
            return copy(orig);
        }

        /** {@inheritDoc} */
        @Override
        public Timestamp getTimestamp(String colName) {
            Timestamp orig = super.getTimestamp(colName);
            return copy(orig);
        }

        /** {@inheritDoc} */
        @Override
        public byte[] getBytes(String colName) {
            byte[] orig = super.getBytes(colName);
            return copy(orig);
        }

        /**
         * オブジェクトのコピーを行う。
         *
         * @param orig 元となるオブジェクト
         * @return コピーされたオブジェクト
         */
        private Date copy(Date orig) {
            if (orig == null) {
                return null;
            }
            return new Date(orig.getTime());
        }

        /**
         * オブジェクトのコピーを行う。
         *
         * @param orig 元となるオブジェクト
         * @return コピーされたオブジェクト
         */
        private Timestamp copy(Timestamp orig) {
            if (orig == null) {
                return null;
            }
            return new Timestamp(orig.getTime());
        }

        /**
         * オブジェクトのコピーを行う。
         *
         * @param orig 元となるオブジェクト
         * @return コピーされたオブジェクト
         */
        private byte[] copy(byte[] orig) {
            if (orig == null) {
                return null;
            }
            return Arrays.copyOf(orig, orig.length);
        }

    }

}
