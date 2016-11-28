package nablarch.core.db.dialect.converter;

/**
 * {@link String}をデータベースとの間で入出力するために変換するOracle用のクラス。
 * 
 * この実装は、{@link StringAttributeConverter}は空文字列の扱いが異なり、
 * 空文字列をデータベースに出力する際には{@code null}に変換する。
 *
 * @author siosio
 */
public class OracleStringAttributeConverter extends StringAttributeConverter {

    /**
     * {@link String}の値をデータベースの型へと変換する。
     * <p>
     * Oracleデータベースには、空文字列を挿入することはできない(null)に自動変換されるため、
     * 本実装では事前に空文字列をnullに変換し使用する。
     * <p>
     * データベースに対応する型が{@link String}以外の場合は、{@link IllegalArgumentException}を送出する。
     *
     * @param javaAttribute 変換対象(Java)の値
     * @param databaseType データベースのデータタイプ
     * @param <DB> DBの型
     * @return 変換後の値
     */
    @Override
    public <DB> DB convertToDatabase(final String javaAttribute, final Class<DB> databaseType) {
        final DB dbAttribute = super.convertToDatabase(javaAttribute, databaseType);
        if (isEmptyString(dbAttribute)) {
            return null;
        } else {
            return dbAttribute;
        }
    }

    /**
     * データベースに出力する値が空文字列かどうか判定する。
     *
     * @param dbAttribute データベースに出力する値
     * @param <DB> データベースの型
     * @return 空文字列の場合{@code true}
     */
    private <DB> boolean isEmptyString(final DB dbAttribute) {
        return dbAttribute instanceof String && ((String) dbAttribute).isEmpty();
    }
}
