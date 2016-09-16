package nablarch.core.db.statement.autoproperty;

import nablarch.core.util.annotation.Published;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * データベース更新時にフィールドの値にシステム日時を設定する事を表すアノテーション。<br>
 * 詳細は、{@link CurrentDateTimeAnnotationHandler}を参照。
 *
 * @author Hisaaki Sioiri
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Published
public @interface CurrentDateTime {

    /**
     * 日時フォーマット。<br>
     * フィールドの型が、{@link String}または{@link Integer}、{@link Long}の場合のみ有効。<br>
     * 指定するフォーマットは、{@link java.text.SimpleDateFormat}の日付フォーマットに準拠する。
     */
    String format() default "";
}
