package nablarch.core.db.statement.autoproperty;

import nablarch.core.util.annotation.Published;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * データベース更新時にフィールド情報にユーザIDを設定する事を表すアノテーション。
 *
 * @author Hisaaki Sioiri
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Published
public @interface UserId {
}
