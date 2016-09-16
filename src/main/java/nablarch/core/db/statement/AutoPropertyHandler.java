package nablarch.core.db.statement;

import nablarch.core.util.annotation.Published;

/**
 * オブジェクトの自動設定項目のフィールドに値を設定するインタフェース。<br>
 * オブジェクトの事前変換処理が必要な場合には、本インターフェースの実装クラスを追加し、
 * 実処理実行前にexecuteメソッドを呼び出すこと。
 * オブジェクトに対する
 *
 * @author Hisaaki Sioiri
 */
@Published(tag = "architect")
public interface AutoPropertyHandler {

    /**
     * 指定されたオブジェクトのフィールドの値に自動設定値を設定する。
     *
     * @param obj オブジェクト
     */
    void handle(Object obj);
}
