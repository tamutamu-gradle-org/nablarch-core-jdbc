package nablarch.core.db.statement.autoproperty;

import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;

import nablarch.core.date.SystemTimeProvider;
import nablarch.core.util.StringUtil;


/**
 * CurrentDateTimeが設定されているフィールドにシステム日時を設定するクラス。<br>
 *
 * @author Hisaaki Sioiri
 */
public class CurrentDateTimeAnnotationHandler extends FieldAnnotationHandlerSupport {

    /** 現在日時取得Util */
    private SystemTimeProvider dateProvider;

    /** デフォルトの日付フォーマット形式 */
    private String dateFormat;

    /**
     * CurrentDateTimeが設定されているフィールドの値にシステム日時を設定する。<br>
     * システム日付は、{@link SystemTimeProvider}から取得を行う。<br>
     * システム日付は、下記のルールでフィールドに設定される。<br>
     * <pre>
     * 1.{@link Date}の場合
     *   {@link SystemTimeProvider#getDate()}を{@link Date}に変換して設定する。
     * 2.{@link Time}の場合
     *   {@link SystemTimeProvider#getDate()}を{@link Time}に変換して設定する。
     * 3.{@link Timestamp}の場合
     *   {@link SystemTimeProvider#getDate()}を{@link Timestamp}に変換して設定する。
     * 4.{@link String}、{@link Integer}(プリミティブ型を含む)、{@link Long}(プリミティブ型を含む)の場合
     *   {@link SystemTimeProvider#getDate()}をCurrentDateTime#format()でフォーマットしそれぞれの型に型変換し設定する。
     *   formatが設定されていない場合は、{@link #setDateFormat}で設定されたデフォルトフォーマットでフォーマットを行う。
     * </pre>
     *
     * @param obj 対象のオブジェクト
     */
    public void handle(Object obj) {

        final List<FieldHolder<CurrentDateTime>> fieldHolders = getFieldList(obj, CurrentDateTime.class);

        if (fieldHolders.isEmpty()) {
            return;
        }

        java.util.Date date = dateProvider.getDate();
        try {
            for (FieldHolder<CurrentDateTime> fieldHolder : fieldHolders) {
                final Field field = fieldHolder.getField();
                final CurrentDateTime dateTime = fieldHolder.getAnnotation();
                final Class<?> type = field.getType();
                if (type == String.class || type == Integer.class || type == int.class || type == Long.class || type == long.class) {
                    String format = dateTime.format();
                    String dateStr;
                    if (StringUtil.isNullOrEmpty(format)) {
                        format = dateFormat;
                    }
                    SimpleDateFormat df = new SimpleDateFormat(format);
                    dateStr = df.format(date);
                    if (type == String.class) {
                        field.set(obj, dateStr);
                    } else if (type == Integer.class || type == int.class) {
                        field.set(obj, Integer.valueOf(dateStr));
                    } else {
                        field.set(obj, Long.valueOf(dateStr));
                    }
                } else if (type == Date.class) {
                    field.set(obj, new Date(date.getTime()));
                } else if (type == Time.class) {
                    field.set(obj, new Time(date.getTime()));
                } else if (type == Timestamp.class) {
                    field.set(obj, new Timestamp(date.getTime()));
                } else {
                    throw new RuntimeException("data type is unsupported. data type:" + type.getName());
                }
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException("field access error.", e);
        }
    }

    /**
     * 現在日時取得コンポーネントを設定する。
     *
     * @param dateProvider 現在日時取得コンポーネント
     */
    public void setDateProvider(SystemTimeProvider dateProvider) {
        this.dateProvider = dateProvider;
    }

    /**
     * デフォルトフォーマット。
     *
     * @param dateFormat 日付のフォーマット形式
     */
    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }
}
