package nablarch.core.db.statement.autoproperty;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import nablarch.core.ThreadContext;
import nablarch.core.date.SystemTimeProvider;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * CurrentDateTimeAnnotationHandlerのテストクラス。
 *
 * @author Hisaaki Sioiri
 */
public class CurrentDateTimeAnnotationHandlerTest {

    /** テスト用の日付 */
    private static Date testDate;


    @BeforeClass
    public static void beforeClass() throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddhhmmssSSS");
        testDate = format.parse("20100802102712345");
    }


    @Test
    public void handle() throws Exception {
        CurrentDateTimeAnnotationHandler handler = new CurrentDateTimeAnnotationHandler();
        handler.setDateFormat("yyyyMMdd");
        handler.setDateProvider(new SystemTimeProvider() {
            public Date getDate() {
                return testDate;
            }

            public Timestamp getTimestamp() {
                return new Timestamp(getDate().getTime());
            }
        });

        ThreadContext.setUserId("user_id");

        Entity entity = new Entity();
        handler.handle(entity);

        Field userId1 = entity.getClass().getSuperclass().getSuperclass().getDeclaredField("userId1");
        userId1.setAccessible(true);
        Field ymdLng = entity.getClass().getSuperclass().getDeclaredField("ymdLng");
        ymdLng.setAccessible(true);

        assertNull("CurrentDateTimeアノテーション以外は設定されない。", userId1.get(entity));

        assertEquals("2010-08-02", entity.date.toString());
        assertEquals("10:27:12", entity.time.toString());
        assertEquals("2010-08-02 10:27:12.345", entity.timestamp.toString());

        assertEquals("20100802", entity.defaultFormatDate);
        assertEquals("2010", entity.year);
        assertEquals("20100802", entity.dateStr);
        assertEquals("102712", entity.timeStr);
        assertEquals("20100802102712345", entity.timestampStr);
        assertEquals(2010, entity.yearInt);
        assertEquals(201008L, entity.ymLng);
        assertEquals(201008, (int) entity.ymInt);
        assertEquals("親クラスでもOK", 20100802L, ymdLng.get(entity));
        assertEquals(0, entity.cnt);

        ErrEntity err = new ErrEntity();
        try {
            handler.handle(err);
            fail("");
        } catch (Exception e) {
            assertEquals("data type is unsupported. data type:java.util.List", e.getMessage());
        }


        // アノテーションがついていないクラスの場合もOK
        NoAnnotationClass noAnnotationClass = new NoAnnotationClass();
        handler.handle(noAnnotationClass);
        assertNull(noAnnotationClass.str);
    }

    private static class Base {
        @UserId
        private String userId1;
    }

    private static class Base2 extends Base {
        @CurrentDateTime(format = "yyyyMMdd")
        private Long ymdLng;
    }

    private static class Entity extends Base2 {
        @UserId
        private String userId1;
        @UserId
        private String userId2;
        private String userId3;

        @CurrentDateTime
        private java.sql.Date date;
        @CurrentDateTime
        private java.sql.Time time;
        @CurrentDateTime
        private Timestamp timestamp;
        @CurrentDateTime
        private String defaultFormatDate;
        @CurrentDateTime(format = "yyyy")
        private String year;
        @CurrentDateTime(format = "yyyyMMdd")
        private String dateStr;
        @CurrentDateTime(format = "hhmmss")
        private String timeStr;
        @CurrentDateTime(format = "yyyyMMddhhmmssSSS")
        private String timestampStr;

        @CurrentDateTime(format = "yyyy")
        private int yearInt;
        @CurrentDateTime(format = "yyyyMM")
        private long ymLng;
        @CurrentDateTime(format = "yyyyMM")
        private Integer ymInt;

        private int cnt;
    }

    private static class ErrEntity {
        @CurrentDateTime
        private List errDate;
    }

    private static class NoAnnotationClass {
        private String str;
    }
}

