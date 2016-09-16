SQL1 =
update user
  set comment = '-- 削除されないコメント', b = '-- hogehoge', --  削除されるコメント
       a       = ?  -- 'aaa'

