SELECT CASE WHEN COUNT(*) > 0
  THEN raise_error(CONCAT('Bronze has unexpected reader group grants: ', TO_JSON(collect_set(Principal))))
  END
FROM (SHOW GRANTS ON CATALOG bronze) t
WHERE t.Principal IN ('sg-dbplat-standard-readers', 'sg-dbplat-pii-readers');
