SELECT CASE WHEN COUNT(*) > 0
  THEN raise_error(CONCAT('Bronze has unexpected reader group grants: ', TO_JSON(collect_set(grantee))))
  END
FROM bronze.information_schema.object_privileges
WHERE object_type = 'CATALOG'
  AND grantee IN ('sg-dbplat-standard-readers', 'sg-dbplat-pii-readers');
