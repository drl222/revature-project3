SELECT url, warc_filename, warc_record_offset, warc_record_length, warc_segment
FROM "ccindex"."ccindex"
WHERE crawl = 'CC-MAIN-2018-05'
  AND subset = 'warc'
  AND url_host_tld = 'com'
  AND url_host_registered_domain = 'indeed.com'
  AND lower(url_path) LIKE '%jobs%'
  AND lower(url) NOT LIKE '%ca.indeed.com%'
  AND lower(url) LIKE '%indeed.com/cmp/%'
  AND (lower(url_path) LIKE '%programmer%' or
       lower(url_path) LIKE '%engineer%' or
       lower(url_path) LIKE '%software%' or
       lower(url_path) LIKE '%computer%' or
       lower(url_path) LIKE '%developer%' or
       lower(url_path) LIKE '%java%'
      )
LIMIT 20