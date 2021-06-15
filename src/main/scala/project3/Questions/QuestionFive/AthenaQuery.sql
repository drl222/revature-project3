SELECT url_host_name, count(url_path) as url_path_count, date_trunc('month', fetch_time) as month_time
FROM "ccindex"."ccindex"
WHERE crawl LIKE '%CC-MAIN-2020%'
  AND subset = 'warc'
  AND url_host_tld = 'com'
  AND fetch_status = 200
  AND url_protocol = 'https'
  AND content_languages = 'eng'
  AND (lower(url_path) LIKE '%job%' or lower(url_path) LIKE '%career%')
  AND (lower(url_path) LIKE '%programmer%' or
       lower(url_path) LIKE '%engineer%' or
       lower(url_path) LIKE '%software%' or
       lower(url_path) LIKE '%computer%' or
       lower(url_path) LIKE '%developer%' or
       lower(url_path) LIKE '%java%' or
       lower(url_path) LIKE '%python%' or
       lower(url_path) LIKE '%scala%' or
       lower(url_path) LIKE '%tech support%' or
       lower(url_path) LIKE '%network%' or
       lower(url_path) LIKE '%analyst%' or
       lower(url_path) LIKE '%big data%' or
       lower(url_path) LIKE '%it specialist%' or
       lower(url_path) LIKE '%technician%' or
       lower(url_path) LIKE '%information technology%'
      )
GROUP BY url_host_name, date_trunc('month', fetch_time) order by url_host_name limit 2000000;