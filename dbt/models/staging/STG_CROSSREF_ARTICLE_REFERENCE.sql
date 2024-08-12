WITH REF_STG_CROSSREF_ARTICLE_REFERENCE_METADATA AS (SELECT *
                                                     FROM {{ ref('STG_CROSSREF_ARTICLE_REFERENCE_METADATA') }})
        ,
     COMBINED_REFERENCES AS (SELECT ARTICLE_DOI,
                                    ARRAY_TO_STRING([
                                                        IF(REFERENCE_ARTICLE_DOI IS NULL OR REFERENCE_ARTICLE_DOI = '',
                                                           NULL, CONCAT('DOI:', REFERENCE_ARTICLE_DOI)),
                                                        IF(REFERENCE_ARTICLE_ISSN IS NULL OR
                                                           REFERENCE_ARTICLE_ISSN = '', NULL,
                                                           CONCAT('issn:', REFERENCE_ARTICLE_ISSN)),
                                                        IF(REFERENCE_ARTICLE_ISSUE IS NULL OR
                                                           REFERENCE_ARTICLE_ISSUE = '', NULL,
                                                           CONCAT('Issue:', REFERENCE_ARTICLE_ISSUE)),
                                                        IF(REFERENCE_ARTICLE_KEY IS NULL OR REFERENCE_ARTICLE_KEY = '',
                                                           NULL, CONCAT('Key:', REFERENCE_ARTICLE_KEY)),
                                                        IF(REFERENCE_ARTICLE_SERIES_TITLE IS NULL OR
                                                           REFERENCE_ARTICLE_SERIES_TITLE = '', NULL,
                                                           CONCAT('Series title:', REFERENCE_ARTICLE_SERIES_TITLE)),
                                                        IF(REFERENCE_ARTICLE_ISBN_TYPE IS NULL OR
                                                           REFERENCE_ARTICLE_ISBN_TYPE = '', NULL,
                                                           CONCAT('ISBN type:', REFERENCE_ARTICLE_ISBN_TYPE)),
                                                        IF(REFERENCE_ARTICLE_DOI_ASSERTED_BY IS NULL OR
                                                           REFERENCE_ARTICLE_DOI_ASSERTED_BY = '', NULL,
                                                           CONCAT('DOI asserted by:', REFERENCE_ARTICLE_DOI_ASSERTED_BY)),
                                                        IF(REFERENCE_ARTICLE_FIRST_PAGE IS NULL OR
                                                           REFERENCE_ARTICLE_FIRST_PAGE = '', NULL,
                                                           CONCAT('First page:', REFERENCE_ARTICLE_FIRST_PAGE)),
                                                        IF(REFERENCE_ARTICLE_ISBN IS NULL OR
                                                           REFERENCE_ARTICLE_ISBN = '', NULL,
                                                           CONCAT('ISBN:', REFERENCE_ARTICLE_ISBN)),
                                                        IF(REFERENCE_ARTICLE_COMPONENT IS NULL OR
                                                           REFERENCE_ARTICLE_COMPONENT = '', NULL,
                                                           CONCAT('Component:', REFERENCE_ARTICLE_COMPONENT)),
                                                        IF(REFERENCE_ARTICLE_ARTICLE_TITLE IS NULL OR
                                                           REFERENCE_ARTICLE_ARTICLE_TITLE = '', NULL,
                                                           CONCAT('Article title:', REFERENCE_ARTICLE_ARTICLE_TITLE)),
                                                        IF(REFERENCE_ARTICLE_VOLUME_TITLE IS NULL OR
                                                           REFERENCE_ARTICLE_VOLUME_TITLE = '', NULL,
                                                           CONCAT('Volume title:', REFERENCE_ARTICLE_VOLUME_TITLE)),
                                                        IF(REFERENCE_ARTICLE_VOLUME IS NULL OR
                                                           REFERENCE_ARTICLE_VOLUME = '', NULL,
                                                           CONCAT('Volume:', REFERENCE_ARTICLE_VOLUME)),
                                                        IF(REFERENCE_ARTICLE_AUTHOR IS NULL OR
                                                           REFERENCE_ARTICLE_AUTHOR = '', NULL,
                                                           CONCAT('Author:', REFERENCE_ARTICLE_AUTHOR)),
                                                        IF(REFERENCE_ARTICLE_YEAR IS NULL OR
                                                           REFERENCE_ARTICLE_YEAR = '', NULL,
                                                           CONCAT('Year:', REFERENCE_ARTICLE_YEAR)),
                                                        IF(REFERENCE_ARTICLE_UNSTRUCTURED IS NULL OR
                                                           REFERENCE_ARTICLE_UNSTRUCTURED = '', NULL,
                                                           CONCAT('Unstructured:', REFERENCE_ARTICLE_UNSTRUCTURED)),
                                                        IF(REFERENCE_ARTICLE_EDITION IS NULL OR
                                                           REFERENCE_ARTICLE_EDITION = '', NULL,
                                                           CONCAT('Edition:', REFERENCE_ARTICLE_EDITION)),
                                                        IF(REFERENCE_ARTICLE_JOURNAL_TITLE IS NULL OR
                                                           REFERENCE_ARTICLE_JOURNAL_TITLE = '', NULL,
                                                           CONCAT('Journaltitle:', REFERENCE_ARTICLE_JOURNAL_TITLE)),
                                                        IF(REFERENCE_ARTICLE_ISSN_TYPE IS NULL OR
                                                           REFERENCE_ARTICLE_ISSN_TYPE = '', NULL,
                                                           CONCAT('ISSN type:', REFERENCE_ARTICLE_ISSN_TYPE))
                                                        ], ', ') AS REFERENCE_TEXT
                             FROM REF_STG_CROSSREF_ARTICLE_REFERENCE_METADATA),
     FINAL_COMBINED_TEXT
         AS (SELECT ARTICLE_DOI,
                    STRING_AGG(REFERENCE_TEXT, '\n') AS REFERENCES
             FROM COMBINED_REFERENCES
             GROUP BY ARTICLE_DOI)
SELECT ARTICLE_DOI,
       REFERENCES
FROM FINAL_COMBINED_TEXT
