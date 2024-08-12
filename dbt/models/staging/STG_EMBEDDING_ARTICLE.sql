WITH REF_STG_CROSSREF_ARTICLE AS (SELECT * FROM {{ ref('STG_CROSSREF_ARTICLE') }}),
     REF_STG_CROSSREF_ARTICLE_REFERENCE AS (SELECT * FROM {{ ref('STG_CROSSREF_ARTICLE_REFERENCE') }})
SELECT DISTINCT A.ARTICLE_DOI
              , CONCAT('query:',
                       '\nTitle:', IFNULL(A.ARTICLE_TITLE, '/'),
                       '\nShort Title:', IFNULL(A.ARTICLE_SHORT_TITLE, '/'),
                       '\nSubtitle:', IFNULL(A.ARTICLE_SUBTITLE, '/'),
                       '\nAbstract:', IFNULL(A.ARTICLE_ABSTRACT, '/'),
                       '\nReferences:', IFNULL(AR.REFERENCES, '/')
                ) AS EMBEDDING_INPUT
              , CONCAT(CASE WHEN A.ARTICLE_TITLE IS NOT NULL THEN CONCAT(A.ARTICLE_TITLE, '\n') ELSE '' END,
                       CASE
                           WHEN A.ARTICLE_SHORT_TITLE IS NOT NULL THEN CONCAT(A.ARTICLE_SHORT_TITLE, '\n')
                           ELSE '' END,
                       CASE
                           WHEN A.ARTICLE_SUBTITLE IS NOT NULL THEN CONCAT(A.ARTICLE_SUBTITLE, '\n')
                           ELSE '' END,
                       CASE
                           WHEN A.ARTICLE_ABSTRACT IS NOT NULL THEN CONCAT(A.ARTICLE_ABSTRACT, '\n')
                           ELSE '' END
                ) AS LANGUAGE_INPUT
FROM REF_STG_CROSSREF_ARTICLE A
         LEFT JOIN REF_STG_CROSSREF_ARTICLE_REFERENCE AR ON A.ARTICLE_DOI = AR.ARTICLE_DOI
WHERE A.IS_EUTOPIA_AFFILIATED_ARTICLE
