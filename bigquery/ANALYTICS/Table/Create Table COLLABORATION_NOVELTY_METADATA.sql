CREATE OR REPLACE TABLE `collaboration-recommender`.ANALYTICS.COLLABORATION_NOVELTY_METADATA
(
    ARTICLE_SID                      STRING,
    AUTHOR_SID                       STRING,
    INSTITUTION_SID                  STRING,
    ARTICLE_PUBLICATION_DT           DATE,
    IS_NEW_AUTHOR_COLLABORATION      BOOLEAN,
    IS_NEW_INSTITUTION_COLLABORATION BOOLEAN
);

