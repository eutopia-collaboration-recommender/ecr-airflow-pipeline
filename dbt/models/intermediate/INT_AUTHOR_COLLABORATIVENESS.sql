/*
Author collaborativeness is measured within the EUTOPIA publication subspace and counts the number of articles
published by the author, the number of external collaborations, and the collaboration rate. The collaboration rate
is defined as the ratio of external collaborations to the total number of publications. The percentile of the
collaboration rate is also calculated to determine the author's position in the distribution of collaboration rates.
*/
WITH REF_ER_COLLABORATION AS (SELECT *
                              FROM {{ ref('ER_COLLABORATION') }}),
     INITIAL_DATA AS (SELECT AUTHOR_SID,
                             COUNT(ARTICLE_SID)                                               AS PUBLICATIONS,
                             COUNT(DISTINCT IF(IS_EXTERNAL_COLLABORATION, ARTICLE_SID, NULL)) AS EXTERNAL_COLLABORATIONS
                      FROM REF_ER_COLLABORATION
                      GROUP BY AUTHOR_SID),
     FILTERED_DATA AS (SELECT AUTHOR_SID,
                              PUBLICATIONS,
                              EXTERNAL_COLLABORATIONS,
                              -- Calculate the collaboration rate as the ratio of external collaborations to the total
                              -- number of publications
                              SAFE_DIVIDE(EXTERNAL_COLLABORATIONS, PUBLICATIONS) AS COLLABORATION_RATE
                       FROM INITIAL_DATA
                       -- Filter out authors with less than 10 publications and non-negative external collaborations
                       WHERE PUBLICATIONS > 0
                         AND EXTERNAL_COLLABORATIONS >= 0
                         AND PUBLICATIONS >= 10),
     CLIPPED_DATA AS (SELECT DISTINCT AUTHOR_SID,
                                      PUBLICATIONS,
                                      EXTERNAL_COLLABORATIONS,
                                      -- Clip the collaboration rate to be between 0.01 and 0.99
                                      LEAST(GREATEST(COLLABORATION_RATE, 0.01), 0.99) AS COLLABORATION_RATE
                      FROM FILTERED_DATA),
     PERCENTILE_DATA AS (SELECT AUTHOR_SID,
                                PUBLICATIONS,
                                EXTERNAL_COLLABORATIONS,
                                COLLABORATION_RATE,
                                -- Calculate the percentile of the collaboration rate
                                RANK() OVER (ORDER BY COLLABORATION_RATE) / COUNT(*) OVER () AS PERCENTILE
                         FROM CLIPPED_DATA)
SELECT AUTHOR_SID,
       PUBLICATIONS            AS AUTHOR_PUBLICATIONS,
       EXTERNAL_COLLABORATIONS AS AUTHOR_EXTERNAL_COLLABORATIONS,
       COLLABORATION_RATE      AS AUTHOR_COLLABORATION_RATE,
       PERCENTILE              AS AUTHOR_COLLABORATION_RATE_PERCENTILE,
       -- Determine if the author is more collaborative than the median author
       CASE
           WHEN PERCENTILE >= 0.5 THEN TRUE
           ELSE FALSE
           END                 AS IS_AUTHOR_MORE_COLLABORATIVE
FROM PERCENTILE_DATA