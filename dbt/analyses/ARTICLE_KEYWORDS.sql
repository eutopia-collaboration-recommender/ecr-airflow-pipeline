INSERT INTO `ANALYTICS.ARTICLE_KEYWORDS`
    (DOI, LIST_OF_KEYWORDS)
SELECT ARTICLE_DOI AS DOI, ML_GENERATE_TEXT_LLM_RESULT AS LIST_OF_KEYWORDS
FROM
    ML.GENERATE_TEXT(
            MODEL `collaboration-recommender`.ANALYTICS.MODEL_TEXT_GENERATION_GEMINI_1_0_PRO,
            (SELECT CONCAT(
                            'Generate exactly 5 keywords that best describe the following text and return them in a list format exactly like the following example: ["keyword1", "keyword2", "keyword3", "keyword4", "keyword5"]. Do not include any additional characters, do not return output as a code block or add any titles or similar. The output should start with a `[` and end with a `]`. In-between there should be 5 keywords in double quotes separated by comma. Here is the text:',
                            SUBSTR(E.EMBEDDING_INPUT, 1, 32766 - 453)) AS PROMPT,
                    E.ARTICLE_DOI
             FROM DBT_DEV.EMBEDDING_ARTICLE E
                      LEFT JOIN `ANALYTICS.ARTICLE_KEYWORDS` K ON E.ARTICLE_DOI = K.DOI
             WHERE K.DOI IS NULL
                LIMIT 10000),
            STRUCT (
                0.2 AS TEMPERATURE,
                512 AS MAX_OUTPUT_TOKENS,
                TRUE AS FLATTEN_JSON_OUTPUT));

