{% macro extract_doi_from_url(input_string) %}

-- This macro extracts a DOI from a given string (URL or plain DOI) using BigQuery's REGEXP_EXTRACT function.
-- Adjusted the pattern to '10\.\d{4,9}[-._;()/:A-Z0-9]*' to handle more flexible input formats.

REGEXP_EXTRACT
({{ input_string }}, r'10\.\d{4,9}/[-._;()/:A-Za-z0-9]+')


{% endmacro %}