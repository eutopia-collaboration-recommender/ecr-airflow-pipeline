CREATE OR REPLACE FUNCTION
    `collaboration-recommender`.AIRFLOW.IS_AUTHOR_INITIALS_SUBSET(name1 STRING, name2 STRING)
    RETURNS BOOL
    LANGUAGE js AS """
  function getInitials(name) {
    return name.split(' ')
               .filter(word => word.length > 0) // To remove any extra spaces
               .map(word => word[0].toUpperCase())
               .join('');
  }

  let initials1 = getInitials(name1);
  let initials2 = getInitials(name2);

  function isSubset(set1, set2) {
    for (let i = 0; i < set1.length; i++) {
      if (set2.indexOf(set1[i]) === -1) {
        return false;
      }
    }
    return true;
  }

  return isSubset(initials1, initials2) || isSubset(initials2, initials1);
""";
