CREATE OR REPLACE FUNCTION AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(data JSON)
    RETURNS DATE
    LANGUAGE js AS """
  try {
    if (data == null) {
        return new Date('2030-01-01');  // Default date if data is null
    }

    if (data.hasOwnProperty('date-time')) {
      return new Date(data['date-time'])
    }

    if (data.hasOwnProperty('timestamp')) {
      return new Date(data['timestamp'])
    }

    if (data.hasOwnProperty('date-parts')) {
      let dateParts = data['date-parts'][0];
      if (dateParts.length === 3) {
        let [year, month, day] = dateParts;
        return new Date(Date.UTC(year, month - 1, day))
      }
      if (dateParts.length === 2) {
        let [year, month] = dateParts;
        return new Date(Date.UTC(year, month - 1))
      }
      if (dateParts.length === 1) {
        let [year] = dateParts;
        return new Date(Date.UTC(year))
      }
    }

    return new Date('2030-01-01');  // Default date if no valid date is found
  } catch (e) {
    return new Date('2030-01-01');  // Default date in case of error
  }
""";