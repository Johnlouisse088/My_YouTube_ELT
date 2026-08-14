from datetime import timedelta, datetime

def transform_data(row):
    duration_td = parse_duration(row["Duration"])  # # Output: 0:00:50
    """
        STAGING                            CORE
        "PT50S" (VARCHAR)      →          00:00:50  (TIME)         
        """
    row["Duration"] = (datetime.min + duration_td).time() # Convert timedelta into time  # Output: 00:00:50
    row["Video_Type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal" # Shorts or Normal # this field use for analytic, like use can query meaningful filter
    return row # Return the transformed row
    """ Example:
    Staging:
    {
        "Video_ID": "f7y2XikE7sY",
        "Duration": "PT50S"
    }
    Core/Transformed
    {
        "Video_ID": "f7y2XikE7sY",
        "Duration": time(0, 0, 50),
        "Video_Type": "Shorts"
    }
    """


def parse_duration(duration_str):
    duration_str = duration_str.replace("P", "").replace("T", "")  # Example: PT50S -> 50S
    components = ["D", "H", "M", "S"]
    values = {"D": 0,
              "H": 0,
              "M": 0,
              "S": 0}

    for component in components:
        if component in duration_str: # Only the letter 'S' satisfied the condition
            value, duration_str = duration_str.split(component)  # Example output: 50S -> ["50", ""] (take note the value or the argument in split fuction will not be included in the output
            values[component] = int(value) # takes the number you extracted and stores it in the values dictionary # Output: "D": 0, "H": 0, "M": 0, "S": 50}

    total_duration = timedelta(
        days=values["D"], hours=values["H"], minutes=values["M"], seconds=values["S"]
    )
    """
       total_duration = timedelta(
           days=0,
           hours=0,
           minutes=0,
           seconds=50
       )
       """

    return total_duration  # Output: 0:00:50
