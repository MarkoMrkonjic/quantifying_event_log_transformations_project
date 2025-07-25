import pandas as pd

def get_csv_metrics(file_path):
    """
    params:
    - file_path: CSV file path
    """

    # define column names for mandatory columns
    case_column = "case_id"
    activity_column = "activity"
    timestamp_column = "timestamp"
    
    df = pd.read_csv(file_path)
    
    # validate form
    required_columns = [case_column, activity_column, timestamp_column]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Spalten fehlen: {missing}")
    
    # declare emtpy case_ids to SYSTEM
    df[case_column] = df[case_column].fillna('SYSTEM').astype(str)
    df.loc[df[case_column].str.strip() == '', case_column] = 'SYSTEM'

    df[timestamp_column] = pd.to_datetime(df[timestamp_column])
    
    # calculate basic statistics
    num_events = len(df)
    num_activities = df[activity_column].nunique()
    num_cases = df[case_column].nunique()
    
    # calculate processing time
    time_min = df[timestamp_column].min()
    time_max = df[timestamp_column].max()
    time_range_hours = (time_max - time_min).total_seconds() / 3600
    
    # calculate event attributes (and resources)
    attribute_columns = [col for col in df.columns if col not in required_columns]
    num_event_attributes = len(attribute_columns)
    
    # calculate multi-value fields
    delimiters = [';', '|', ',', '&', '+']
    multi_value_attributes = []
    
    for col in attribute_columns:
        # check if column has delimiters
        col_values = df[col].astype(str)
        for delimiter in delimiters:
            if col_values.str.contains(f'\\{delimiter}', regex=True, na=False).any():
                multi_value_attributes.append(col)
                break
    
    num_multi_attributes = len(multi_value_attributes)
    avg_events_per_case = num_events / num_cases if num_cases > 0 else 0
    
    # print function to be used if csv_metrics is used alone (not in a quantifier)
    """
    print(f"\nDatei: {file_path}")
    print(f"Metriken:")
    print(f"  Cases: {num_cases}")
    print(f"  Events: {num_events}")
    print(f"  Aktivitäten: {num_activities}")
    print(f"  Event-Attribute: {num_event_attributes} wovon {num_multi_attributes} potenziell Multi-Values sind.")
    print(f"\n  Durchschn. Events/Case: {avg_events_per_case:.2f}")
    print(f"  Zeitspanne: {time_range_hours:.2f} Stunden")
    print("\n")
    """
    
    return { 
        "stats": {
            "num_cases": num_cases,
            "num_events": num_events,
            "avg_events_per_case": avg_events_per_case,
            "num_activities": num_activities,
            "num_event_attributes": num_event_attributes,
            "num_multi_attributes": num_multi_attributes,
            "time_range_hours": time_range_hours
        }
    }