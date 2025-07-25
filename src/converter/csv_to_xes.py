import pandas as pd
import pm4py
from itertools import product

def csv_to_xes(csv_path: str, xes_path: str,
               case_col='case_id',
               activity_col='activity',
               timestamp_col='timestamp',
               resource_col='resource',
               timestamp_format='%Y-%m-%d %H:%M:%S'):
    """
    params:
    - csv_path: csv file path
    - xes_path: xes output file path
    - case_col: column name for case ids (default: 'case_id')
    - activity_col: column name for activities (default: 'activity') 
    - timestamp_col: column name for timestamps (default: 'timestamp')
    - resource_col: column name for resources (default: 'resource')
    - timestamp_format: Format des Timestamps (default: '%Y-%m-%d %H:%M:%S')
    """

    df = pd.read_csv(csv_path)
    
    expanded_rows = []
    # identify potential multi-value fields
    attr_cols = [c for c in df.columns if c not in [case_col, activity_col, timestamp_col]]
    
    for _, row in df.iterrows():
        # declare empty case_ids to SYSTEM
        raw_case = row.get(case_col)
        if pd.isna(raw_case) or not str(raw_case).strip():
            case_ids = ['SYSTEM']
        else:
            case_ids = [str(raw_case).strip()]
        
        # create lists for existing multi-value fields
        split_values = {}
        for col in attr_cols:
            val = row.get(col)
            if pd.notna(val) and isinstance(val, str) and ';' in val:
                split_values[col] = [v.strip() for v in val.split(';')]
            else:
                split_values[col] = [val]
        
        # duplicate events for multi-value holding rows       
        for case_id, combo in product(case_ids, product(*split_values.values())):
            new_row = row.copy()
            new_row[case_col] = case_id
            for col, v in zip(split_values.keys(), combo):
                new_row[col] = v
            expanded_rows.append(new_row)
    
    # create data frame for newly added rows (events)
    df_expanded = pd.DataFrame(expanded_rows)
    
    # rename columns according to XES-specification
    rename_dict = {
        case_col: 'case:concept:name',
        activity_col: 'concept:name',
        timestamp_col: 'time:timestamp',
        resource_col: 'org:resource'
    }
    df_expanded = df_expanded.rename(columns={k: v for k, v in rename_dict.items() if k in df_expanded.columns})
    
    if 'time:timestamp' in df_expanded.columns:
        df_expanded['time:timestamp'] = pd.to_datetime(
            df_expanded['time:timestamp'], format=timestamp_format, errors='coerce')
    
    if 'case:concept:name' in df_expanded.columns and 'time:timestamp' in df_expanded.columns:
        df_expanded = df_expanded.sort_values(['case:concept:name', 'time:timestamp'])
    
    event_log = pm4py.convert_to_event_log(df_expanded)
    pm4py.write_xes(event_log, xes_path)
    
    return event_log

if __name__ == '__main__':
    csv_to_xes('data/sample_data/csv_sample_simple.csv', 'data/generated_data/roundtrip/xes_from_csv_simple.xes')
