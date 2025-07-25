import pm4py
import pandas as pd
import numpy as np
from datetime import datetime
import json

# helper function to ensure json conformity
def convert_to_json_serializable(obj):
    """
    params:
    - obj: object
    """

    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif pd.isna(obj):
        return None
    else:
        return obj

def xes_to_ocel2(xes_path, ocel_path, 
                 case_object_type='case',
                 resource_object_type='resource',
                 resource_attr='org:resource'):
    """
    params:
    - ocel2_path: ocel2 file path
    - xes_path: xes output file path
    """
    
    log = pm4py.read_xes(xes_path)
    
    df = pm4py.convert_to_dataframe(log)
    
    # initialize OCEL2 structure
    ocel = {
        "objectTypes": [
            {"name": case_object_type, "attributes": []},
            {"name": resource_object_type, "attributes": []}
        ],
        "eventTypes": [],
        "objects": [],
        "events": [],
        "objectRelations": []
    }
    
    event_types = {}
    attribute_types = {}
    
    # identify activities and attribute types
    for _, row in df.iterrows():
        activity = row.get('concept:name', 'unknown')
        
        if activity not in event_types:
            event_types[activity] = set()
        
        # collect attribute types
        for col, val in row.items():
            if col not in ['concept:name', 'case:concept:name', 'time:timestamp', resource_attr]:
                if pd.notna(val):
                    event_types[activity].add(col)
                    # set attribute type based on python type
                    if col not in attribute_types and pd.notna(val):
                        test_val = val
                        if hasattr(test_val, 'isoformat') or isinstance(test_val, pd.Timestamp):
                            attribute_types[col] = "string"  # convert timestamps to string
                        elif isinstance(test_val, bool) or isinstance(test_val, np.bool_):
                            attribute_types[col] = "boolean"
                        elif isinstance(test_val, (int, np.integer)):
                            attribute_types[col] = "integer"
                        elif isinstance(test_val, (float, np.floating)):
                            attribute_types[col] = "float"
                        else:
                            attribute_types[col] = "string"
    
    # create objects from uniquely identified resources
    resources_seen = set()
    if resource_attr in df.columns:
        for resource in df[resource_attr].dropna().unique():
            ocel["objects"].append({
                "id": f"{resource_object_type}_{resource}",
                "type": resource_object_type,
                "attributes": [],
                "relationships": []
            })
            resources_seen.add(resource)
    
    # create objects from each case
    for case_id in df['case:concept:name'].unique():
        case_id = str(case_id)  # check for string
        ocel["objects"].append({
            "id": f"{case_object_type}_{case_id}",
            "type": case_object_type,
            "attributes": [],
            "relationships": []
        })
    
    event_counter = 0
    # create events
    for _, row in df.iterrows():
        activity = str(row.get('concept:name', 'unknown'))
        case_id = str(row.get('case:concept:name', 'unknown'))
        timestamp = row.get('time:timestamp', datetime.now())
        
        event = {
            "id": f"e{event_counter}",
            "type": activity,
            "time": convert_to_json_serializable(timestamp),
            "attributes": [],
            "relationships": []
        }
        
        # create case relationship
        event["relationships"].append({
            "objectId": f"{case_object_type}_{case_id}",
            "qualifier": case_object_type
        })
        
        # create resource relationship (if present)
        resource_value = row.get(resource_attr)
        if pd.notna(resource_value):
            event["relationships"].append({
                "objectId": f"{resource_object_type}_{resource_value}",
                "qualifier": resource_object_type
            })
        
        # create event attributes from remaining columns
        for col, val in row.items():
            if col not in ['concept:name', 'case:concept:name', 'time:timestamp', resource_attr]:
                if pd.notna(val):
                    val = convert_to_json_serializable(val)
                    event["attributes"].append({
                        "name": col,
                        "value": val
                    })
        
        ocel["events"].append(event)
        event_counter += 1
    
    # create event types
    final_event_types = []
    for activity, attributes in event_types.items():
        if isinstance(attributes, dict):
            final_event_types.append(attributes)
        else:
            event_type_def = {
                "name": activity,
                "attributes": []
            }
            for attr in attributes:
                event_type_def["attributes"].append({
                    "name": attr,
                    "type": attribute_types.get(attr, "string")
                })
            final_event_types.append(event_type_def)
    
    ocel["eventTypes"] = final_event_types
    
    class NumpyEncoder(json.JSONEncoder):
        def default(self, obj):
            return convert_to_json_serializable(obj)
    
    with open(ocel_path, 'w') as f:
        json.dump(ocel, f, indent=2, cls=NumpyEncoder)

if __name__ == "__main__":
    xes_to_ocel2('data/sample_data/xes_sample.xes', 'data/generated_data/xes_to_ocel2/ocel2_from_xes.json')