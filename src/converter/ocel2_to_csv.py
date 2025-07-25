import pm4py  
import pandas as pd 
  
def ocel2_to_csv(ocel2_path: str, csv_path: str):
    """
    params:
    - ocel2_path: OCEL2 file path
    - csv_path: CSV output file path
    """
    
    ocel = pm4py.read_ocel2_json(ocel2_path)
      
    # identify primary case type by frequency
    primary_case_type = ocel.relations.groupby('ocel:type').size().idxmax()
      
    # collect event attributes
    event_attributes = [col for col in ocel.events.columns if not col.startswith('ocel:')]
    
    # create list contaning all events
    csv_rows = []
    for _, event in ocel.events.iterrows():
        event_id = event['ocel:eid']
        event_relations = ocel.relations[ocel.relations['ocel:eid'] == event_id]
          
        # mandatory columns
        row = {
            'case_id': '; '.join(event_relations[event_relations['ocel:type'] == primary_case_type]['ocel:oid']),
            'activity': event['ocel:activity'],
            'timestamp': event['ocel:timestamp']
        }
        
        # add attributes
        for attr in event_attributes:
            row[attr] = event.get(attr, '')
          
        csv_rows.append(row)  

    pd.DataFrame(csv_rows).to_csv(csv_path, index=False)

if __name__ == "__main__":
    ocel2_to_csv('data/generated_data/roundtrip/ocel2_from_xes_simple.json', 'data/generated_data/roundtrip/csv_from_ocel2_simple.csv')