import pm4py

def ocel2_to_xes(ocel2_path: str, xes_path: str):
    """
    params:
    - ocel2_path: OCEL2 file path
    - xes_path: XES output file path
    """

    ocel = pm4py.read_ocel2_json(ocel2_path)
    
    # identify primary case type by frequency
    object_type = ocel.relations.groupby('ocel:type').size().idxmax()

    flattened_log = pm4py.ocel_flattening(ocel, object_type)
      
    pm4py.write_xes(flattened_log, xes_path)

    # print the used object type after success 
    print(f"Verwendeter Objekttyp als Case-Notion: {object_type}")

if __name__ == "__main__":
    ocel2_to_xes('data/sample_data/ocel2_sample.json', 'data/generated_data/ocel2_to_xes/xes_from_ocel2.xes')