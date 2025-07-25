import pm4py
import pandas as pd

def get_ocel2_metrics(file_path):
    """
    params:
    - file_path: OCEL 2 file path
    """

    ocel = pm4py.read_ocel2(file_path)
    
    # pm4py allows for simple extraction of statistics
    stats = {
        'num_events': len(ocel.events),
        'num_event_types': len(ocel.events[ocel.event_activity].unique()),
        'num_event_attributes': len([col for col in ocel.events.columns if not col.startswith("ocel:")]),
        'num_objects': len(ocel.objects),
        'num_object_types': len(pm4py.ocel_get_object_types(ocel)),  
        'num_object_attributes': len([col for col in ocel.objects.columns if not col.startswith("ocel:")]),
        'num_dynamic_changes': len(ocel.object_changes) if hasattr(ocel, 'object_changes') else 0,
        'num_e2o_relationships': len(ocel.relations),
        'num_o2o_relationships': len(ocel.o2o) if hasattr(ocel, 'o2o') else 0,
        'avg_events_per_object': len(ocel.relations) / len(ocel.objects) if len(ocel.objects) > 0 else 0,
        'avg_e2o_per_event': len(ocel.relations) / len(ocel.events) if len(ocel.events) > 0 else 0,  
        'avg_o2o_per_object': len(ocel.o2o) / len(ocel.objects) if hasattr(ocel, 'o2o') and len(ocel.objects) > 0 else 0,
        'time_range_hours': (pd.to_datetime(ocel.events[ocel.event_timestamp]).max() -   
                           pd.to_datetime(ocel.events[ocel.event_timestamp]).min()).total_seconds() / 3600
    }  

    # print function to be used if ocel2_metrics is used alone (not in a quantifier)
    """
    print(f"\nFile: {file_path}")
    print(f"Metrics:")
    print(f"  Events: {stats['num_events']}")
    print(f"  Event-Typen: {stats['num_event_types']}")
    print(f"  Event-Attribute: {stats['num_event_attributes']}")
    print(f"  Objects: {stats['num_objects']}")
    print(f"  Object-Typen: {stats['num_object_types']}")
    print(f"  Object-Attribute: {stats['num_object_attributes']}")
    print(f"  E2O-Beziehungen: {stats['num_e2o_relationships']}")
    print(f"  O2O-Beziehungen: {stats['num_o2o_relationships']}")
    print(f"  Dynamische Änderungen: {stats['num_dynamic_changes']}")
    print(f"\n  Durchschn. Events/Object: {stats['avg_events_per_object']:.2f}")
    print(f"  Durchschn. E2O/Event: {stats['avg_e2o_per_event']:.2f}")
    print(f"  Durchschn. O2O/Object: {stats['avg_o2o_per_object']:.2f}")
    print(f"  Zeitspanne: {stats['time_range_hours']:.2f} Stunden")
    print("\n")
    """

    return stats
