import pm4py
import pandas as pd

def get_xes_metrics(file_path):
    """
    params:
    - file_path: XES file path
    """

    log = pm4py.read_xes(file_path)

    df = pm4py.convert_to_dataframe(log)
 
    # extract statistics
    stats = {
        'num_events': len(df),  
        'num_cases': df['case:concept:name'].nunique(),
        'num_activities': df['concept:name'].nunique(),
        'num_resources': df['org:resource'].nunique() if 'org:resource' in df.columns else 0,
        'num_event_attributes': len([col for col in df.columns if not col.startswith('case:')]),
        'num_case_attributes': len([col for col in df.columns if col.startswith('case:')]),
        'avg_events_per_case': len(df) / df['case:concept:name'].nunique(),
        'avg_case_duration_hours': pm4py.get_all_case_durations(log)[0] / 3600 if pm4py.get_all_case_durations(log) else 0,  
        'time_range_hours': (pd.to_datetime(df['time:timestamp']) .max() - pd.to_datetime(df['time:timestamp']) .min()).total_seconds() / 3600,
        'most_frequent_activity': df['concept:name'].mode().iloc[0] if not df.empty else None,
        'most_active_resource': df['org:resource'].mode().iloc[0] if 'org:resource' in df.columns and not df.empty else None
    }

    # print function to be used if xes_metrics is used alone (not in a quantifier)
    """
    print(f"\nFile: {file_path}")
    print(f"Metrics:")
    print(f"  Cases: {stats['num_cases']}")
    print(f"  Events: {stats['num_events']}")
    print(f"  Aktivitäten: {stats['num_activities']}")
    print(f"  Ressourcen: {stats['num_resources']}")
    print(f"  Event-Attribute: {stats['num_event_attributes']}")
    print(f"  Case-Attribute: {stats['num_case_attributes']}")
    print(f"\n  Durchschn. Events/Case: {stats['avg_events_per_case']:.2f}")
    print(f"  Durchschn. Case-Dauer: {stats['avg_case_duration_hours']:.2f} Stunden")
    print(f"  Zeitspanne: {stats['time_range_hours']:.2f} Stunden")
    if stats['most_frequent_activity']:
        print(f"\n  Häufigste Aktivität: {stats['most_frequent_activity']}")
    if stats['most_active_resource']:
        print(f"  Aktivste Ressource: {stats['most_active_resource']}")
    print("\n")
    """

    return stats
