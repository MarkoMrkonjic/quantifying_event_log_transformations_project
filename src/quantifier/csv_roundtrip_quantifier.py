import pandas as pd  
from typing import Dict, Any  
from csv_metrics import get_csv_metrics

def csv_roundtrip_quantifier(original_csv_path: str, roundtrip_csv_path: str) -> Dict[str, Any]:  
    """
    params:
    - original_csv_path: starting CSV file path
    - roundtrip_csv_path: file path of CSV after roundtrip
    """

    original_metrics = get_csv_metrics(original_csv_path)['stats']  
    roundtrip_metrics = get_csv_metrics(roundtrip_csv_path)['stats']  
       
    preservation_metrics = calculate_preservation_metrics(original_metrics, roundtrip_metrics)  
 
    structural_analysis = analyze_structural_differences(original_csv_path, roundtrip_csv_path)  
      
    data_quality = analyze_data_quality(original_csv_path, roundtrip_csv_path)  
      
    overall_score = calculate_roundtrip_score(preservation_metrics, structural_analysis, data_quality)  
      
    results = {  
        'original_metrics': original_metrics,  
        'roundtrip_metrics': roundtrip_metrics,  
        'preservation_analysis': preservation_metrics,  
        'structural_analysis': structural_analysis,  
        'data_quality_analysis': data_quality,  
        'overall_roundtrip_score': overall_score,  
        'insights': generate_roundtrip_insights(preservation_metrics, structural_analysis, data_quality)  
    }  
      
    print_roundtrip_analysis(results)  
    return results  
  
def calculate_preservation_metrics(original_metrics: Dict, roundtrip_metrics: Dict) -> Dict[str, float]:  
    """
    params:
    - original_metrics: starting CSV file metrics
    - roundtrip_metrics: roundtrip CSV file metrics
    """
    
    # any deviation of 1 (100% preservation) affects the score negatively
    case_preservation = 1 - abs(1 - (roundtrip_metrics['num_cases'] / original_metrics['num_cases'])) if original_metrics['num_cases'] > 0 else 0  
    event_preservation = 1 - abs(1 - (roundtrip_metrics['num_events'] / original_metrics['num_events'])) if original_metrics['num_events'] > 0 else 0  
    activity_preservation = 1 - abs(1 - (roundtrip_metrics['num_activities'] / original_metrics['num_activities'])) if original_metrics['num_activities'] > 0 else 0  
    attribute_preservation = 1 - abs(1 - (roundtrip_metrics['num_event_attributes'] / original_metrics['num_event_attributes'])) if original_metrics['num_event_attributes'] > 0 else 0 
        
    avg_events_per_case_preservation = (  
        1 - abs(1 - roundtrip_metrics['avg_events_per_case'] / original_metrics['avg_events_per_case'])
        if original_metrics['avg_events_per_case'] > 0 else 0  
    )  
        
    multi_attr_preservation = (  
        1 - abs(1 - roundtrip_metrics['num_multi_attributes'] / original_metrics['num_multi_attributes'])   
        if original_metrics['num_multi_attributes'] > 0 else 1.0  
    )  
        
    time_range_preservation = (  
        1 - abs(1 - roundtrip_metrics['time_range_hours'] / original_metrics['time_range_hours'])
        if original_metrics['time_range_hours'] > 0 else 1.0  
    )  
      
    return {  
        'case_preservation_ratio': float(case_preservation),  
        'event_preservation_ratio': float(event_preservation),  
        'activity_preservation_ratio': float(activity_preservation),  
        'attribute_preservation_ratio': float(attribute_preservation),  
        'avg_events_per_case_preservation': float(avg_events_per_case_preservation),  
        'multi_attribute_preservation': float(multi_attr_preservation),  
        'time_range_preservation': float(time_range_preservation)  
    }  
  
def analyze_structural_differences(original_path: str, roundtrip_path: str) -> Dict[str, Any]:  
    """
    params:
    - original_path: starting CSV file path
    - roundtrip_path: roundtrip CSV file path
    """
      
    df_original = pd.read_csv(original_path)  
    df_roundtrip = pd.read_csv(roundtrip_path)  
      
    original_columns = set(df_original.columns)  
    roundtrip_columns = set(df_roundtrip.columns)  
    
    # calculate structural differences
    added_columns = roundtrip_columns - original_columns  
    removed_columns = original_columns - roundtrip_columns  
    preserved_columns = original_columns & roundtrip_columns  
      
    # calculate data type changes  
    dtype_changes = {}  
    for col in preserved_columns: 
        orig_dtype = str(df_original[col].dtype)  
        round_dtype = str(df_roundtrip[col].dtype)  
        if orig_dtype != round_dtype:  
            dtype_changes[col] = {'original': orig_dtype, 'roundtrip': round_dtype}
    
    dtype_preservation = 1 - len(dtype_changes) / len(preserved_columns) if len(preserved_columns) > 0 else 0
      
    # calculate schema preservation rate
    schema_preservation = (len(preserved_columns) / len(original_columns)) if len(original_columns) > 0 else 0
      
    return {  
        'added_columns': list(added_columns),  
        'removed_columns': list(removed_columns),  
        'preserved_columns': list(preserved_columns),  
        'dtype_preservation_ratio': float(dtype_preservation),  
        'schema_preservation_ratio': float(schema_preservation),
        'column_count_change': len(roundtrip_columns) - len(original_columns)  
    }  
  
def analyze_data_quality(original_path: str, roundtrip_path: str) -> Dict[str, Any]:  
    """
    params:
    - original_path: starting CSV file path
    - roundtrip_path: roundtrip CSV file path
    """ 
      
    df_original = pd.read_csv(original_path)  
    df_roundtrip = pd.read_csv(roundtrip_path)  
      
    # Gemeinsame Spalten für Vergleich  
    common_columns = set(df_original.columns) & set(df_roundtrip.columns)  
      
    # calculate difference in null values
    null_analysis = {}  
    for col in common_columns:  
        orig_nulls = df_original[col].isnull().sum()  
        round_nulls = df_roundtrip[col].isnull().sum()  
        null_analysis[col] = {  
            'original_nulls': orig_nulls,  
            'roundtrip_nulls': round_nulls,  
            'null_change': 1 - abs(1 - round_nulls - orig_nulls)
        }  
      
    # calculate unique values for cases and activities
    orig_unique_total = []
    unique_values_preserved = [] 
    for col in common_columns:  
        if col in ['case_id', 'activity']:
            orig_unique = df_original[col].unique()  
            round_unique = df_roundtrip[col].unique()
            common_uniques_count = len([item for item in orig_unique if item in round_unique])
            orig_unique_total.append(len(orig_unique))
            unique_values_preserved.append(common_uniques_count)
            
    return {  
        'null_values_analysis': float(len(null_analysis) / len(common_columns)),  
        'unique_values_analysis': sum(unique_values_preserved) / sum(orig_unique_total) 
    }  
  
def calculate_roundtrip_score(preservation: Dict, structural: Dict, quality: Dict) -> Dict[str, Any]:  
    """
    params:
    - preservation: preservation dic scores
    - structural: preservation dic scores
    - quality: quality dic scores
    """ 

    # calculate mean for each part score  
    preservation_score = float((  
        preservation['case_preservation_ratio'] +  
        preservation['event_preservation_ratio'] +  
        preservation['activity_preservation_ratio'] +  
        preservation['attribute_preservation_ratio'] +  
        preservation['avg_events_per_case_preservation'] +
        preservation['multi_attribute_preservation'] +
        preservation['time_range_preservation'] 
    ) / 7)
      
    structural_score = float((structural['schema_preservation_ratio'] + structural['dtype_preservation_ratio']) / 2)

    quality_score = float((quality['null_values_analysis'] + quality['unique_values_analysis']) / 2)

    # weighted total score
    overall_score = (preservation_score * 0.6 + structural_score * 0.3 + quality_score * 0.1)  
      
    return {  
        'overall_roundtrip_score': float(overall_score), 
        'preservation_contribution': float(preservation_score * 0.6),  
        'structural_contribution': float(structural_score * 0.3),  
        'quality_contribution': float(quality_score * 0.1)  
    }  
  
def generate_roundtrip_insights(preservation: Dict, structural: Dict, quality: Dict) -> list:  
    """
    params:
    - preservation: preservation dic scores
    - structural: preservation dic scores
    - quality: quality dic scores
    """

    insights = []  
      
    # structural insights
    if len(structural['removed_columns']) > 0:  
        insights.append(f"Spalten entfernt: {', '.join(structural['removed_columns'])}")  
      
    if len(structural['added_columns']) > 0:  
        insights.append(f"Neue Spalten hinzugefügt: {', '.join(structural['added_columns'])}")   
      
    return insights  
  
def print_roundtrip_analysis(results: Dict):  
    """
    params:
    - results: dic containing each score
    """  
    print("CSV ROUNDTRIP ANALYSE")    
      
    print(f"\nGRUNDVERGLEICH:")  
    orig = results['original_metrics']  
    round_trip = results['roundtrip_metrics']  
    print(f"  Cases: {orig['num_cases']} → {round_trip['num_cases']}")  
    print(f"  Events: {orig['num_events']} → {round_trip['num_events']}")  
    print(f"  Aktivitäten: {orig['num_activities']} → {round_trip['num_activities']}")  
    print(f"  Event-Attribute: {orig['num_event_attributes']} → {round_trip['num_event_attributes']}")  
      
    print(f"\nPRESERVATION-ANALYSE:")  
    pres = results['preservation_analysis']
    print(f"  Case-Preservation: {pres['case_preservation_ratio']:.1%}") 
    print(f"  Event-Preservation: {pres['event_preservation_ratio']:.1%}") 
    print(f"  Aktivitäten-Preservation: {pres['activity_preservation_ratio']:.1%}")  
    print(f"  Attribut-Preservation: {pres['attribute_preservation_ratio']:.1%}")
    print(f"  Durchschnittlice Events/Case-Preservation: {pres['avg_events_per_case_preservation']:.1%}")
    print(f"  Multi-Attribute-Preservation: {pres['multi_attribute_preservation']:.1%}")
    print(f"  Prozesszeit-Preservation: {pres['time_range_preservation']:.1%}") 
      
    print(f"\nSTRUKTURELLE ANALYSE:")  
    struct = results['structural_analysis']
    print(f"  Datentyp-Preservation: {struct['dtype_preservation_ratio']:.1%}")
    print(f"  Schema-Preservation: {struct['schema_preservation_ratio']:.1%}") 
    print(f"  Entfernte Spalten: {len(struct['removed_columns'])}")  
    print(f"  Hinzugefügte Spalten: {len(struct['added_columns'])}")   
    
    print(f"\nDATENQUALITÄT ANALYSE:")  
    struct = results['data_quality_analysis']
    print(f"  NULL-Preservation: {struct['null_values_analysis']:.1%}")
    print(f"  Eindeutige Cases/Aktivitäten-Preservation: {struct['unique_values_analysis']:.1%}")

    print(f"\nINSIGHTS:")  
    for insight in results['insights']:  
        print(f"  {insight}")

    print(f"\nGESAMTBEWERTUNG:")  
    overall = results['overall_roundtrip_score']  
    print(f"  Gesamtscore: {overall['overall_roundtrip_score']:.3f}") 

if __name__ == "__main__":
    csv_roundtrip_quantifier('data/sample_data/csv_sample_multi2.csv', 'data/generated_data/roundtrip/csv_from_ocel2_multi2.csv')
