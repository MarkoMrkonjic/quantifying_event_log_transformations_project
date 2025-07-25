import numpy as np
from typing import Dict
from xes_metrics import get_xes_metrics
from ocel2_metrics import get_ocel2_metrics

def xes_to_ocel2_quantifier(xes_file_path: str, ocel2_file_path: str):
    """
    params:
    - xes_file_path: XES file path
    - ocel2_file_path: OCEL2 file path
    """

    xes_metrics = get_xes_metrics(xes_file_path)
    ocel2_metrics = get_ocel2_metrics(ocel2_file_path)
    quality_scores = {}
    
    # 1. information preservation metrics
    
    # event preservation
    event_preservation = 1 - abs(1 - (ocel2_metrics['num_events'] / xes_metrics['num_events'])) if xes_metrics['num_events'] > 0 else 0
    quality_scores['event_preservation'] = float(event_preservation)
    
    # activity preservation
    activity_preservation = 1 - abs(1 - (ocel2_metrics['num_event_types'] / xes_metrics['num_activities'])) if xes_metrics['num_activities'] > 0 else 0
    quality_scores['activity_preservation'] = float(activity_preservation)
    
    # temporal consistency
    xes_range = xes_metrics['time_range_hours']
    ocel2_range = ocel2_metrics['time_range_hours']
    temporal_consistency = 1 - abs(xes_range - ocel2_range) / max(xes_range, ocel2_range) if max(xes_range, ocel2_range) > 0 else 1
    quality_scores['temporal_consistency'] = float(temporal_consistency)
    
    # attribute preservation
    xes_total_attrs = xes_metrics['num_event_attributes'] + xes_metrics['num_case_attributes']
    ocel2_total_attrs = ocel2_metrics['num_event_attributes'] + ocel2_metrics['num_object_attributes']
    attribute_preservation = 1 - abs(ocel2_total_attrs / xes_total_attrs) if xes_total_attrs > 0 else 1
    quality_scores['attribute_preservation'] = float(attribute_preservation)
    
    information_score = np.mean([
        quality_scores['event_preservation'],
        quality_scores['activity_preservation'],
        quality_scores['temporal_consistency'],
        quality_scores['attribute_preservation']
    ])
    

    # 2. object-centry enrichment metrics

    # object discovery rate
    # exception: at least cases and resources as objects
    expected_objects = xes_metrics['num_cases'] + xes_metrics['num_resources']
    actual_objects = ocel2_metrics['num_objects']
    object_discovery_rate = min(actual_objects / expected_objects, 1.0) if expected_objects > 0 else 0
    quality_scores['object_discovery_rate'] = float(object_discovery_rate)
    
    # e2o relationship discovery
    # exception: at least e2o to case, e2o to resource (if available)
    e2o_density = ocel2_metrics['avg_e2o_per_event']
    expected_e2o = 2.0  # Case + Resource
    e2o_quality = min(e2o_density / expected_e2o, 1.0)
    quality_scores['e2o_density'] = float(e2o_quality)
    
    # object type diversity
    # exception: at least case, resource (if available)
    object_type_diversity = min(ocel2_metrics['num_object_types'] / 2, 1.0)
    quality_scores['object_type_diversity'] = float(object_type_diversity)
    
    # o2o relationship discovery
    o2o_quality = min(ocel2_metrics['num_o2o_relationships'] / ocel2_metrics['num_objects'], 1) if ocel2_metrics['num_objects'] > 0 else 0
    quality_scores['o2o_discovery'] = float(o2o_quality)

    # utilization of dynamic attributes
    dynamic_utilization = min(ocel2_metrics['num_dynamic_changes'] / (ocel2_metrics['num_event_attributes'] + ocel2_metrics['num_object_attributes']), 0.1) * 10 \
                        if max(ocel2_metrics['num_event_attributes'], ocel2_metrics['num_object_attributes']) > 0 else 1
    quality_scores['dynamic_utilization'] = float(dynamic_utilization)
    
    enrichment_score = np.mean([
        quality_scores['object_discovery_rate'],
        quality_scores['e2o_density'],
        quality_scores['object_type_diversity'],
        quality_scores['o2o_discovery'],
        quality_scores['dynamic_utilization']
    ])
    
    # 3. structural integrity metrics
    
    # case coverage
    case_objects = ocel2_metrics['num_objects'] - xes_metrics['num_resources']
    case_coverage = 1 - abs(1 - (case_objects / xes_metrics['num_cases'])) if xes_metrics['num_cases'] > 0 else 0
    quality_scores['case_coverage'] = float(case_coverage)
    
    # event distribution consistency 
    # Vergleich der durchschnittlichen Events pro Case/Object
    xes_avg_events = xes_metrics['avg_events_per_case']
    ocel2_avg_events = ocel2_metrics['avg_events_per_object']
    distribution_consistency = 1 - abs(xes_avg_events - ocel2_avg_events) / max(xes_avg_events, ocel2_avg_events) if max(xes_avg_events, ocel2_avg_events) > 0 else 1
    quality_scores['distribution_consistency'] = float(distribution_consistency)
    
    structural_score = np.mean([
        quality_scores['case_coverage'],
        quality_scores['distribution_consistency']
    ])
    
    # total score (weighted)
    weights = {
        'information_preservation': 0.30,
        'object_enrichment': 0.40,
        'structural_integrity': 0.30,
    }
    
    dimension_scores = {
        'information_preservation': information_score,
        'object_enrichment': enrichment_score,
        'structural_integrity': structural_score,
    }
    
    total_score = sum(dimension_scores[dim] * weights[dim] for dim in dimension_scores)
    
    result = {
        'total_score': total_score,
        'dimension_scores': {k: v for k, v in dimension_scores.items()},
        'detailed_metrics': {k: v for k, v in quality_scores.items()}
    }
    
    print_quality_report(result)
    #return result

def print_quality_report(results: Dict):
    """
    params:
    - results: dic containing each score
    """  
    
    print("'XES -> OCEL2'-Converter Quantifier")
    
    print("\nDIMENSIONEN:")
    for dim, score in results['dimension_scores'].items():
        print(f"  {dim.replace('_', ' ').title()}: {score:.1%}")
    
    print("\nDETAILMETRIKEN:")
    for metric, value in results['detailed_metrics'].items():
        print(f"  {metric.replace('_', ' ').title()}: {value:.1%}")
    
    print(f"\nGESAMTBEWERTUNG: {results['total_score']:.3f}")

if __name__ == "__main__":
    xes_to_ocel2_quantifier('data/sample_data/xes_sample2.xes','data/generated_data/xes_to_ocel2/ocel2_from_xes2.json')