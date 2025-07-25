import numpy as np
from typing import Dict
from xes_metrics import  get_xes_metrics
from ocel2_metrics import get_ocel2_metrics

def ocel2_to_xes_quantifier(ocel2_file_path: str, xes_file_path: str):
    """
    params:
    - ocel2_file_path: OCEL2 file path
    - xes_file_path: XES file path
    """

    ocel2_metrics = get_ocel2_metrics(ocel2_file_path)
    xes_metrics = get_xes_metrics(xes_file_path)
    quality_scores = {}

    # 1. basic preservation metrics

    # case notion selection impact (convergence)
    case_selection_ratio = xes_metrics['num_cases'] / ocel2_metrics['num_objects'] if ocel2_metrics['num_objects'] > 0 else 1
    case_selection_quality = min(case_selection_ratio, 1.0)
    quality_scores['case_selection_preservation'] = float(case_selection_quality)

    # activity type preservation
    activity_preservation = min(xes_metrics['num_activities'] / ocel2_metrics['num_event_types'], 1.0) if ocel2_metrics['num_event_types'] > 0 else 1
    quality_scores['activity_type_preservation'] = float(activity_preservation)

    # attribute mapping impact
    ocel2_total_attrs = ocel2_metrics['num_event_attributes'] + ocel2_metrics['num_object_attributes']
    xes_total_attrs = xes_metrics['num_event_attributes'] + xes_metrics['num_case_attributes']
    attribute_mapping_quality = abs(xes_total_attrs / ocel2_total_attrs) - 1 if ocel2_total_attrs > 0 else 1
    quality_scores['attribute_mapping_preservation'] = float(attribute_mapping_quality)
    
    # temporal distortion
    xes_range = xes_metrics['time_range_hours']
    ocel2_range = ocel2_metrics['time_range_hours']
    temporal_consistency = 1 - abs(xes_range - ocel2_range) / max(xes_range, ocel2_range) if max(xes_range, ocel2_range) > 0 else 1
    quality_scores['temporal_preservation'] = float(temporal_consistency)
    
    basic_preservation_score = np.mean([
        quality_scores['case_selection_preservation'],
        quality_scores['activity_type_preservation'],
        quality_scores['attribute_mapping_preservation'],
        quality_scores['temporal_preservation'],
    ])

    # 2. information loss metrics

    # o2o relationships loss 
    o2o_loss = ocel2_metrics['num_o2o_relationships'] if ocel2_metrics['num_objects'] > 0 else 0
    quality_scores['o2o_relationship_loss'] = o2o_loss

    # e2o flattening loss
    e2o_loss = max(0, 1 - (1 / ocel2_metrics['avg_e2o_per_event'])) if ocel2_metrics['avg_e2o_per_event'] > 0 else 0
    quality_scores['e2o_relationship_loss'] = e2o_loss
    
    # multi-object association loss
    avg_objects_per_event = ocel2_metrics['avg_e2o_per_event']
    multi_object_loss = max(0, 1 - (1 / avg_objects_per_event)) if avg_objects_per_event > 0 else 0
    quality_scores['multi_object_loss'] = multi_object_loss
    
    # dynamic attributes loss
    dynamic_attr_loss = 1 if ocel2_metrics['num_dynamic_changes'] > 0 else 0
    quality_scores['dynamic_attribute_loss'] = dynamic_attr_loss
    
    # object type reduction
    object_type_loss = 1 - (1 / ocel2_metrics['num_object_types']) if ocel2_metrics['num_object_types'] > 0 else 0
    quality_scores['object_type_loss'] = object_type_loss
    
    information_loss_score = 1 - np.mean([
        quality_scores['o2o_relationship_loss'],
        quality_scores['e2o_relationship_loss'],
        quality_scores['multi_object_loss'],
        quality_scores['dynamic_attribute_loss'],
        quality_scores['object_type_loss']
    ])
    
    # 3. complexity metrics
    
    # event distribution distortion (divergence)
    ocel2_events_per_object = ocel2_metrics['avg_events_per_object']
    xes_events_per_case = xes_metrics['avg_events_per_case']
    distribution_distortion = abs(xes_events_per_case - ocel2_events_per_object) / ocel2_events_per_object if ocel2_events_per_object > 0 else 0
    quality_scores['event_distribution_distortion'] = distribution_distortion
    
    # complexity increase
    complexity_increase = abs(xes_metrics['num_events'] - ocel2_metrics['num_events']) / ocel2_metrics['num_events'] if ocel2_metrics['num_events'] > 0 else 0
    quality_scores['complexity_increase'] = complexity_increase
    
    complexity_score = 1 - np.mean([
        quality_scores['event_distribution_distortion'],
        quality_scores['complexity_increase']
    ])
    
    # total score (weighted)
    
    weights = {
        'basic_preservation': 0.30,
        'information_loss': 0.50,
        'complexity': 0.20,
    }
    
    dimension_scores = {
        'basic_preservation': basic_preservation_score,
        'information_loss': information_loss_score,
        'complexity': complexity_score,
    }
    
    total_score = sum(dimension_scores[dim] * weights[dim] for dim in dimension_scores)
    
    result = {
        'quality_score': total_score,
        'loss_percentage': 1 - information_loss_score, #inverted
        'dimension_scores': {
            'basic_preservation': round(basic_preservation_score, 2),
            'information_loss': round((1 - information_loss_score), 2),
            'complexity': round((1 - complexity_score), 2)
        },
        'detailed_metrics': {k: round(v, 4) for k, v in quality_scores.items()},
    }
    
    print_quality_report(result)
    #return result

def print_quality_report(results: Dict):
    """
    params:
    - results: dic containing each score
    """  

    print("'OCEL2 -> XES'-Converter Quantifier")
    print("\nDIMENSIONEN:")
    for dim, score in results['dimension_scores'].items():
        print(f"  {dim.replace('_', ' ').title()}: {score:.1%}")
    
    print("\nDETAILMETRIKEN:")
    for metric, value in results['detailed_metrics'].items():
        print(f"  {metric.replace('_', ' ').title()}: {value:.1%}")

    print(f"\nGESAMTBEWERTUNG: {results['quality_score']:.3f}")
    print(f"INFORMATIONSVERLUST: {results['loss_percentage']:.3f}%")

if __name__ == "__main__":
    ocel2_to_xes_quantifier('data/sample_data/ocel2_sample.json', 'data/generated_data/ocel2_to_xes/xes_from_ocel2.xes')