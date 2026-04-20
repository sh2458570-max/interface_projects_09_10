from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[2]
PDF_BASIS = ROOT / 'MIL-STD-6016D(无水印)(1).pdf'
SOURCE_DOC_REFERENCES = [
    ROOT / 'data/real_docs/j12_structured_excerpt_clean.txt',
    ROOT / 'data/real_docs/j12_latlong_probe.txt',
    ROOT / 'data/real_docs/j12_mission_assignment_probe.txt',
    ROOT / 'data/real_docs/j12_receipt_compliance_probe.txt',
    ROOT / 'data/real_docs/j12_full_structured_excerpt.txt',
]

MISSION_ASSIGNMENT_MAPPING = (
    '0=NO_STATEMENT, 1=REFUEL, 2=ORBIT, 3=RECALL, 4=RETURN_TO_BASE, '
    '5=ENGAGE, 6=PRIORITY_KILL, 7=BREAK_ENGAGEMENT, 8=INVESTIGATE_INTERROGATE, '
    '9=CLEAR_TO_DROP, 10=CEASE_DO_NOT_DROP, 11=INTERVENE, 12=DISUSED, '
    '13=AIR_TO_SURFACE, 14=AIR_TO_AIR, 15=SEARCH_AND_RESCUE, '
    '16=COMBAT_AIR_PATROL, 17=DISUSED, 18=LASER_DESIGNATION, 19=DISUSED, '
    '20=CLOSE_AIR_SUPPORT, 21=INTERDICTION, 22=AERIAL_RECONNAISSANCE, '
    '23=ESCORT, 24=SHADOW, 25=WEAPONS_FREE, 26=WEAPONS_TIGHT, '
    '27=SALVO_CLEAR_AIRCRAFT, 28=ALERT_CONDITION_WHITE, '
    '29=ALERT_CONDITION_YELLOW, 30=ALERT_CONDITION_RED, 31=COVER, '
    '32=VISUAL_IDENTIFICATION, 33=DISUSED, 34=GO_TO_VOICE, '
    '35=HIGH_INTEREST_TRACK_DESIGNATION, '
    '36=CANCEL_HIGH_INTEREST_TRACK_DESIGNATION, 37=SENSOR_TARGET_REPORTS_ON, '
    '38=SENSOR_TARGET_REPORTS_OFF, 39=SUPPRESSION_OF_ENEMY_AIR_DEFENSES, '
    '40=ARMED_RECONNAISSANCE_ATTACK, 41=CEASE_ATTACK, '
    '42=RELATED_MISSION_DATA, 43=COUNTER_AIR_ATTACK, 44=FIGHTER_SWEEP, '
    '45=CEASE_FIRE, 46=HOLD_FIRE, 47=ATTACK_TARGET_COMPLEX, '
    '48-62=UNDEFINED, 63=CEASE_MISSION'
)

RECEIPT_COMPLIANCE_MAPPING = (
    '0=ORIGINAL_ORDER_RESPONSE_REQUIRED, 1=ORIGINAL_ORDER_RESPONSE_NOT_REQUIRED, '
    '2=MACHINE_RECEIPT, 3=WILCO, 4=HAVCO, 6=CANTCO, '
    '11=CANTPRO_APPLICABLE_ASSIGNMENT_TABLE_FULL, '
    '12=CANTPRO_RELAY_FUNCTION_ALREADY_ASSIGNED, '
    '13=CANTPRO_INTERFERENCE_PROTECTION_VIOLATION, '
    '14=CANTPRO_HOST_SUBSCRIBER_INTERFACE_INACTIVE, 23-30=UNDEFINED'
)


@dataclass(frozen=True)
class J12FieldSpec:
    source_field: str
    target_field: str
    conversion_mode: str
    rule: str
    bit_length: int | None
    description: str
    source_reference: str
    sample_value: int
    unit: str | None = None

    def to_kb_rule(self) -> Dict[str, Any]:
        return {
            'protocol_type': 'Link16',
            'message_code': 'J12.0',
            'field_name': self.source_field,
            'aliases': [],
            'conversion_mode': self.conversion_mode,
            'formula': self.rule,
            'target_field': self.target_field,
            'unit': self.unit,
            'bit_length': self.bit_length,
            'description': self.description,
            'source': 'MIL-STD-6016D J12.0 full bundle',
        }


J12_0_FULL_FIELD_SPECS: List[J12FieldSpec] = [
    J12FieldSpec('TRACK_NUMBER_ADDRESSEE', 'TRACK_NUMBER_ADDRESSEE', 'transcoding', 'value', 15, 'J12.0I track number addressee passthrough.', 'J12.0I TRACK NUMBER, ADDRESSEE', 1234),
    J12FieldSpec('MISSION_ASSIGNMENT_DISCRETE', 'MISSION_ASSIGNMENT_LABEL', 'mapping', MISSION_ASSIGNMENT_MAPPING, 6, 'J12.0I mission assignment discrete label mapping.', 'J12.0I MISSION ASSIGNMENT DISCRETE', 5),
    J12FieldSpec('TRACK_NUMBER_OBJECTIVE', 'TRACK_NUMBER_OBJECTIVE', 'transcoding', 'value', 19, 'J12.0I objective track number passthrough.', 'J12.0I TRACK NUMBER, OBJECTIVE', 54321),
    J12FieldSpec('THREAT_WARNING_ENVIRONMENT', 'THREAT_WARNING_ENVIRONMENT', 'transcoding', 'value', 3, 'J12.0I threat warning environment passthrough.', 'J12.0I THREAT WARNING ENVIRONMENT', 1),
    J12FieldSpec('RECEIPT_COMPLIANCE', 'RECEIPT_COMPLIANCE_LABEL', 'mapping', RECEIPT_COMPLIANCE_MAPPING, 5, 'J12.0I receipt/compliance mapping.', 'J12.0I RECEIPT/COMPLIANCE', 3),
    J12FieldSpec('RECURRENCE_RATE_RECEIPT_COMPLIANCE', 'RECURRENCE_RATE_RECEIPT_COMPLIANCE', 'transcoding', 'value', 4, 'J12.0I recurrence rate passthrough.', 'J12.0I RECURRENCE RATE, RECEIPT/COMPLIANCE', 2),
    J12FieldSpec('STRENGTH', 'STRENGTH', 'transcoding', 'value', 4, 'J12.0C1 strength passthrough.', 'J12.0C1 STRENGTH', 4),
    J12FieldSpec('NUMBER_OF_ASSOCIATED_DMPIS', 'NUMBER_OF_ASSOCIATED_DMPIS', 'transcoding', 'value', 4, 'J12.0C1 number of associated DMPIs passthrough.', 'J12.0C1 NUMBER OF ASSOCIATED DMPIS', 2),
    J12FieldSpec('ENVIRONMENT', 'ENVIRONMENT', 'transcoding', 'value', 3, 'J12.0C1 environment passthrough.', 'J12.0C1 ENVIRONMENT', 2),
    J12FieldSpec('ORIGIN_OF_INDEX_NUMBER', 'ORIGIN_OF_INDEX_NUMBER', 'transcoding', 'value', 1, 'J12.0C1 origin of index number passthrough.', 'J12.0C1 ORIGIN OF INDEX NUMBER', 1),
    J12FieldSpec('INDEX_NUMBER', 'INDEX_NUMBER', 'transcoding', 'value', 6, 'J12.0C1 index number passthrough.', 'J12.0C1 INDEX NUMBER', 12),
    J12FieldSpec('TARGET_POSITION_LATITUDE', 'TARGET_POSITION_LATITUDE_DEG', 'transcoding', 'signed(value, bits=20) * 0.0103 / 60', 20, 'J12.0C1 target position latitude converted to degree.', 'J12.0C1 LATITUDE, 0.0103 MINUTE', 2000, 'degree'),
    J12FieldSpec('TARGET_POSITION_LONGITUDE', 'TARGET_POSITION_LONGITUDE_DEG', 'transcoding', 'signed(value, bits=21) * 0.0103 / 60', 21, 'J12.0C1 target position longitude converted to degree.', 'J12.0C1 LONGITUDE, 0.0103 MINUTE', 8000, 'degree'),
    J12FieldSpec('METHOD_OF_FIRE', 'METHOD_OF_FIRE', 'transcoding', 'value', 3, 'J12.0C2 method of fire passthrough.', 'J12.0C2 METHOD OF FIRE', 2),
    J12FieldSpec('COURSE', 'COURSE', 'transcoding', 'value', 9, 'J12.0C2 course raw passthrough.', 'J12.0C2 COURSE', 128),
    J12FieldSpec('SPEED', 'SPEED', 'transcoding', 'value', 11, 'J12.0C2 speed raw passthrough.', 'J12.0C2 SPEED', 450),
    J12FieldSpec('ALTITUDE_100FT', 'ALTITUDE_FT', 'transcoding', 'unsigned(value, bits=11) * 100', 11, 'J12.0C2 altitude converted to feet.', 'J12.0C2 ALTITUDE, 100 FT', 250, 'ft'),
    J12FieldSpec('IDENTITY', 'IDENTITY', 'transcoding', 'value', 3, 'J12.0C2 identity passthrough.', 'J12.0C2 IDENTITY', 3),
    J12FieldSpec('IDENTITY_AMPLIFYING', 'IDENTITY_AMPLIFYING', 'transcoding', 'value', 3, 'J12.0C2 identity amplifying passthrough.', 'J12.0C2 IDENTITY AMPLIFYING', 1),
    J12FieldSpec('DESCRIPTOR', 'DESCRIPTOR', 'transcoding', 'value', 12, 'J12.0C2 descriptor passthrough.', 'J12.0C2 DESCRIPTOR', 12),
    J12FieldSpec('EXERCISE_INDICATOR', 'EXERCISE_INDICATOR', 'transcoding', 'value', 1, 'J12.0C2 exercise indicator passthrough.', 'J12.0C2 EXERCISE INDICATOR', 0),
    J12FieldSpec('AIR_SPECIFIC_TYPE', 'AIR_SPECIFIC_TYPE', 'transcoding', 'value', 12, 'J12.0C2 air specific type passthrough.', 'J12.0C2 AIR SPECIFIC TYPE', 120),
    J12FieldSpec('SPECIAL_INTEREST_INDICATOR', 'SPECIAL_INTEREST_INDICATOR', 'transcoding', 'value', 1, 'J12.0C2 special interest indicator passthrough.', 'J12.0C2 SPECIAL INTEREST INDICATOR', 1),
    J12FieldSpec('POINT_LOCATION_LATITUDE', 'POINT_LOCATION_LATITUDE_DEG', 'transcoding', 'signed(value, bits=23) * 0.0013 / 60', 23, 'J12.0C3 point latitude converted to degree.', 'J12.0C3 LATITUDE, 0.0013 MINUTE', 1200, 'degree'),
    J12FieldSpec('POINT_LOCATION_LONGITUDE', 'POINT_LOCATION_LONGITUDE_DEG', 'transcoding', 'signed(value, bits=24) * 0.0013 / 60', 24, 'J12.0C3 point longitude converted to degree.', 'J12.0C3 LONGITUDE, 0.0013 MINUTE', 6000, 'degree'),
    J12FieldSpec('POINT_LOCATION_ELEVATION', 'POINT_LOCATION_ELEVATION_FT', 'transcoding', 'unsigned(value, bits=10) * 25', 10, 'J12.0C3 point elevation converted to feet.', 'J12.0C3 ELEVATION, 25 FT, 1', 16, 'ft'),
    J12FieldSpec('POINT_TYPE', 'POINT_TYPE', 'transcoding', 'value', 4, 'J12.0C3 point type passthrough.', 'J12.0C3 POINT TYPE, 1', 2),
    J12FieldSpec('POINT_NUMBER', 'POINT_NUMBER', 'transcoding', 'value', 2, 'J12.0C3 point number passthrough.', 'J12.0C3 POINT NUMBER', 1),
    J12FieldSpec('TARGET_TYPE', 'TARGET_TYPE', 'transcoding', 'value', 5, 'J12.0C4 target type passthrough.', 'J12.0C4 TARGET TYPE', 7),
    J12FieldSpec('TARGET_DEFENSES', 'TARGET_DEFENSES', 'transcoding', 'value', 3, 'J12.0C4 target defenses passthrough.', 'J12.0C4 TARGET DEFENSES', 3),
    J12FieldSpec('RUN_IN_HEADING', 'RUN_IN_HEADING', 'transcoding', 'value', 9, 'J12.0C4 run-in heading raw passthrough.', 'J12.0C4 RUN IN HEADING', 90),
    J12FieldSpec('EGRESS_HEADING', 'EGRESS_HEADING', 'transcoding', 'value', 9, 'J12.0C4 egress heading raw passthrough.', 'J12.0C4 EGRESS HEADING', 180),
    J12FieldSpec('NUMBER_OF_STORES', 'NUMBER_OF_STORES', 'transcoding', 'value', 6, 'J12.0C4 number of stores passthrough.', 'J12.0C4 NUMBER OF STORES', 4),
    J12FieldSpec('TYPE_OF_STORES', 'TYPE_OF_STORES', 'transcoding', 'value', 8, 'J12.0C4 type of stores passthrough.', 'J12.0C4 TYPE OF STORES', 12),
    J12FieldSpec('MODE_OF_DELIVERY', 'MODE_OF_DELIVERY', 'transcoding', 'value', 7, 'J12.0C4 mode of delivery passthrough.', 'J12.0C4 MODE OF DELIVERY', 5),
    J12FieldSpec('TIME_DISCRETE', 'TIME_DISCRETE', 'transcoding', 'value', 3, 'J12.0C4 time discrete passthrough.', 'J12.0C4 TIME DISCRETE', 2),
    J12FieldSpec('MINUTE', 'MINUTE', 'transcoding', 'value', 6, 'J12.0C4 minute passthrough.', 'J12.0C4 MINUTE', 30),
    J12FieldSpec('HOUR', 'HOUR', 'transcoding', 'value', 5, 'J12.0C4 hour passthrough.', 'J12.0C4 HOUR', 14),
    J12FieldSpec('HAE_ADJUSTMENT', 'HAE_ADJUSTMENT', 'transcoding', 'value', 10, 'J12.0C5 HAE adjustment raw passthrough.', 'J12.0C5 HAE ADJUSTMENT', 20),
    J12FieldSpec('TIME_WINDOW', 'TIME_WINDOW', 'transcoding', 'value', 5, 'J12.0C5 time window passthrough.', 'J12.0C5 TIME WINDOW', 4),
    J12FieldSpec('TARGET_BEARING', 'TARGET_BEARING', 'transcoding', 'value', 16, 'J12.0C5 target bearing raw passthrough.', 'J12.0C5 TARGET BEARING', 1234),
    J12FieldSpec('LATITUDE_LSB', 'LATITUDE_LSB', 'transcoding', 'value', 1, 'J12.0C5 latitude LSB passthrough.', 'J12.0C5 LATITUDE, LSB', 0),
    J12FieldSpec('LONGITUDE_LSB', 'LONGITUDE_LSB', 'transcoding', 'value', 1, 'J12.0C5 longitude LSB passthrough.', 'J12.0C5 LONGITUDE, LSB', 1),
    J12FieldSpec('ELEVATION_LSBS', 'ELEVATION_LSBS', 'transcoding', 'value', 3, 'J12.0C5 elevation LSBs passthrough.', 'J12.0C5 ELEVATION, LSBS', 3),
    J12FieldSpec('SECOND', 'SECOND', 'transcoding', 'value', 6, 'J12.0C5 second passthrough.', 'J12.0C5 SECOND', 45),
    J12FieldSpec('LASER_ILLUMINATOR_CODE', 'LASER_ILLUMINATOR_CODE', 'transcoding', 'value', 16, 'J12.0C6 laser illuminator code passthrough.', 'J12.0C6 LASER ILLUMINATOR CODE', 1688),
    J12FieldSpec('TRACK_NUMBER_RELATED_3', 'TRACK_NUMBER_RELATED_3', 'transcoding', 'value', 19, 'J12.0C6 track number related 3 passthrough.', 'J12.0C6 TRACK NUMBER, RELATED 3', 23456),
    J12FieldSpec('INDEX_NUMBER_RELATED', 'INDEX_NUMBER_RELATED', 'transcoding', 'value', 6, 'J12.0C6 INDEX NUMBER, RELATED passthrough.', 'J12.0C6 INDEX NUMBER, RELATED', 8),
    J12FieldSpec('TRACK_NUMBER_INDEX_ORIGINATOR', 'TRACK_NUMBER_INDEX_ORIGINATOR', 'transcoding', 'value', 15, 'J12.0C7 track number index originator passthrough.', 'J12.0C7 TRACK NUMBER, INDEX ORIGINATOR', 11111),
    J12FieldSpec('INDEX_NUMBER_THIRD_PARTY', 'INDEX_NUMBER_THIRD_PARTY', 'transcoding', 'value', 6, 'J12.0C7 index number third party passthrough.', 'J12.0C7 INDEX NUMBER, THIRD PARTY', 6),
    J12FieldSpec('ELEVATION_ANGLE', 'ELEVATION_ANGLE', 'transcoding', 'value', 15, 'J12.0C7 elevation angle raw passthrough.', 'J12.0C7 ELEVATION ANGLE, 2', 12),
]


def build_j12_0_full_source_message() -> Dict[str, int]:
    return {item.source_field: item.sample_value for item in J12_0_FULL_FIELD_SPECS}


J12_0_FULL_SOURCE_MESSAGE = build_j12_0_full_source_message()


def render_j12_0_source_protocol_content() -> str:
    lines = [
        'MIL-STD-6016D J12.0 full message structured excerpt',
        'Derived from J12.0 MESSAGE SUMMARY / FIELD CODING pages in MIL-STD-6016D and receipt/compliance excerpts.',
        '',
    ]
    for item in J12_0_FULL_FIELD_SPECS:
        unit_suffix = f' | unit: {item.unit}' if item.unit else ''
        bit_text = f'{item.bit_length} bits' if item.bit_length else 'bit length unknown'
        lines.append(
            f'{item.source_field} | {item.source_reference} | {bit_text} | '
            f'mode: {item.conversion_mode} | rule: {item.rule}{unit_suffix} | {item.description}'
        )
    return '\n'.join(lines)



def render_j12_0_target_protocol_content() -> str:
    lines = [
        'Normalized J12.0 full bundle target specification',
        'Each target field must be generated from the complete J12.0 source message.',
        '',
    ]
    for item in J12_0_FULL_FIELD_SPECS:
        unit_suffix = f' ({item.unit})' if item.unit else ''
        lines.append(
            f'- {item.target_field}: generated from {item.source_field} using '
            f'{item.conversion_mode} rule `{item.rule}`{unit_suffix}.'
        )
    return '\n'.join(lines)



def build_j12_0_full_source_protocol() -> Dict[str, str]:
    return {
        'name': 'MIL-STD-6016D J12.0 Mission Assignment Full Message',
        'protocol_type': 'Link16',
        'message_code': 'J12.0',
        'content': render_j12_0_source_protocol_content(),
    }



def build_j12_0_full_target_protocol() -> Dict[str, str]:
    return {
        'name': 'Normalized J12.0 Full Bundle',
        'protocol_type': 'NormalizedLink16J12FullBundle',
        'message_code': 'J12.0-full-normalized',
        'content': render_j12_0_target_protocol_content(),
    }



def build_j12_0_full_bundle_payload() -> Dict[str, Any]:
    return {
        'pdf_basis': str(PDF_BASIS),
        'source_docs': [str(path) for path in SOURCE_DOC_REFERENCES],
        'source_protocol': build_j12_0_full_source_protocol(),
        'target_protocol': build_j12_0_full_target_protocol(),
        'source_message': dict(J12_0_FULL_SOURCE_MESSAGE),
    }



def build_j12_0_full_kb_rules() -> List[Dict[str, Any]]:
    return [item.to_kb_rule() for item in J12_0_FULL_FIELD_SPECS]
