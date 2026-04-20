from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from .j12_full_bundle import (
    J12_0_FULL_FIELD_SPECS,
    RECEIPT_COMPLIANCE_MAPPING,
    build_j12_0_full_bundle_payload,
    build_j12_0_full_kb_rules,
)

ROOT = Path(__file__).resolve().parents[2]
PDF_BASIS = ROOT / 'MIL-STD-6016D(无水印)(1).pdf'

J12_6_AIR_STATUS_MAPPING = (
    '0=NO_STATEMENT, 1=ENGAGING, 2=INVESTIGATING, '
    '3=MISSILE_IN_FLIGHT_WEAPON_RELEASED, 4=NEW_SENSOR_TARGET_REPORT, '
    '5=CANCEL_SENSOR_TARGET_REPORT, 6=TRACK_TARGET_DESTROYED, 7=DISENGAGING, '
    '8=TARGET_PARTIALLY_DESTROYED, 9=HEADS_UP, 10=LOCK_ON_PRIMARY_DESIGNATED_TARGET, '
    '11=NOT_EFFECTIVE, 12=COVERING, 13-15=DISUSED'
)


@dataclass(frozen=True)
class BundleFieldSpec:
    source_field: str
    target_field: str
    conversion_mode: str
    rule: str
    bit_length: int | None
    source_reference: str
    sample_value: int
    unit: str | None = None
    description: str = ''

    def to_kb_rule(self, message_code: str) -> Dict[str, Any]:
        return {
            'protocol_type': 'Link16',
            'message_code': message_code,
            'field_name': self.source_field,
            'aliases': [],
            'conversion_mode': self.conversion_mode,
            'formula': self.rule,
            'target_field': self.target_field,
            'unit': self.unit,
            'bit_length': self.bit_length,
            'description': self.description or f'{message_code} full bundle field rule.',
            'source': f'MIL-STD-6016D {message_code} full bundle',
        }


@dataclass(frozen=True)
class MessageBundleSpec:
    message_code: str
    source_name: str
    target_name: str
    purpose: str
    field_specs: Sequence[BundleFieldSpec]
    notes: Sequence[str] = ()

    def build_source_message(self) -> Dict[str, int]:
        return {field.source_field: field.sample_value for field in self.field_specs}

    def render_source_protocol_content(self) -> str:
        lines = [
            f'MIL-STD-6016D {self.message_code} full message structured excerpt',
            self.purpose,
            'Derived from the message summary and field-coding pages of MIL-STD-6016D.',
        ]
        lines.extend(self.notes)
        lines.append('')
        for field in self.field_specs:
            bits = f'{field.bit_length} bits' if field.bit_length is not None else 'bit length unknown'
            unit = f' | unit: {field.unit}' if field.unit else ''
            lines.append(
                f'{field.source_field} | {field.source_reference} | {bits} | '
                f'mode: {field.conversion_mode} | rule: {field.rule}{unit}'
            )
        return '\n'.join(lines)

    def render_target_protocol_content(self) -> str:
        lines = [
            f'Normalized {self.message_code} full bundle target specification',
            f'Each target field must be generated from the complete {self.message_code} source message.',
            '',
        ]
        for field in self.field_specs:
            unit = f' ({field.unit})' if field.unit else ''
            lines.append(
                f'- {field.target_field}: generated from {field.source_field} '
                f'using {field.conversion_mode} rule `{field.rule}`{unit}.'
            )
        return '\n'.join(lines)

    def build_payload(self) -> Dict[str, Any]:
        return {
            'pdf_basis': str(PDF_BASIS),
            'source_docs': [str(PDF_BASIS)],
            'source_protocol': {
                'name': self.source_name,
                'protocol_type': 'Link16',
                'message_code': self.message_code,
                'content': self.render_source_protocol_content(),
            },
            'target_protocol': {
                'name': self.target_name,
                'protocol_type': 'NormalizedLink16FullBundle',
                'message_code': f'{self.message_code}-full-normalized',
                'content': self.render_target_protocol_content(),
            },
            'source_message': self.build_source_message(),
        }

    def build_kb_rules(self) -> List[Dict[str, Any]]:
        return [field.to_kb_rule(self.message_code) for field in self.field_specs]


def _field(
    source_field: str,
    target_field: str,
    conversion_mode: str,
    rule: str,
    bit_length: int | None,
    source_reference: str,
    sample_value: int,
    unit: str | None = None,
    description: str = '',
) -> BundleFieldSpec:
    return BundleFieldSpec(
        source_field=source_field,
        target_field=target_field,
        conversion_mode=conversion_mode,
        rule=rule,
        bit_length=bit_length,
        source_reference=source_reference,
        sample_value=sample_value,
        unit=unit,
        description=description,
    )


J7_0_SPEC = MessageBundleSpec(
    message_code='J7.0',
    source_name='MIL-STD-6016D J7.0 Track Management Full Message (Air Variant)',
    target_name='Normalized J7.0 Full Bundle',
    purpose='Air-track management fixture using the J7.0 air-variant fields as one complete message payload.',
    notes=('For environment-dependent alternates, this fixture selects the AIR platform/activity/specific-type branch.',),
    field_specs=(
        _field('ACTION_TRACK_MANAGEMENT', 'ACTION_TRACK_MANAGEMENT', 'transcoding', 'value', 3, 'J7.0I ACTION, TRACK MANAGEMENT', 2),
        _field('CONTROLLING_UNIT_INDICATOR', 'CONTROLLING_UNIT_INDICATOR', 'transcoding', 'value', 1, 'J7.0I CONTROLLING UNIT INDICATOR', 1),
        _field('TRACK_NUMBER_REFERENCE', 'TRACK_NUMBER_REFERENCE', 'transcoding', 'value', 19, 'J7.0I TRACK NUMBER, REFERENCE', 31001),
        _field('AIR_PLATFORM', 'AIR_PLATFORM', 'transcoding', 'value', 6, 'J7.0I AIR PLATFORM', 12),
        _field('AIR_ACTIVITY', 'AIR_ACTIVITY', 'transcoding', 'value', 7, 'J7.0I AIR ACTIVITY', 45),
        _field('ENVIRONMENT', 'ENVIRONMENT', 'transcoding', 'value', 3, 'J7.0I ENVIRONMENT', 2),
        _field('IDENTITY', 'IDENTITY', 'transcoding', 'value', 3, 'J7.0I IDENTITY', 3),
        _field('IDENTITY_AMPLIFYING', 'IDENTITY_AMPLIFYING', 'transcoding', 'value', 3, 'J7.0I IDENTITY AMPLIFYING', 1),
        _field('DESCRIPTOR', 'DESCRIPTOR', 'transcoding', 'value', None, 'J7.0I DESCRIPTOR', 9),
        _field('SPECIAL_INTEREST_INDICATOR', 'SPECIAL_INTEREST_INDICATOR', 'transcoding', 'value', 1, 'J7.0I SPECIAL INTEREST INDICATOR', 1),
        _field('AIR_SPECIFIC_TYPE', 'AIR_SPECIFIC_TYPE', 'transcoding', 'value', 12, 'J7.0C1 AIR SPECIFIC TYPE', 640),
    ),
)

J9_0_SPEC = MessageBundleSpec(
    message_code='J9.0',
    source_name='MIL-STD-6016D J9.0 Command Full Message',
    target_name='Normalized J9.0 Full Bundle',
    purpose='Command message fixture covering the non-disused J9.0 initial, extension, and continuation fields.',
    field_specs=(
        _field('TRACK_NUMBER_ADDRESSEE', 'TRACK_NUMBER_ADDRESSEE', 'transcoding', 'value', 15, 'J9.0I TRACK NUMBER, ADDRESSEE', 1201),
        _field('COMMAND', 'COMMAND', 'transcoding', 'value', 5, 'J9.0I COMMAND', 7),
        _field('THREAT_WARNING_CONDITION', 'THREAT_WARNING_CONDITION', 'transcoding', 'value', 2, 'J9.0I THREAT WARNING CONDITION', 2),
        _field('WEAPON_TYPE', 'WEAPON_TYPE', 'transcoding', 'value', 2, 'J9.0I WEAPON TYPE', 1),
        _field('TRACK_NUMBER_OBJECTIVE', 'TRACK_NUMBER_OBJECTIVE', 'transcoding', 'value', 19, 'J9.0I TRACK NUMBER, OBJECTIVE', 48111),
        _field('NUMBER_OF_ASSOCIATED_DMPIS', 'NUMBER_OF_ASSOCIATED_DMPIS', 'transcoding', 'value', 4, 'J9.0I NUMBER OF ASSOCIATED DMPIS', 2),
        _field('RECEIPT_COMPLIANCE', 'RECEIPT_COMPLIANCE_LABEL', 'mapping', RECEIPT_COMPLIANCE_MAPPING, 5, 'J9.0I RECEIPT/COMPLIANCE', 3),
        _field('RECURRENCE_RATE_RECEIPT_COMPLIANCE', 'RECURRENCE_RATE_RECEIPT_COMPLIANCE', 'transcoding', 'value', 4, 'J9.0I RECURRENCE RATE, RECEIPT/COMPLIANCE', 2),
        _field('HOUR', 'HOUR', 'transcoding', 'value', 5, 'J9.0C2 HOUR', 14),
        _field('MINUTE', 'MINUTE', 'transcoding', 'value', 6, 'J9.0C2 MINUTE', 28),
        _field('SECOND', 'SECOND', 'transcoding', 'value', 6, 'J9.0C2 SECOND', 35),
        _field('NUMBER_OF_MISSILES', 'NUMBER_OF_MISSILES', 'transcoding', 'value', 3, 'J9.0C2 NUMBER OF MISSILES', 2),
        _field('DUTY_ASSIGNMENT', 'DUTY_ASSIGNMENT', 'transcoding', 'value', 6, 'J9.0E0 DUTY ASSIGNMENT', 5),
        _field('COMMAND_MISSION', 'COMMAND_MISSION', 'transcoding', 'value', 5, 'J9.0E0 COMMAND MISSION', 6),
        _field('NUMBER_OF_AIRCRAFT', 'NUMBER_OF_AIRCRAFT', 'transcoding', 'value', 6, 'J9.0E0 NUMBER OF AIRCRAFT', 4),
        _field('THREAT_WARNING_ENVIRONMENT', 'THREAT_WARNING_ENVIRONMENT', 'transcoding', 'value', 3, 'J9.0E0 THREAT WARNING ENVIRONMENT', 2),
        _field('DUTY_ASSIGNMENT_FUNCTIONAL_AREA', 'DUTY_ASSIGNMENT_FUNCTIONAL_AREA', 'transcoding', 'value', 4, 'J9.0E0 DUTY ASSIGNMENT FUNCTIONAL AREA', 3),
        _field('TRACK_NUMBER_FRIENDLY_WEAPON', 'TRACK_NUMBER_FRIENDLY_WEAPON', 'transcoding', 'value', 19, 'J9.0E0 TRACK NUMBER, FRIENDLY WEAPON', 51120),
        _field('VOICE_CALL_SIGN', 'VOICE_CALL_SIGN', 'transcoding', 'value', 24, 'J9.0C1 VOICE CALL SIGN', 703710),
        _field('VOICE_FREQUENCY_CHANNEL', 'VOICE_FREQUENCY_CHANNEL', 'transcoding', 'value', 13, 'J9.0C1 VOICE FREQUENCY/CHANNEL', 421),
        _field('CONTROL_CHANNEL', 'CONTROL_CHANNEL', 'transcoding', 'value', 7, 'J9.0C1 CONTROL CHANNEL', 12),
        _field('SECURE_RADIO_INDICATOR', 'SECURE_RADIO_INDICATOR', 'transcoding', 'value', 1, 'J9.0C1 SECURE RADIO INDICATOR', 1),
        _field('METHOD_OF_FIRE', 'METHOD_OF_FIRE', 'transcoding', 'value', 3, 'J9.0C1 METHOD OF FIRE', 2),
    ),
)

J10_6_SPEC = MessageBundleSpec(
    message_code='J10.6',
    source_name='MIL-STD-6016D J10.6 Pairing Full Message',
    target_name='Normalized J10.6 Full Bundle',
    purpose='Pairing message fixture using the complete non-spare J10.6 initial word.',
    field_specs=(
        _field('TRACK_NUMBER_REFERENCE', 'TRACK_NUMBER_REFERENCE', 'transcoding', 'value', 19, 'J10.6I TRACK NUMBER, REFERENCE', 22017),
        _field('TRACK_NUMBER_OBJECTIVE', 'TRACK_NUMBER_OBJECTIVE', 'transcoding', 'value', 19, 'J10.6I TRACK NUMBER, OBJECTIVE', 22018),
        _field('PAIRING_ACTION', 'PAIRING_ACTION', 'transcoding', 'value', 4, 'J10.6I PAIRING ACTION', 1),
    ),
)

J12_1_SPEC = MessageBundleSpec(
    message_code='J12.1',
    source_name='MIL-STD-6016D J12.1 Vector Full Message',
    target_name='Normalized J12.1 Full Bundle',
    purpose='Vector message fixture covering the complete non-spare J12.1 field set.',
    field_specs=(
        _field('TRACK_NUMBER_ADDRESSEE', 'TRACK_NUMBER_ADDRESSEE', 'transcoding', 'value', 15, 'J12.1I TRACK NUMBER, ADDRESSEE', 4096),
        _field('COURSE', 'COURSE', 'transcoding', 'value', 9, 'J12.1I COURSE', 196),
        _field('ALTITUDE_25FT', 'ALTITUDE_FT', 'transcoding', 'unsigned(ALTITUDE_25FT, bits=13) * 25', 13, 'J12.1I ALTITUDE, 25 FT', 820, 'ft'),
        _field('VECTOR_DISCRETE', 'VECTOR_DISCRETE', 'transcoding', 'value', 6, 'J12.1I VECTOR DISCRETE', 9),
        _field('SPEED', 'SPEED', 'transcoding', 'value', 11, 'J12.1I SPEED', 480),
        _field('TIME_TO_INTERCEPT', 'TIME_TO_INTERCEPT', 'transcoding', 'value', 8, 'J12.1E0 TIME TO INTERCEPT', 75),
        _field('RECEIPT_COMPLIANCE', 'RECEIPT_COMPLIANCE_LABEL', 'mapping', RECEIPT_COMPLIANCE_MAPPING, 5, 'J12.1E0 RECEIPT/COMPLIANCE', 3),
        _field('RECURRENCE_RATE_RECEIPT_COMPLIANCE', 'RECURRENCE_RATE_RECEIPT_COMPLIANCE', 'transcoding', 'value', 4, 'J12.1E0 RECURRENCE RATE, RECEIPT/COMPLIANCE', 1),
    ),
)

J12_6_SPEC = MessageBundleSpec(
    message_code='J12.6',
    source_name='MIL-STD-6016D J12.6 Target Sorting Full Message (Air Variant)',
    target_name='Normalized J12.6 Full Bundle',
    purpose='Target sorting fixture using the air-specific branches of J12.6 for a whole-message batch payload.',
    notes=('Environment-dependent alternates select the AIR specific-type and AIR ambiguous-type branches.',),
    field_specs=(
        _field('STATUS_INFORMATION_DISCRETE', 'STATUS_INFORMATION_LABEL', 'mapping', J12_6_AIR_STATUS_MAPPING, 4, 'J12.6I STATUS INFORMATION DISCRETE', 1),
        _field('TARGET_POSITION_QUALITY', 'TARGET_POSITION_QUALITY', 'transcoding', 'value', 2, 'J12.6I TARGET POSITION QUALITY', 2),
        _field('ENVIRONMENT', 'ENVIRONMENT', 'transcoding', 'value', 3, 'J12.6I ENVIRONMENT', 2),
        _field('ORIGIN_OF_INDEX_NUMBER', 'ORIGIN_OF_INDEX_NUMBER', 'transcoding', 'value', 1, 'J12.6I ORIGIN OF INDEX NUMBER', 1),
        _field('INDEX_NUMBER', 'INDEX_NUMBER', 'transcoding', 'value', 6, 'J12.6I INDEX NUMBER', 24),
        _field('LATITUDE_00103MIN', 'LATITUDE_DEG', 'transcoding', 'signed(LATITUDE_00103MIN, bits=20) * 0.0103 / 60', 20, 'J12.6I LATITUDE, 0.0103 MINUTE', 1800, 'degree'),
        _field('LONGITUDE_00103MIN', 'LONGITUDE_DEG', 'transcoding', 'signed(LONGITUDE_00103MIN, bits=21) * 0.0103 / 60', 21, 'J12.6I LONGITUDE, 0.0103 MINUTE', 7200, 'degree'),
        _field('TRACK_NUMBER_OBJECTIVE', 'TRACK_NUMBER_OBJECTIVE', 'transcoding', 'value', 19, 'J12.6E0 TRACK NUMBER, OBJECTIVE', 35001),
        _field('CORRELATION_INDICATOR_TN_IN', 'CORRELATION_INDICATOR_TN_IN', 'transcoding', 'value', 1, 'J12.6E0 CORRELATION INDICATOR, TN/IN', 1),
        _field('COURSE', 'COURSE', 'transcoding', 'value', 9, 'J12.6E0 COURSE', 188),
        _field('SPEED', 'SPEED', 'transcoding', 'value', 11, 'J12.6E0 SPEED', 520),
        _field('ALTITUDE_100FT', 'ALTITUDE_FT', 'transcoding', 'unsigned(ALTITUDE_100FT, bits=11) * 100', 11, 'J12.6E0 ALTITUDE, 100 FT', 240, 'ft'),
        _field('IDENTITY', 'IDENTITY', 'transcoding', 'value', 3, 'J12.6E0 IDENTITY', 3),
        _field('IDENTITY_AMPLIFYING', 'IDENTITY_AMPLIFYING', 'transcoding', 'value', 3, 'J12.6E0 IDENTITY AMPLIFYING', 1),
        _field('DESCRIPTOR', 'DESCRIPTOR', 'transcoding', 'value', None, 'J12.6E0 DESCRIPTOR', 10),
        _field('EXERCISE_INDICATOR', 'EXERCISE_INDICATOR', 'transcoding', 'value', 1, 'J12.6E0 EXERCISE INDICATOR', 0),
        _field('AIR_SPECIFIC_TYPE', 'AIR_SPECIFIC_TYPE', 'transcoding', 'value', 12, 'J12.6E0 AIR SPECIFIC TYPE', 512),
        _field('SPECIAL_INTEREST_INDICATOR', 'SPECIAL_INTEREST_INDICATOR', 'transcoding', 'value', 1, 'J12.6E0 SPECIAL INTEREST INDICATOR', 1),
        _field('TRACK_NUMBER_ADDRESSEE', 'TRACK_NUMBER_ADDRESSEE', 'transcoding', 'value', 15, 'J12.6C1 TRACK NUMBER, ADDRESSEE', 2048),
        _field('POINTER', 'POINTER', 'transcoding', 'value', 1, 'J12.6C1 POINTER', 1),
        _field('WEAPON_SYSTEM', 'WEAPON_SYSTEM', 'transcoding', 'value', 5, 'J12.6C1 WEAPON SYSTEM', 8),
        _field('WEAPON_ENGAGEMENT_STATUS', 'WEAPON_ENGAGEMENT_STATUS', 'transcoding', 'value', 4, 'J12.6C1 WEAPON ENGAGEMENT STATUS', 6),
        _field('TRACK_NUMBER_REFERENCE', 'TRACK_NUMBER_REFERENCE', 'transcoding', 'value', 19, 'J12.6C1 TRACK NUMBER, REFERENCE', 35002),
        _field('MODE_III_IFF_INTERROGATION', 'MODE_III_IFF_INTERROGATION', 'transcoding', 'value', 2, 'J12.6C1 MODE III IFF INTERROGATION', 2),
        _field('MODE_IV_INDICATOR', 'MODE_IV_INDICATOR', 'transcoding', 'value', 2, 'J12.6C1 MODE IV INDICATOR', 1),
        _field('STRENGTH', 'STRENGTH', 'transcoding', 'value', 4, 'J12.6C1 STRENGTH', 4),
        _field('END_OF_TARGET_REPORTING_CYCLE', 'END_OF_TARGET_REPORTING_CYCLE', 'transcoding', 'value', 1, 'J12.6C2 END OF TARGET REPORTING CYCLE', 1),
        _field('TIME_INDICATOR', 'TIME_INDICATOR', 'transcoding', 'value', 3, 'J12.6C2 TIME INDICATOR', 3),
        _field('MINUTE', 'MINUTE', 'transcoding', 'value', 6, 'J12.6C2 MINUTE', 22),
        _field('SECOND', 'SECOND', 'transcoding', 'value', 6, 'J12.6C2 SECOND', 41),
        _field('HUNDREDTHS', 'HUNDREDTHS', 'transcoding', 'value', 7, 'J12.6C2 HUNDREDTHS', 12),
        _field('EMITTER_PRF', 'EMITTER_PRF', 'transcoding', 'value', 3, 'J12.6C2 EMITTER PRF', 4),
        _field('EMITTER_STATUS', 'EMITTER_STATUS', 'transcoding', 'value', 3, 'J12.6C2 EMITTER STATUS', 2),
        _field('JAMMER_TYPE', 'JAMMER_TYPE', 'transcoding', 'value', 3, 'J12.6C2 JAMMER TYPE', 1),
        _field('TRACK_NUMBER_INDEX_ORIGINATOR', 'TRACK_NUMBER_INDEX_ORIGINATOR', 'transcoding', 'value', 15, 'J12.6C2 TRACK NUMBER, INDEX ORIGINATOR', 1987),
        _field('INDEX_NUMBER_THIRD_PARTY', 'INDEX_NUMBER_THIRD_PARTY', 'transcoding', 'value', 6, 'J12.6C2 INDEX NUMBER, THIRD PARTY', 17),
        _field('SENSOR_TYPE', 'SENSOR_TYPE', 'transcoding', 'value', 4, 'J12.6C2 SENSOR TYPE', 3),
        _field('FUSION_TYPE', 'FUSION_TYPE', 'transcoding', 'value', 1, 'J12.6C2 FUSION TYPE', 1),
        _field('SLANT_RANGE_POSITION_UNCERTAINTY', 'SLANT_RANGE_POSITION_UNCERTAINTY', 'transcoding', 'value', 4, 'J12.6C3 SLANT RANGE POSITION UNCERTAINTY', 3),
        _field('CROSS_RANGE_POSITION_UNCERTAINTY', 'CROSS_RANGE_POSITION_UNCERTAINTY', 'transcoding', 'value', 4, 'J12.6C3 CROSS RANGE POSITION UNCERTAINTY', 4),
        _field('VERTICAL_POSITION_UNCERTAINTY', 'VERTICAL_POSITION_UNCERTAINTY', 'transcoding', 'value', 4, 'J12.6C3 VERTICAL POSITION UNCERTAINTY', 2),
        _field('UNCERTAINTY_ORIENTATION', 'UNCERTAINTY_ORIENTATION', 'transcoding', 'value', 11, 'J12.6C3 UNCERTAINTY ORIENTATION', 245),
        _field('UNCERTAINTY_TILT', 'UNCERTAINTY_TILT', 'transcoding', 'value', 11, 'J12.6C3 UNCERTAINTY TILT', 17),
        _field('VELOCITY_DOWN', 'VELOCITY_DOWN', 'transcoding', 'value', 13, 'J12.6C3 VELOCITY, DOWN', 120),
        _field('VELOCITY_NORTH_UNCERTAINTY', 'VELOCITY_NORTH_UNCERTAINTY', 'transcoding', 'value', 4, 'J12.6C3 VELOCITY NORTH UNCERTAINTY', 4),
        _field('VELOCITY_EAST_UNCERTAINTY', 'VELOCITY_EAST_UNCERTAINTY', 'transcoding', 'value', 4, 'J12.6C3 VELOCITY EAST UNCERTAINTY', 3),
        _field('VELOCITY_DOWN_UNCERTAINTY', 'VELOCITY_DOWN_UNCERTAINTY', 'transcoding', 'value', 4, 'J12.6C3 VELOCITY DOWN UNCERTAINTY', 2),
        _field('PROBABILITY_OF_OWN_FORCES', 'PROBABILITY_OF_OWN_FORCES', 'transcoding', 'value', 9, 'J12.6C4 PROBABILITY OF OWN FORCES', 400),
        _field('PROBABILITY_OF_ENEMY_FORCES', 'PROBABILITY_OF_ENEMY_FORCES', 'transcoding', 'value', 9, 'J12.6C4 PROBABILITY OF ENEMY FORCES', 120),
        _field('AIR_AMBIGUOUS_TYPE_1', 'AIR_AMBIGUOUS_TYPE_1', 'transcoding', 'value', 12, 'J12.6C4 AIR AMBIGUOUS TYPE 1', 620),
        _field('AIR_AMBIGUOUS_TYPE_2', 'AIR_AMBIGUOUS_TYPE_2', 'transcoding', 'value', 12, 'J12.6C4 AIR AMBIGUOUS TYPE 2', 730),
        _field('AMBIGUOUS_TYPE_1_PROBABILITY', 'AMBIGUOUS_TYPE_1_PROBABILITY', 'transcoding', 'value', 9, 'J12.6C4 AMBIGUOUS TYPE 1 PROBABILITY', 260),
        _field('AMBIGUOUS_TYPE_2_PROBABILITY', 'AMBIGUOUS_TYPE_2_PROBABILITY', 'transcoding', 'value', 9, 'J12.6C4 AMBIGUOUS TYPE 2 PROBABILITY', 180),
        _field('FUTURE_EVENT_DESCRIPTOR', 'FUTURE_EVENT_DESCRIPTOR', 'transcoding', 'value', 3, 'J12.6C5 FUTURE EVENT DESCRIPTOR', 2),
        _field('TIME_OF_EVENT_MINUTE', 'TIME_OF_EVENT_MINUTE', 'transcoding', 'value', 6, 'J12.6C5 TIME OF EVENT, MINUTE', 23),
        _field('TIME_OF_EVENT_SECOND', 'TIME_OF_EVENT_SECOND', 'transcoding', 'value', 6, 'J12.6C5 TIME OF EVENT, SECOND', 11),
        _field('TIME_OF_EVENT_HUNDREDTHS', 'TIME_OF_EVENT_HUNDREDTHS', 'transcoding', 'value', 7, 'J12.6C5 TIME OF EVENT, HUNDREDTHS', 9),
        _field('EVENT_DURATION_SECONDS', 'EVENT_DURATION_SECONDS', 'transcoding', 'value', 6, 'J12.6C5 EVENT DURATION, SECONDS', 18),
        _field('EVENT_DURATION_HUNDREDTHS', 'EVENT_DURATION_HUNDREDTHS', 'transcoding', 'value', 7, 'J12.6C5 EVENT DURATION, HUNDREDTHS', 22),
        _field('SENSOR_TYPE_INDICATOR', 'SENSOR_TYPE_INDICATOR', 'transcoding', 'value', 3, 'J12.6C6 SENSOR TYPE INDICATOR', 3),
        _field('SENSOR_STATUS_COMMAND_INDICATOR', 'SENSOR_STATUS_COMMAND_INDICATOR', 'transcoding', 'value', 1, 'J12.6C6 SENSOR STATUS/COMMAND INDICATOR', 1),
        _field('SENSOR_CHANNEL_CODE', 'SENSOR_CHANNEL_CODE', 'transcoding', 'value', 12, 'J12.6C6 SENSOR CHANNEL/CODE', 840),
        _field('SENSOR_CHANNEL_SET', 'SENSOR_CHANNEL_SET', 'transcoding', 'value', 5, 'J12.6C6 SENSOR CHANNEL SET', 9),
        _field('FREQUENCY_AGILITY_INDICATOR', 'FREQUENCY_AGILITY_INDICATOR', 'transcoding', 'value', 2, 'J12.6C6 FREQUENCY AGILITY INDICATOR', 1),
        _field('PDI_INDICATOR', 'PDI_INDICATOR', 'transcoding', 'value', 1, 'J12.6C6 PDI INDICATOR', 1),
        _field('PDI_MISSILE_DATA_LINK_CHANNEL', 'PDI_MISSILE_DATA_LINK_CHANNEL', 'transcoding', 'value', 9, 'J12.6C6 PDI/MISSILE DATA LINK CHANNEL', 77),
        _field('AIR_GROUND_WEAPON_DATA_LINK_CHANNEL', 'AIR_GROUND_WEAPON_DATA_LINK_CHANNEL', 'transcoding', 'value', 5, 'J12.6C6 AIR-GROUND WEAPON DATA LINK CHANNEL', 14),
        _field('WEAPON_ID', 'WEAPON_ID', 'transcoding', 'value', 5, 'J12.6C6 WEAPON ID', 11),
        _field('RADAR_PRF', 'RADAR_PRF', 'transcoding', 'value', 3, 'J12.6C6 RADAR PRF', 4),
        _field('ARM_THREAT_NUMBER', 'ARM_THREAT_NUMBER', 'transcoding', 'value', 8, 'J12.6C6 ARM THREAT NUMBER', 120),
        _field('ARM_TYPE', 'ARM_TYPE', 'transcoding', 'value', 4, 'J12.6C6 ARM TYPE', 3),
        _field('VARIANCE_XX', 'VARIANCE_XX', 'transcoding', 'value', 8, 'J12.6C7 VARIANCE XX', 10),
        _field('VARIANCE_YY', 'VARIANCE_YY', 'transcoding', 'value', 8, 'J12.6C7 VARIANCE YY', 12),
        _field('VARIANCE_ZZ', 'VARIANCE_ZZ', 'transcoding', 'value', 8, 'J12.6C7 VARIANCE ZZ', 15),
        _field('VARIANCE_XY', 'VARIANCE_XY', 'transcoding', 'value', 9, 'J12.6C7 VARIANCE XY', 64),
        _field('VARIANCE_XZ', 'VARIANCE_XZ', 'transcoding', 'value', 9, 'J12.6C7 VARIANCE XZ', 42),
        _field('VARIANCE_YZ', 'VARIANCE_YZ', 'transcoding', 'value', 9, 'J12.6C7 VARIANCE YZ', 21),
        _field('DIVE_ANGLE', 'DIVE_ANGLE', 'transcoding', 'value', 8, 'J12.6C7 DIVE ANGLE', 30),
        _field('LATITUDE_LSB_2', 'LATITUDE_LSB_2', 'transcoding', 'value', 5, 'J12.6C8 LATITUDE, LSB 2', 3),
        _field('LONGITUDE_LSB_2', 'LONGITUDE_LSB_2', 'transcoding', 'value', 5, 'J12.6C8 LONGITUDE, LSB 2', 5),
        _field('ALTITUDE_LSBS', 'ALTITUDE_LSBS', 'transcoding', 'value', 6, 'J12.6C8 ALTITUDE, LSBS', 7),
        _field('X_POSITION_UNCERTAINTY', 'X_POSITION_UNCERTAINTY', 'transcoding', 'value', 5, 'J12.6C8 X POSITION UNCERTAINTY', 4),
        _field('Y_POSITION_UNCERTAINTY', 'Y_POSITION_UNCERTAINTY', 'transcoding', 'value', 5, 'J12.6C8 Y POSITION UNCERTAINTY', 3),
        _field('Z_POSITION_UNCERTAINTY', 'Z_POSITION_UNCERTAINTY', 'transcoding', 'value', 3, 'J12.6C8 Z POSITION UNCERTAINTY', 2),
        _field('POSITION_UNCERTAINTY_ORIENTATION', 'POSITION_UNCERTAINTY_ORIENTATION', 'transcoding', 'value', 10, 'J12.6C8 POSITION UNCERTAINTY ORIENTATION', 120),
        _field('POSITION_UNCERTAINTY_TILT', 'POSITION_UNCERTAINTY_TILT', 'transcoding', 'value', 5, 'J12.6C8 POSITION UNCERTAINTY TILT', 5),
        _field('TARGET_X_VELOCITY_UNCERTAINTY', 'TARGET_X_VELOCITY_UNCERTAINTY', 'transcoding', 'value', 2, 'J12.6C8 TARGET X VELOCITY UNCERTAINTY', 1),
        _field('TARGET_Y_VELOCITY_UNCERTAINTY', 'TARGET_Y_VELOCITY_UNCERTAINTY', 'transcoding', 'value', 2, 'J12.6C8 TARGET Y VELOCITY UNCERTAINTY', 2),
        _field('TARGET_VELOCITY_UNCERTAINTY_ORIENTATION', 'TARGET_VELOCITY_UNCERTAINTY_ORIENTATION', 'transcoding', 'value', 3, 'J12.6C8 TARGET VELOCITY UNCERTAINTY ORIENTATION', 4),
        _field('TARGET_VELOCITY_UNCERTAINTY_VALIDITY_INDICATOR', 'TARGET_VELOCITY_UNCERTAINTY_VALIDITY_INDICATOR', 'transcoding', 'value', 1, 'J12.6C8 TARGET VELOCITY UNCERTAINTY VALIDITY INDICATOR', 1),
        _field('HAE_ADJUSTMENT_1', 'HAE_ADJUSTMENT_1', 'transcoding', 'value', 11, 'J12.6C8 HAE ADJUSTMENT 1', 40),
    ),
)


MESSAGE_BUNDLE_REGISTRY: Dict[str, MessageBundleSpec] = {
    J7_0_SPEC.message_code: J7_0_SPEC,
    J9_0_SPEC.message_code: J9_0_SPEC,
    J10_6_SPEC.message_code: J10_6_SPEC,
    J12_1_SPEC.message_code: J12_1_SPEC,
    J12_6_SPEC.message_code: J12_6_SPEC,
}
DEFAULT_MESSAGE_CODES = ('J7.0', 'J9.0', 'J10.6', 'J12.0', 'J12.1', 'J12.6')


def _build_j12_0_adapted_spec() -> MessageBundleSpec:
    return MessageBundleSpec(
        message_code='J12.0',
        source_name='MIL-STD-6016D J12.0 Mission Assignment Full Message',
        target_name='Normalized J12.0 Full Bundle',
        purpose='Mission assignment fixture reusing the existing J12.0 whole-message specification.',
        field_specs=tuple(
            BundleFieldSpec(
                source_field=item.source_field,
                target_field=item.target_field,
                conversion_mode=item.conversion_mode,
                rule=item.rule,
                bit_length=item.bit_length,
                source_reference=item.source_reference,
                sample_value=item.sample_value,
                unit=item.unit,
                description=item.description,
            )
            for item in J12_0_FULL_FIELD_SPECS
        ),
    )


def list_full_bundle_message_codes() -> List[str]:
    return list(DEFAULT_MESSAGE_CODES)


def get_full_bundle_message_spec(message_code: str) -> MessageBundleSpec:
    normalized = str(message_code or '').strip().upper()
    if normalized == 'J12.0':
        return _build_j12_0_adapted_spec()
    spec = MESSAGE_BUNDLE_REGISTRY.get(normalized)
    if spec is None:
        raise KeyError(f'unsupported message_code: {message_code}')
    return spec


def build_full_bundle_payload(message_code: str) -> Dict[str, Any]:
    if str(message_code or '').strip().upper() == 'J12.0':
        return build_j12_0_full_bundle_payload()
    return get_full_bundle_message_spec(message_code).build_payload()


def build_full_bundle_kb_rules(message_code: str) -> List[Dict[str, Any]]:
    if str(message_code or '').strip().upper() == 'J12.0':
        return build_j12_0_full_kb_rules()
    return get_full_bundle_message_spec(message_code).build_kb_rules()


def build_multi_message_full_bundle_payloads(message_codes: Iterable[str] | None = None) -> List[Dict[str, Any]]:
    codes = list(message_codes or DEFAULT_MESSAGE_CODES)
    return [build_full_bundle_payload(code) for code in codes]


def build_multi_message_full_kb_rules(message_codes: Iterable[str] | None = None) -> List[Dict[str, Any]]:
    rules: List[Dict[str, Any]] = []
    for code in message_codes or DEFAULT_MESSAGE_CODES:
        rules.extend(build_full_bundle_kb_rules(code))
    return rules
