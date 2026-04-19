# MIL-STD-6016D_J12_excerpt.pdf

- protocol_type: J12.0
- message_codes: N/A
- tags: N/A

## Page 1 / Block 19

- block_type: text
- protocol_fields: N/A

MIL-STD-6016D
APPENDIX M
M.1.1.3.7 A record of each mission assignment shall be made.
The mission assignment record shall contain the information listed in
Table M.0.10-1. All mission assignment records shall be available for
display.
M.1.1.3.8 Transaction M.1.2, Transmission of Mission Assignment,
shall be stimulated.
M-61

## Page 2 / Block 20

- block_type: table
- protocol_fields: MISSION_ASSIGNMENT_DISCRETE | 离散值映射表 | 1=REFUEL, 2=ORBIT, 3=RECALL, 4=RETURN_TO_BASE, 5=ENGAGE, 6=PRIORITY_KILL, 8=INVESTIGATE/INTERROGATE, 9=CLEAR_TO_DROP, 11=INTERVENE, 13=AIR-TO-SURFACE, 14=AIR-TO-AIR, 15=SEARCH_AND_RESCUE, 16=COMBAT_AIR_PATROL, 18=LASER_DESIGNATION, 20=CLOSE_AIR_SUPPORT, 22=AERIAL_RECONNAISSANCE, 23=ESCORT, 24=SHADOW, 31=COVER, 32=VISUAL_IDENTIFICATION, 39=SUPPRESSION_OF_ENEMY_AIR_DEFENSES, 40=ARMED_RECONNAISSANCE, 41=ATTACK, 45=FIGHTER_SWEEP, 48=ATTACK_TARGET_COMPLEX

Mission Assignment | J12.0 MAD Value | Transmit Table
Destruction Orders | |
Engage | 5 | Table 5-4-J12.0-1
Priority Kill | 6 | Table 5-4-J12.0-1
Attack | 41 | Table 5-4-J12.0-3
Attack (final execution authority) | 41 | Table 5-4-J12.0-4
Attack Target Complex | 48 | Table 5-4-J12.0-2
Interception Orders | |
Investigate/Interrogate | 8 | Table 5-4-J12.0-11
Intervene | 11 | Table 5-4-J12.0-12
Laser Designation | 18 | Table 5-4-J12.0-9 followed by Table 5-4-J12.0-10 (Laser Designator Information)
Aerial Reconnaissance | 22 | Table 5-4-J12.0-8
Shadow | 24 | Table 5-4-J12.0-11
Cover | 31 | Table 5-4-J12.0-11
Visual Identification | 32 | Table 5-4-J12.0-11
Suppression of Enemy Air Defenses | 39 | Table 5-4-J12.0-7
Armed Reconnaissance | 40 | Table 5-4-J12.0-8
Fighter Sweep | 45 | Table 5-4-J12.0-19
Procedural Orders | |
Refuel | 1 | Table 5-4-J12.0-16
Orbit | 2 | Table 5-4-J12.0-15
Recall | 3 | Table 5-4-J12.0-14
Return To Base | 4 | Table 5-4-J12.0-13

## Page 2 / Block 21

- block_type: table
- protocol_fields: MISSION_ASSIGNMENT_DISCRETE | 离散值映射表 | 1=REFUEL, 2=ORBIT, 3=RECALL, 4=RETURN_TO_BASE, 5=ENGAGE, 6=PRIORITY_KILL, 8=INVESTIGATE/INTERROGATE, 9=CLEAR_TO_DROP, 11=INTERVENE, 13=AIR-TO-SURFACE, 14=AIR-TO-AIR, 15=SEARCH_AND_RESCUE, 16=COMBAT_AIR_PATROL, 18=LASER_DESIGNATION, 20=CLOSE_AIR_SUPPORT, 22=AERIAL_RECONNAISSANCE, 23=ESCORT, 24=SHADOW, 31=COVER, 32=VISUAL_IDENTIFICATION, 39=SUPPRESSION_OF_ENEMY_AIR_DEFENSES, 40=ARMED_RECONNAISSANCE, 41=ATTACK, 45=FIGHTER_SWEEP, 48=ATTACK_TARGET_COMPLEX

Recall | 3 | Table 5-4-J12.0-14
Return To Base | 4 | Table 5-4-J12.0-13
Clear To Drop | 9 | Table 5-4-J12.0-21
Air-To-Surface | 13 | Table 5-4-J12.0-20
Air-To-Air | 14 | Table 5-4-J12.0-20
Search And Rescue | 15 | Table 5-4-J12.0-17
Combat Air Patrol | 16 | Table 5-4-J12.0-18
Close Air Support | 20 | Table 5-4-J12.0-5 followed by Table 5-4-J12.0-6 (CAS Contact Point Information)
Escort | 23 | Table 5-4-J12.0-12

## Page 2 / Block 22

- block_type: text
- protocol_fields: MISSION_ASSIGNMENT_DISCRETE | 离散值映射表 | 5=ENGAGE, 6=PRIORITY_KILL

MIL-STD-6016D
APPENDIX M
M.1.2 TRANSMISSION OF MISSION ASSIGNMENT
M.1.2.1 Mission Assignment Transmission Stimulus
M.1.2.1.1 Transaction M.1.1, Preparation for Transmission of
Mission Assignment (Paragraph M.1.1.3.8).
M.1.2.2 Mission Assignment Transmission Constraints
M.1.2.2.1 If any of the following conditions is true, the host
system shall alert (cat 4) the operator and no further processing of
this transaction shall be performed:
a. The addressee is not held in own database.
b. The addressee is held in own database with NPSI set to other
than 1 (Active-Non Specific), 3 (Conditional Radio Silent), 4 (High
Error Rate), or 5 (No J0.0 Message Being Received).
c. The addressee is held in own database as a J2.0 with
Originator Environment set to other than value 3 (Air).
M.1.2.3 Mission Assignment Transmission Processing
M.1.2.3.1 Mission assignments shall be transmitted in accordance
with the appropriate transmit table indicated in Table M.1.2-1.

## Page 2 / Block 23

- block_type: text
- protocol_fields: MISSION_ASSIGNMENT_DISCRETE | 离散值映射表 | 5=ENGAGE, 6=PRIORITY_KILL

with the appropriate transmit table indicated in Table M.1.2-1.
TABLE M.1.2-1. J12.0 Mission Assignment Transmit Tables (Sheet 1 of
3)
J12.0 MAD
Mission Assignment Transmit Table
Value
Destruction Orders
Engage 5 Table 5-4-J12.0-1
Priority Kill 6 Table 5-4-J12.0-1
M-62

## Page 3 / Block 24

- block_type: text
- protocol_fields: MISSION_ASSIGNMENT_DISCRETE | 离散值映射表 | 1=REFUEL, 2=ORBIT, 3=RECALL, 4=RETURN_TO_BASE, 8=INVESTIGATE/INTERROGATE, 9=CLEAR_TO_DROP, 11=INTERVENE, 13=AIR-TO-SURFACE, 14=AIR-TO-AIR, 15=SEARCH_AND_RESCUE, 16=COMBAT_AIR_PATROL, 18=FOLLOWED_BY, 20=CLOSE_AIR_SUPPORT, 22=AERIAL_RECONNAISSANCE, 24=SHADOW, 31=COVER, 32=VISUAL_IDENTIFICATION, 39=SUPPRESSION_OF_ENEMY_AIR, 40=ARMED_RECONNAISSANCE, 41=ATTACK, 45=FIGHTER_SWEEP, 48=ATTACK_TARGET_COMPLEX

MIL-STD-6016D
APPENDIX M
TABLE M.1.2-1. J12.0 Mission Assignment Transmit Tables (Sheet 2 of
3)
J12.0 MAD
Mission Assignment Transmit Table
Value
Attack 41 Table 5-4-J12.0-3
Attack (final execution
41 Table 5-4-J12.0-4
authority)
Attack Target Complex 48 Table 5-4-J12.0-2
Interception Orders
Investigate/Interrogate 8 Table 5-4-J12.0-11
Intervene 11 Table 5-4-J12.0-12
Laser Designation Table 5-4-J12.0-9
followed by
18 Table 5-4-J12.0-10
(Laser Designator
Information)
Aerial Reconnaissance 22 Table 5-4-J12.0-8
Shadow 24 Table 5-4-J12.0-11
Cover 31 Table 5-4-J12.0-11
Visual Identification 32 Table 5-4-J12.0-11
Suppression of Enemy Air
39 Table 5-4-J12.0-7
Defenses
Armed Reconnaissance 40 Table 5-4-J12.0-8
Fighter Sweep 45 Table 5-4-J12.0-19
Procedural Orders
Refuel 1 Table 5-4-J12.0-16
Orbit 2 Table 5-4-J12.0-15
Recall 3 Table 5-4-J12.0-14
Return To Base 4 Table 5-4-J12.0-13
Clear To Drop 9 Table 5-4-J12.0-21
Air-To-Surface 13 Table 5-4-J12.0-20
Air-To-Air 14 Table 5-4-J12.0-20

## Page 3 / Block 25

- block_type: text
- protocol_fields: MISSION_ASSIGNMENT_DISCRETE | 离散值映射表 | 1=REFUEL, 2=ORBIT, 3=RECALL, 4=RETURN_TO_BASE, 8=INVESTIGATE/INTERROGATE, 9=CLEAR_TO_DROP, 11=INTERVENE, 13=AIR-TO-SURFACE, 14=AIR-TO-AIR, 15=SEARCH_AND_RESCUE, 16=COMBAT_AIR_PATROL, 18=FOLLOWED_BY, 20=CLOSE_AIR_SUPPORT, 22=AERIAL_RECONNAISSANCE, 24=SHADOW, 31=COVER, 32=VISUAL_IDENTIFICATION, 39=SUPPRESSION_OF_ENEMY_AIR, 40=ARMED_RECONNAISSANCE, 41=ATTACK, 45=FIGHTER_SWEEP, 48=ATTACK_TARGET_COMPLEX

Air-To-Surface 13 Table 5-4-J12.0-20
Air-To-Air 14 Table 5-4-J12.0-20
Search And Rescue 15 Table 5-4-J12.0-17
Combat Air Patrol 16 Table 5-4-J12.0-18
Table 5-4-J12.0-5
followed by
Close Air Support 20 Table 5-4-J12.0-6 (CAS
Contact Point
Information)
M-63

## Page 4 / Block 26

- block_type: text
- protocol_fields: MISSION_ASSIGNMENT_DISCRETE | 离散值映射表 | 23=ESCORT

MIL-STD-6016D
APPENDIX M
TABLE M.1.2-1. J12.0 Mission Assignment Transmit Tables (Sheet 3 of
3)
J12.0 MAD
Mission Assignment Transmit Table
Value
Escort 23 Table 5-4-J12.0-12
M.1.2.3.2 With the exception of mission assignments listed in
Table M.1.1-3 having No Statement as a valid objective and Attack
(final execution authority), sufficient information is included in the
J12.0 Mission Assignment message to ensure that the recipient can
establish the target, track, point, line, or single point area which
is the objective of the assignment in cases where the objective is not
already held by the recipient in its database.
M.1.2.3.3 The Environment field in the J12.0C1 word is needed by
the receiver to establish a track file when the mission objective is
not already held. With the exception of mission assignments listed in
Table M.1.1-3 having No Statement as a valid objective and Attack
(final execution authority), the C1 word shall be transmitted, even

## Page 4 / Block 27

- block_type: text
- protocol_fields: MISSION_ASSIGNMENT_DISCRETE | 离散值映射表 | 23=ESCORT

(final execution authority), the C1 word shall be transmitted, even
when all the fields in the C1 word are set to 0 or No Statement
values.
M.1.2.3.4 The Exercise Indicator field in the J12.0C2 word shall
be set to the exercise status of the mission objective since it
pertains to the Identity Amplification Descriptor of the mission
objective rather than to the assignment itself.
M.1.2.3.5 Since the Altitude field in the J12.0C2 word does not
explicitly accommodate negative or unknown altitude values, a No
Statement value shall be specified when the altitude of the mission
objective is either negative or unknown.
M.1.2.3.6 The Elevation field in the J12.0C3 word and the
J12.0C5 word shall be set as follows:
M-64
