import pandas as pd
import json

#Safely parses JSON strings from a column (returns an empty dict on failure)
def safe_json_parse(x):
    try:
        return json.loads(x)
    except Exception:
        return {}

# Load raw data
df = pd.read_excel("raw_data.xlsx")

# Add surrogate comm_id used as the primary key in FACT table
df['comm_id'] = df.index + 1

# Parse raw_content JSON
df['raw_content_parsed'] = df['raw_content'].apply(safe_json_parse)

# Extract important fields
fields = ['id', 'title', 'duration', 'audio_url', 'video_url', 'calendar_id',
          'transcript_url', 'source_id', 'start_time', 'is_processed',
          'ingested_at', 'processed_at']
for f in fields:
    df[f'record_{f}'] = df['raw_content_parsed'].apply(lambda x: x.get(f))

# Fill nulls to avoid merge issues
df['comm_type'] = df['comm_type'].fillna('')
df['subject'] = df['subject'].fillna('')

# Dimension tables
dim_comm_type = df[['comm_type']].drop_duplicates().reset_index(drop=True)
dim_comm_type['comm_type_id'] = dim_comm_type.index + 1

dim_subject = df[['subject']].drop_duplicates().reset_index(drop=True)
dim_subject['subject_id'] = dim_subject.index + 1

dim_calendar = df[['record_calendar_id']].drop_duplicates().reset_index(drop=True)
dim_calendar['calendar_id'] = dim_calendar.index + 1

dim_audio = df[['record_audio_url']].drop_duplicates().reset_index(drop=True)
dim_audio['audio_id'] = dim_audio.index + 1

dim_video = df[['record_video_url']].drop_duplicates().reset_index(drop=True)
dim_video['video_id'] = dim_video.index + 1

dim_transcript = df[['record_transcript_url']].drop_duplicates().reset_index(drop=True)
dim_transcript['transcript_id'] = dim_transcript.index + 1

# Build dim_user from meeting_attendees, speakers, and participants
user_rows = []

for record in df['raw_content_parsed']:
    # From meeting_attendees
    for u in record.get('meeting_attendees', []):
        user_rows.append({
            'name': u.get('name'),
            'email': u.get('email'),
            'location': u.get('location'),
            'displayName': u.get('displayName'),
            'phoneNumber': u.get('phoneNumber')
        })

    # From speakers
    for s in record.get('speakers', []):
        user_rows.append({
            'name': s.get('name'),
            'email': None,
            'location': None,
            'displayName': None,
            'phoneNumber': None
        })

    # From participants
    for p in record.get('participants', []):
        user_rows.append({
            'name': None,
            'email': p,
            'location': None,
            'displayName': None,
            'phoneNumber': None
        })

dim_user = pd.DataFrame(user_rows).drop_duplicates().reset_index(drop=True)
dim_user['user_id'] = dim_user.index + 1
dim_user = dim_user[['user_id', 'name', 'email', 'location', 'displayName', 'phoneNumber']]

# Merge for fact_communication
fact_df = df.merge(dim_comm_type, on='comm_type', how='left') \
            .merge(dim_subject, on='subject', how='left') \
            .merge(dim_calendar, on='record_calendar_id', how='left') \
            .merge(dim_audio, on='record_audio_url', how='left') \
            .merge(dim_video, on='record_video_url', how='left') \
            .merge(dim_transcript, on='record_transcript_url', how='left')

fact_communication = fact_df[[
    'comm_id', 'record_id', 'source_id', 'comm_type_id', 'subject_id',
    'calendar_id', 'audio_id', 'video_id', 'transcript_id',
    'record_start_time', 'record_ingested_at', 'record_processed_at',
    'record_is_processed', 'record_title', 'record_duration'
]].rename(columns={
    'record_id': 'raw_id',
    'record_start_time': 'datetime_id',
    'record_ingested_at': 'ingested_at',
    'record_processed_at': 'processed_at',
    'record_is_processed': 'is_processed',
    'record_title': 'raw_title',
    'record_duration': 'raw_duration'
})

# Build bridge_comm_user with flags
bridge_rows = []
for idx, row in df[['comm_id', 'raw_content_parsed']].iterrows():
    comm_id = row['comm_id']
    record = row['raw_content_parsed']
    
    attendees = record.get('meeting_attendees', [])
    participants = record.get('participants', [])
    speakers = [s.get('name') for s in record.get('speakers', [])]
    organizer_email = record.get('organizer_email')

    for u in attendees:
        user_email = u.get('email')
        user_name = u.get('name')

        matched_user = pd.DataFrame()
        if user_email:
            matched_user = dim_user[dim_user['email'] == user_email]
        if matched_user.empty and user_name:
            matched_user = dim_user[dim_user['name'] == user_name]

        if matched_user.empty:
            continue

        user_id = matched_user.iloc[0]['user_id']

        bridge_rows.append({
            'comm_id': comm_id,
            'user_id': user_id,
            'isAttendee': True,
            'isParticipant': user_email in participants if user_email else False,
            'isSpeaker': user_name in speakers if user_name else False,
            'isOrganiser': user_email == organizer_email if user_email else False
        })

bridge_comm_user = pd.DataFrame(bridge_rows).drop_duplicates().reset_index(drop=True)

# Export to Excel
with pd.ExcelWriter('star_schema_output.xlsx') as writer:
    dim_comm_type.to_excel(writer, sheet_name='dim_comm_type', index=False)
    dim_subject.to_excel(writer, sheet_name='dim_subject', index=False)
    dim_user.to_excel(writer, sheet_name='dim_user', index=False)
    dim_calendar.to_excel(writer, sheet_name='dim_calendar', index=False)
    dim_audio.to_excel(writer, sheet_name='dim_audio', index=False)
    dim_video.to_excel(writer, sheet_name='dim_video', index=False)
    dim_transcript.to_excel(writer, sheet_name='dim_transcript', index=False)
    fact_communication.to_excel(writer, sheet_name='fact_communication', index=False)
    bridge_comm_user.to_excel(writer, sheet_name='bridge_comm_user', index=False)

print("Star schema tables exported to 'star_schema_output.xlsx'.")