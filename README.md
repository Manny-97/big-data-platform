## Columns Name, Datatype with Description

**id (Integer)** – (Serial) Unique identifier for each record  
**subscriber_id (String)** – Unique ID assigned to each telecom subscriber  
**sim_card_number (String)** – SIM card number (ICCID)  
**imsi_number (String)** – International Mobile Subscriber Identity  
**imei_number (String)** – Device IMEI number  
**call_type (String)** – Type of service: 'voice', 'sms', or 'data'  
**service_type (String)** – Type of subscription: e.g., 'prepaid', 'postpaid'  
**direction (String)** – Direction of communication: 'incoming' or 'outgoing'  
**roaming_status (Boolean)** – Indicates if activity happened while roaming (True/False)  
**caller_number (String)** – Phone number that initiated the call or SMS  
**receiver_number (String)** – Phone number that received the call or SMS  
**country_code (String)** – Country code where activity originated  
**call_start_time (Timestamp)** – Start time of the call or data session  
**call_end_time (Timestamp)** – End time of the call or data session  
**call_duration_seconds (Integer)** – Duration of the call in seconds  
**message_content (Text)** – Text of the message (if call_type is 'sms')  
**message_length (Integer)** – Number of characters in the message  
**data_volume_mb (Numeric(10,2))** – Data used in megabytes (if call_type is 'data')  
**data_session_duration_seconds (Integer)** – Duration of data session in seconds  
**cell_tower_id (String)** – ID of the cell tower serving the subscriber at the time  
**location_latitude (Float)** – Latitude of the device during activity  
**location_longitude (Float)** – Longitude of the device during activity  
**location_area_code (String)** – Code representing the service area  
**region (String)** – Region or state where the activity occurred  
**city (String)** – City where the activity occurred  
**network_type (String)** – Type of network: '2G', '3G', '4G', '5G'  
**network_provider (String)** – Telecom provider name (e.g., MTN, Airtel)  
**signal_strength_dbm (Integer)** – Signal strength in decibels  
**cost (Numeric(10,2))** – Cost incurred for the activity  
**currency (String)** – Currency code, e.g., 'USD', 'NGN'  
**created_at (Timestamp)** – Timestamp when the record was created  
**updated_at (Timestamp)** – Timestamp when the record was last updated  




## Telecommunication Data Quality Table
```markdown
| Column Name                    | Null Allowed | Unique | Format / Allowed Values                      | Transformation/Validation Rules                            |
|-------------------------------|--------------|--------|-----------------------------------------------|-------------------------------------------------------------|
| id                            | No           | Yes    | Positive integer                               | Check uniqueness; Ensure no nulls                          |
| subscriber_id                 | No           | No     | UUID format                                    | Validate UUID format                                       |
| sim_card_number               | No           | Yes    | Starts with '8930' and 19 digits total         | Regex: ^8930\d{15}$                                        |
| imsi_number                   | No           | No     | 15-digit numeric string                        | Regex: ^\d{15}$                                            |
| imei_number                   | No           | No     | 15-digit numeric string                        | Regex: ^\d{15}$                                            |
| call_type                     | No           | No     | voice, sms, data                               | Validate against known list                                    |
| service_type                  | No           | No     | prepaid, postpaid                              | Validate against known list                                    |
| direction                     | No           | No     | incoming, outgoing                             | Validate against known list                                    |
| roaming_status                | No           | No     | True, False                                    | Standardize to boolean                                    |
| caller_number                 | No           | No     | Valid phone number                             | Validate phone format                                     |
| receiver_number               | Yes          | No     | Valid phone number or blank                    | Can be null; Validate if present                          |
| country_code                  | No           | No     | E.g. +234, +1, +44                             | Regex: ^\+\d+$                                             |
| call_start_time               | No           | No     | ISO format datetime                            | Convert to datetime; check parsing errors                 |
| call_end_time                 | No           | No     | ISO format datetime                            | Must be >= start_time                                     |
| call_duration_seconds         | Yes          | No     | >= 0                                           | Fill missing for SMS/data; must be non-negative           |
| message_content               | Yes          | No     | Any string (160 characters max)                | Truncate if too long                                      |
| message_length                | Yes          | No     | 0 - 160                                        | Calculate if missing                                      |
| data_volume_mb                | Yes          | No     | >= 0                                           | Fill nulls with 0 for voice/sms                           |
| data_session_duration_seconds| Yes          | No     | >= 0                                           | Same as above                                              |
| cell_tower_id                 | No           | No     | Format: CT-####                                | Regex: ^CT-\d{4}$                                          |
| location_latitude             | No           | No     | -90.0 to 90.0                                  | Clamp values; Validate range                              |
| location_longitude            | No           | No     | -180.0 to 180.0                                | Clamp values; Validate range                              |
| location_area_code            | No           | No     | Format: ###-###                                | Regex: ^\d{3}-\d{3}$                                       |
| region                        | No           | No     | Valid region name                              | Map/standardize spellings                                 |
| city                          | No           | No     | Valid city name                                | Standardize cases/spellings                               |
| network_type                  | No           | No     | 2G, 3G, 4G, 5G                                 | Validate known list                                            |
| network_provider              | No           | No     | MTN, Airtel, Glo, 9mobile                      | Validate known list                                            |
| signal_strength_dbm           | No           | No     | -120 to -50 dBm                                | Clamp values; flag weak signal                            |
| cost                          | No           | No     | >= 0                                           | Validate positive                                         |
| currency                      | No           | No     | NGN, USD                                       | Validate known list                                            |
| created_at                    | No           | No     | ISO datetime                                   | Check format                                              |
| updated_at                    | No           | No     | ISO datetime                                   | Must be >= created_at                                     |
```