# S3 Access Log Merger & CSV Converter

## Problem

- S3 access log files are generated at fixed intervals that may not align with user needs.
- Users often require dynamic merging of logs based on custom time ranges.
- Each file is in `.txt` format, which is not ideal for readability or data processing.
- Conversion to `.csv` is necessary for better usability.

## Solution

- The script merges all log files within a date-specific folder.
- Logs can be reviewed by date, and the merging logic can be easily adjusted for different time intervals.
- Files are converted from `.txt` to `.csv` format.
- If a file has no extension, `.txt` is added by default.
- The contents of individual `.txt` files are consolidated into a single `.csv` output file.

## Results
### Before
<img width="868" alt="Screenshot 2025-06-13 at 11 53 48 AM" src="https://github.com/user-attachments/assets/b5b334da-1530-4de9-a919-be70f91b3f1b" />

### After
<img width="1470" alt="result" src="https://github.com/user-attachments/assets/4996bd42-9618-49f0-8689-3565f159d117" />


## Expected Benefits

- Log files split into narrow time intervals can be merged based on custom time ranges.
- Converting `.txt` to `.csv` improves readability and facilitates further processing.
