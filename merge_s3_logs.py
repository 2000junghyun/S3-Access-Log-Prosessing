import os
import csv
import glob

# S3 로그가 저장된 루트 디렉토리 경로
log_dir = '/Users/admin/Downloads/s3-logs/06/12'

# 결과를 저장할 CSV 파일 경로
output_csv = '06-12-2025.csv'

# S3 Access Log 컬럼
columns = [
    'bucket_owner', 'bucket', 'datetime', 'remote_ip', 'requester', 'request_id',
    'operation', 'key', 'request_uri', 'http_status', 'error_code',
    'bytes_sent', 'object_size', 'total_time', 'turnaround_time',
    'referrer', 'user_agent', 'version_id', 'host_id', 'signature_version',
    'cipher_suite', 'auth_type', 'endpoint', 'tls_version',
    'access_point_arn', 'acl_required'
]

def rename_files_to_txt(root_dir):
    count = 0
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            old_path = os.path.join(dirpath, filename)
            if not filename.endswith('.txt'):
                new_path = old_path + '.txt'
                os.rename(old_path, new_path)
                count += 1
    print(f"확장자를 .txt로 변경한 파일 수: {count}")

def parse_log_line(line):
    parts = line.strip().split()
    if len(parts) < 15:
        raise ValueError("필드 수 부족")

    datetime_str = parts[2].strip('[') + ' ' + parts[3].strip(']')
    cleaned_parts = parts[:2] + [datetime_str] + parts[4:]
    return cleaned_parts[:len(columns)]

def merge_logs_to_csv(log_dir, output_csv):
    log_files = glob.glob(os.path.join(log_dir, '**/*.txt'), recursive=True)
    parsed_count = 0

    with open(output_csv, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(columns)

        for file_path in log_files:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                for line in file:
                    if line.strip() == '':
                        continue
                    try:
                        row = parse_log_line(line)
                        writer.writerow(row)
                        parsed_count += 1
                    except Exception:
                        continue
    print(f"로그 줄 파싱 성공: {parsed_count} 줄")
    print(f"처리된 로그 파일 수: {len(log_files)}")

# 실행 흐름
print(f"Processed folder path: {log_dir}")
rename_files_to_txt(log_dir)
merge_logs_to_csv(log_dir, output_csv)