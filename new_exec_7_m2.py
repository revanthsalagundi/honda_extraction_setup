import csv
import time
import os
import subprocess
import sys
import shutil
import logging
import pandas as pd
from tqdm import tqdm
from urllib.parse import urlparse
import signal
from datetime import datetime
from teligram_notifier import initialize as init_telegram, send_notification_sync
SAS_TOKEN = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2025-03-15T02:51:51Z&st=2025-01-31T18:51:51Z&spr=https&sig=U8effska8HwB%2B0xxEplR19a9f8XTtJGtOkK1j1lu%2Bn4%3D"


# Constants
CSV_INPUT = 'blob_info2.csv'
CSV_COMPLETED = 'completed_urls_log.csv'
CSV_ERRORS = f'errored_urls.csv'
BIN_FOLDER = 'bin_files'
IMG_FOLDER = 'img_files'
AZURE_STORAGE_ACCOUNT = 'forhondafotus'
AZURE_OUTPUT_CONTAINER = 'do-not-delete-extracted'
LOGS_FOLDER = 'logs'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('output.log')
    ]
)

# Global variables
running = True
last_progress_time = time.time()
total_processed = 0
errored_urls = []

def signal_handler(signum, frame):
    global running
    if not running:  # If already shutting down, exit immediately
        sys.exit(0)
    logging.info("Received shutdown signal, finishing current task...")
    running = False

def initialize_completed_log():
    if not os.path.exists(CSV_COMPLETED):
        pd.DataFrame(columns=['Blob URL', 'File Size', 'Extracted Files', 'Output Path', 'Timestamp']).to_csv(CSV_COMPLETED, index=False)

def get_completed_urls():
    if os.path.exists(CSV_COMPLETED):
        df = pd.read_csv(CSV_COMPLETED)
        return set(df['Blob URL'])
    return set()

def get_progress_stats():
    total_files = len(pd.read_csv(CSV_INPUT))
    completed_files = len(get_completed_urls())
    remaining_files = total_files - completed_files
    completion_percentage = (completed_files / total_files * 100) if total_files > 0 else 0
    
    return {
        'total': total_files,
        'completed': completed_files,
        'remaining': remaining_files,
        'percentage': completion_percentage
    }

def send_progress_update():
    stats = get_progress_stats()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    message = f"""
Progress Update ({current_time}):
Total Files: {stats['total']}
Completed: {stats['completed']}
Remaining: {stats['remaining']}
Progress: {stats['percentage']:.2f}%
Errored URLs: {len(errored_urls)}

Processing Rate: {total_processed} files in the last hour
"""
    send_notification_sync(message)
    return time.time()

# def perform_azcopy_login():
#     """
#     Perform azcopy login with managed identity and return success status
#     """
#     try:
#         logging.info("Attempting to login with azcopy using managed identity...")
#         # login_command = "azcopy login --identity"
#         # result = subprocess.run(login_command, shell=True, capture_output=True, text=True)
        
#         if result.returncode == 0:
#             logging.info("Successfully logged in with azcopy")
#             return True
#         else:
#             logging.error(f"azcopy login failed: {result.stderr}")
#             return False
#     except Exception as e:
#         logging.error(f"Error during azcopy login: {str(e)}")
#         return False

def download_and_process_bin(blob_url):
    try:
        # perform_azcopy_login()
        # Parse the blob URL to get container and path
        container_name = blob_url.split('/')[4]
        file_path = '/'.join(blob_url.split('/')[4:])
        file_name = os.path.basename(file_path)
        
        # Create temporary download path
        download_path = os.path.join(BIN_FOLDER, file_name)
        
        logging.info(f"Downloading: {file_name}")
        # Download the bin file
        command = f"azcopy copy '{blob_url}?{SAS_TOKEN}' {download_path}"
        subprocess.run(command, shell=True, check=True)
        
        # Create temporary extraction path
        temp_extract_path = os.path.join(IMG_FOLDER, os.path.splitext(file_name)[0])
        os.makedirs(temp_extract_path, exist_ok=True)

        folder_name = download_path.replace(file_name, "")
        print(os.listdir(folder_name))
        logging.info(f"Processing: {file_name}")
        
        # Process the bin file
        command = [sys.executable, "camera_parser_v3.py", "--src", folder_name, "--dst", temp_extract_path]
        subprocess.run(command, check=True)
        
        # Construct the Azure upload path
        azure_upload_path = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/{AZURE_OUTPUT_CONTAINER}/{container_name}/{file_path}/?{SAS_TOKEN}"
        
        logging.info(f"Uploading to: {azure_upload_path}")
        command = f"azcopy copy '{temp_extract_path}/*' '{azure_upload_path}' --recursive=true"
        subprocess.run(command, shell=True, check=True)
        
        # Get file stats
        file_size = os.path.getsize(download_path)
        extracted_files = len(os.listdir(temp_extract_path))
        
        # Clean up
        os.remove(download_path)
        shutil.rmtree(temp_extract_path)
        
        return {
            'file_size': file_size,
            'extracted_files': extracted_files,
            'output_path': azure_upload_path
        }
        
    except Exception as e:
        logging.error(f"Error processing {blob_url}: {str(e)}")
        raise

def main():
    global running, last_progress_time, total_processed, errored_urls
    
    try:
        send_notification_sync("Blob processor started")
        
        # Perform initial azcopy login
        # if not perform_azcopy_login():
        #     send_notification_sync("Failed to login with azcopy. Exiting.")
        #     return
        
        send_notification_sync("Successfully logged in with azcopy")
        
        while running:
            try:
                logging.info("Starting new processing cycle...")
                initialize_completed_log()
                completed_urls = get_completed_urls()
                
                # Read and filter the input CSV
                df = pd.read_csv(CSV_INPUT)
                to_process = df[~df['Blob URL'].isin(completed_urls)]
                
                if len(to_process) == 0:
                    logging.info("No new files to process. Waiting for 1 hour...")
                    send_notification_sync("No new files to process. Waiting for 1 hour...")
                    for _ in range(360):  # Break up the hour into 10-second intervals
                        if not running:
                            break
                        time.sleep(10)
                    continue
                
                logging.info(f"Found {len(to_process)} files to process")
                send_notification_sync(f"Found {len(to_process)} files to process")
                
                # Reset hourly counter
                total_processed = 0
                last_progress_time = time.time()
                
                # Process each file
                for index, (_, row) in enumerate(to_process.iterrows(), 1):
                    if not running:
                        break
                    
                    current_time = time.time()
                    if current_time - last_progress_time >= 3600:
                        # # Refresh azcopy login every hour
                        # if not perform_azcopy_login():
                        #     send_notification_sync("Failed to refresh azcopy login. Continuing with current session.")
                        
                        last_progress_time = send_progress_update()
                        total_processed = 0
                    
                    blob_url = row['Blob URL']
                    try:
                        result = download_and_process_bin(blob_url)
                        total_processed += 1
                        
                        with open(CSV_COMPLETED, 'a', newline='') as f:
                            writer = csv.writer(f)
                            writer.writerow([
                                blob_url,
                                result['file_size'],
                                result['extracted_files'],
                                result['output_path'],
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            ])
                        send_notification_sync(f"total processed{total_processed} files ")
                    except Exception as exc:
                        logging.error(f"Error processing {blob_url}: {exc}")
                        send_notification_sync(f"Error processing {blob_url}: {exc}")
                        errored_urls.append({'Blob URL': blob_url, 'Error': str(exc)})
                        continue
                    
                    if not running:
                        break
                
                if running:
                    send_progress_update()
                    send_notification_sync("Cycle completed. Waiting for 1 hour before next check...")
                    for _ in range(360):  # Break up the hour into 10-second intervals
                        if not running:
                            break
                        time.sleep(10)
                
            except Exception as e:
                logging.error(f"Error in main loop: {e}")
                send_notification_sync(f"Error in main loop: {e}")
                if running:
                    time.sleep(300)
    
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received, stopping gracefully...")
    finally:
        running = False

if __name__ == "__main__":
    try:
        # Register signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Initialize Telegram notifier
        init_telegram()
        
        # Create necessary folders
        os.makedirs(BIN_FOLDER, exist_ok=True)
        os.makedirs(IMG_FOLDER, exist_ok=True)
        os.makedirs(LOGS_FOLDER, exist_ok=True)
        
        logging.info("Starting blob processor...")
        main()
        
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
    except Exception as e:
        logging.error(f"Unhandled exception: {e}")
    finally:
        # Clean up temporary folders
        if os.path.exists(BIN_FOLDER):
            shutil.rmtree(BIN_FOLDER)
        if os.path.exists(IMG_FOLDER):
            shutil.rmtree(IMG_FOLDER)
        logging.info("Cleanup completed")
        
        # Save errored URLs to CSV
        if errored_urls:
            pd.DataFrame(errored_urls).to_csv(CSV_ERRORS, index=False)
            logging.info(f"Saved {len(errored_urls)} errored URLs to {CSV_ERRORS}")
            send_notification_sync(f"Saved {len(errored_urls)} errored URLs to {CSV_ERRORS}")
        
        send_notification_sync("Blob processor stopped")