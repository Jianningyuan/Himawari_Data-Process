import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Set, Dict, Optional, List

import paramiko
import paramiko.sftp_client

# --- Configuration ---
# Option 1: Import from a separate config.py file
# Make sure you have a config.py file in the same directory with:
# FTP_HOST = "your_ftp_host"
# FTP_USER = "your_ftp_username"
# FTP_PASS = "your_ftp_password"
# FTP_PORT = 2051 # Or your specific port
try:
    from config import FTP_HOST, FTP_PASS, FTP_USER
except ImportError:
    print("Warning: config.py not found or missing variables. Using placeholder values.")
    print("Please create config.py with FTP_HOST, FTP_USER, FTP_PASS")
    FTP_HOST = "replace_with_host"
    FTP_USER = "replace_with_user"
    FTP_PASS = "replace_with_pass"

FTP_PORT = 2051 # Default port, adjust if needed

# Option 2: Define directly here (less recommended for credentials)
# FTP_HOST = "your_ftp_host"
# FTP_USER = "your_ftp_username"
# FTP_PASS = "your_ftp_password"
# FTP_PORT = 2051

# Default bands to target if user doesn't specify
DEFAULT_TARGET_BANDS: Set[str] = {"01", "02", "03", '04', "08", '07', "13"} # Use strings for band numbers

# Local directory to save data
LOCAL_DATA_DIR = "./data"

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Global variable to track current download for cleanup ---
current_temp_file_path: Optional[str] = None

# --- Functions ---

def get_datetime_input(prompt: str) -> datetime:
    """Prompts the user for a date and time and validates the input."""
    while True:
        try:
            datetime_str = input(f"{prompt} (YYYYMMDD HH:MM): ")
            return datetime.strptime(datetime_str, "%Y%m%d %H:%M")
        except ValueError:
            logging.error("Invalid date/time format. Please use YYYYMMDD HH:MM.")

def get_band_input(default_bands: Set[str]) -> Set[str]:
    """Prompts the user to select bands or use defaults."""
    while True:
        choice = input(f"Enter target bands (e.g., 01,03,08) or press Enter to use defaults ({','.join(sorted(default_bands))}): ").strip()
        if not choice:
            return default_bands
        try:
            # Split, strip whitespace, and remove empty strings
            selected_bands = {band.strip() for band in choice.split(',') if band.strip().isdigit()}
            if selected_bands:
                logging.info(f"Selected bands: {', '.join(sorted(selected_bands))}")
                return selected_bands
            else:
                logging.warning("No valid band numbers entered. Please enter digits separated by commas.")
        except Exception as e:
            logging.error(f"Error processing band input: {e}")

def generate_time_range(start_time: datetime,
                        end_time: datetime,
                        interval_minutes: int = 10) -> List[datetime]:
    """
    Generates a list of datetime objects within a specified range and interval.

    :param start_time: The starting datetime.
    :param end_time: The ending datetime.
    :param interval_minutes: The interval in minutes between time points.
    :return: A list of datetime objects.
    """
    if start_time > end_time:
        logging.warning("Start time is after end time. No time points will be generated.")
        return []

    time_range = []
    current_time = start_time
    interval = timedelta(minutes=interval_minutes)

    while current_time <= end_time:
        time_range.append(current_time)
        current_time += interval

    logging.info(f"Generated {len(time_range)} time points from {start_time} to {end_time}.")
    return time_range

def extract_date_time_info(time_object: datetime) -> Dict[str, str]:
    """
    Extracts formatted date and time components from a datetime object.

    :param time_object: The datetime object.
    :return: A dictionary with formatted 'year_month', 'day', 'hour', 'minute'.
    """
    return {
        'year_month': time_object.strftime("%Y%m"),
        'day': time_object.strftime('%d'),
        'hour': time_object.strftime("%H"),
        'minute': time_object.strftime("%M")
    }

def download_data(sftp_obj: paramiko.sftp_client.SFTPClient,
                  time_points: List[datetime],
                  target_bands: Set[str],
                  local_base_path: str = LOCAL_DATA_DIR) -> None:
    """
    Downloads satellite data files for specified time points and bands.

    :param sftp_obj: Active SFTP client object.
    :param time_points: List of datetime objects to download data for.
    :param target_bands: Set of band numbers (as strings) to download.
    :param local_base_path: The base directory to save downloaded files locally.
    """
    global current_temp_file_path
    # Removed os.makedirs(local_base_path, exist_ok=True) here as subdirs will handle it

    if not time_points:
        logging.warning("No time points provided for download.")
        return

    logging.info(f"Starting download process for {len(time_points)} time points...")

    for time_dt in time_points:
        detail_time = extract_date_time_info(time_dt)
        remote_dir = f'/jma/hsd/{detail_time['year_month']}/{detail_time['day']}/{detail_time['hour']}/'
        target_minute = detail_time['minute']

        # Create local subdirectory based on year_month and hour
        local_subdir = os.path.join(local_base_path, detail_time['year_month']+detail_time["day"], detail_time['hour'])
        print(f"Creating local directory: {local_subdir}")
        os.makedirs(local_subdir, exist_ok=True)  # Create directory if it doesn't exist

        logging.info(f"Checking remote directory: {remote_dir} for time {time_dt.strftime('%Y-%m-%d %H:%M')}")

        try:
            files_in_dir = sftp_obj.listdir(remote_dir)
        except FileNotFoundError:
            logging.warning(f"Remote directory not found: {remote_dir}. Skipping this time slot.")
            continue
        except Exception as e:
            logging.error(f"Error listing directory {remote_dir}: {e}")
            continue

        found_files_for_time = False
        for filename in files_in_dir:
            if not filename.endswith('.DAT.bz2') or '_FLDK_' not in filename:
                continue

            try:
                parts = filename.split('_')
                if len(parts) < 5: continue

                file_timestamp_str = parts[2] + parts[3]
                file_band_part = ""
                for part in parts:
                    if part.startswith('B') and part[1:].isdigit():
                        file_band_part = part
                        break
                    elif '.fldk.' in part.lower():
                         sub_parts = part.split('.')
                         if len(sub_parts) > 1 and sub_parts[-1].isdigit():
                            file_band_part = f"B{sub_parts[-1].zfill(2)}"
                            break
                    elif 'FLDK' in part:
                        sub_parts = part.split('.')
                        if len(sub_parts) > 1 and sub_parts[-1].isdigit():
                           file_band_part = f"B{sub_parts[-1].zfill(2)}"
                           break
                        elif len(sub_parts) > 1 and sub_parts[-1].startswith('B') and sub_parts[-1][1:].isdigit():
                            file_band_part = sub_parts[-1]
                            break

                if not file_band_part:
                    try:
                        fldk_part = filename.split('FLDK.')[1].split('.')[0]
                        if fldk_part.isdigit():
                            file_band_part = f"B{fldk_part.zfill(2)}"
                    except IndexError:
                        pass

                if not file_band_part:
                    continue

                file_band_check = file_band_part[1:]

                if file_timestamp_str != time_dt.strftime("%Y%m%d%H%M"):
                     if len(parts) > 3 and parts[3][-2:] != target_minute:
                        continue

                if file_band_check not in target_bands:
                    continue

                found_files_for_time = True
                remote_file_path = os.path.join(remote_dir, filename).replace("\\", "/")
                # Updated local file path to use the subdirectory
                local_file_path = os.path.join(local_subdir, filename)
                temp_local_file_path = local_file_path + ".part"

                logging.info(f"Found matching file: {filename} (Band {file_band_check})")

                if os.path.exists(local_file_path):
                    logging.info(f"File already exists locally: {local_file_path}. Skipping.")
                    continue
                if os.path.exists(temp_local_file_path):
                    logging.warning(f"Partial file exists: {temp_local_file_path}. Attempting to resume/overwrite.")

                logging.info(f"Downloading {remote_file_path} to {temp_local_file_path}")
                try:
                    current_temp_file_path = temp_local_file_path
                    sftp_obj.get(remote_file_path, temp_local_file_path)
                    os.rename(temp_local_file_path, local_file_path)
                    logging.info(f"Successfully downloaded and saved: {local_file_path}")
                    current_temp_file_path = None

                except Exception as download_err:
                    logging.error(f"Failed to download {remote_file_path}: {download_err}")
                    if os.path.exists(temp_local_file_path):
                        try:
                            os.remove(temp_local_file_path)
                            logging.info(f"Removed partial download file: {temp_local_file_path}")
                        except OSError as remove_err:
                            logging.error(f"Error removing partial file {temp_local_file_path}: {remove_err}")
                    current_temp_file_path = None

            except Exception as file_process_err:
                logging.error(f"Error processing file {filename}: {file_process_err}")
                continue

        if not found_files_for_time:
            logging.info(f"No matching files found for {time_dt.strftime('%Y-%m-%d %H:%M')} in {remote_dir} with target bands {target_bands}.")

    logging.info("Download process finished.")


def main():
    """Main function to orchestrate the connection and download."""
    global current_temp_file_path

    if FTP_HOST == "replace_with_host":
         logging.error("FTP credentials are not configured. Please edit the script or create config.py.")
         sys.exit(1)

    logging.info("--- Satellite Data Downloader ---")

    # Get date range from user
    start_dt = get_datetime_input("Enter start date and time")
    end_dt = get_datetime_input("Enter end date and time")

    # Get target bands from user
    selected_bands = get_band_input(DEFAULT_TARGET_BANDS)

    # Generate time points
    time_points_to_download = generate_time_range(start_dt, end_dt)

    if not time_points_to_download:
        logging.info("No time points to process. Exiting.")
        sys.exit(0)

    # Establish SFTP Connection
    transport = None
    sftp = None
    try:
        logging.info(f"Connecting to SFTP server: {FTP_HOST}:{FTP_PORT}")
        transport = paramiko.Transport((FTP_HOST, FTP_PORT))
        transport.connect(username=FTP_USER, password=FTP_PASS)
        sftp = paramiko.SFTPClient.from_transport(transport)
        logging.info("SFTP connection successful.")

        # Start download process
        download_data(sftp, time_points_to_download, selected_bands, LOCAL_DATA_DIR)

    except paramiko.AuthenticationException:
        logging.error("Authentication failed. Please check FTP_USER and FTP_PASS.")
    except paramiko.SSHException as ssh_err:
        logging.error(f"Could not establish SSH connection: {ssh_err}")
    except FileNotFoundError as fnf_err:
         # This might be caught earlier, but as a fallback
        logging.error(f"A required directory or file not found: {fnf_err}")
    except KeyboardInterrupt:
        logging.warning("\n--- Process interrupted by user (Ctrl+C) ---")
        # Cleanup partial download if interruption happened during sftp.get()
        if current_temp_file_path and os.path.exists(current_temp_file_path):
            logging.info(f"Cleaning up partial download: {current_temp_file_path}")
            try:
                os.remove(current_temp_file_path)
                logging.info("Partial file removed.")
            except OSError as e:
                logging.error(f"Could not remove partial file {current_temp_file_path}: {e}")
        else:
            logging.info("No partial file to clean up.")
        sys.exit(1) # Indicate script was interrupted
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True) # Log traceback
    finally:
        # --- Cleanup ---
        logging.info("Closing SFTP connection.")
        if sftp:
            sftp.close()
        if transport and transport.is_active():
            transport.close()
        logging.info("Connection closed.")
        current_temp_file_path = None # Ensure tracker is cleared on exit

    logging.info("--- Script finished ---")
    sys.exit(0) # Indicate successful completion


if __name__ == "__main__":
    # Ensure config.py is set up or edit credentials directly
    # if FTP_HOST == "replace_with_host":
    #    print("ERROR: Please configure FTP connection details in the script or in config.py")
    # else:
    main()