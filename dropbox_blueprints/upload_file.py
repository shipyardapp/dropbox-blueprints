import os
import re
import json
import tempfile
import argparse
import glob

from dropbox import Dropbox
from dropbox.files import UploadSessionCursor, CommitInfo
from dropbox.exceptions import *

CHUNK_SIZE = 10 * 1024 * 1024


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-file-name-match-type',
                        dest='source_file_name_match_type',
                        default='exact_match',
                        choices={
                            'exact_match',
                            'regex_match'},
                        required=False)
    parser.add_argument(
        '--source-file-name',
        dest='source_file_name',
        required=True)
    parser.add_argument(
        '--source-folder-name',
        dest='source_folder_name',
        default='',
        required=False)
    parser.add_argument(
        '--destination-folder-name',
        dest='destination_folder_name',
        default='',
        required=False)
    parser.add_argument(
        '--destination-file-name',
        dest='destination_file_name',
        default=None,
        required=False)
    parser.add_argument(
        '--access-key',
        dest='access_key',
        default=None,
        required=True)
    return parser.parse_args()


def extract_file_name_from_source_full_path(source_full_path):
    """
    Use the file name provided in the source_full_path variable. Should be run
    only if a destination_file_name is not provided.
    """
    destination_file_name = os.path.basename(source_full_path)
    return destination_file_name


def enumerate_destination_file_name(destination_file_name, file_number=1):
    """
    Append a number to the end of the provided destination file name.
    Only used when multiple files are matched to, preventing the destination
    file from being continuously overwritten.
    """
    if re.search(r'\.', destination_file_name):
        destination_file_name = re.sub(
            r'\.', f'_{file_number}.', destination_file_name, 1)
    else:
        destination_file_name = f'{destination_file_name}_{file_number}'
    return destination_file_name


def determine_destination_file_name(
    *,
    source_full_path,
    destination_file_name,
        file_number=None):
    """
    Determine if the destination_file_name was provided, or should be extracted
    from the source_file_name, or should be enumerated for multiple file
    uploads.
    """
    if destination_file_name:
        if file_number:
            destination_file_name = enumerate_destination_file_name(
                destination_file_name, file_number)
        else:
            destination_file_name = destination_file_name
    else:
        destination_file_name = extract_file_name_from_source_full_path(
            source_full_path)

    return destination_file_name


def clean_folder_name(folder_name):
    """
    Cleans folders name by removing duplicate '/' as well as leading and
    trailing '/' characters.
    """
    folder_name = folder_name.strip('/')
    if folder_name != '':
        folder_name = os.path.normpath(folder_name)
    return folder_name


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path
    variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}')
    combined_name = os.path.normpath(combined_name)

    return combined_name


def determine_destination_full_path(
        destination_folder_name,
        destination_file_name,
        source_full_path,
        file_number=None):
    """
    Determine the final destination name of the file being uploaded.
    """
    destination_file_name = determine_destination_file_name(
        destination_file_name=destination_file_name,
        source_full_path=source_full_path,
        file_number=file_number)
    destination_full_path = combine_folder_and_file_name(
        destination_folder_name, destination_file_name)
    return f'/{destination_full_path}'


def find_all_local_file_names(source_folder_name):
    """
    Returns a list of all files that exist in the current working directory,
    filtered by source_folder_name if provided.
    """
    cwd = os.getcwd()
    cwd_extension = os.path.normpath(f'{cwd}/{source_folder_name}/**')
    file_names = glob.glob(cwd_extension, recursive=True)
    return [file_name for file_name in file_names if os.path.isfile(file_name)]


def find_all_file_matches(file_names, file_name_re):
    """
    Return a list of all file_names that matched the regular expression.
    """
    matching_file_names = []
    for file in file_names:
        if re.search(file_name_re, file):
            matching_file_names.append(file)

    return matching_file_names


def upload_dropbox_file(
        client,
        source_full_path,
        destination_full_path):
    """
    Uploads a single file to Dropbox.
    """
    if os.path.getsize(source_full_path) <= CHUNK_SIZE:
        upload_small_dropbox_file(client=client,
                                  source_full_path=source_full_path,
                                  destination_full_path=destination_full_path)
    else:
        upload_large_dropbox_file(client=client,
                                  source_full_path=source_full_path,
                                  destination_full_path=destination_full_path)


def upload_small_dropbox_file(
        client,
        source_full_path,
        destination_full_path):
    """
    Uploads a small (<=CHUNK_SIZE) single file to Dropbox.
    """
    with open(source_full_path, 'rb') as f:
        try:
            client.files_upload(f.read(CHUNK_SIZE), destination_full_path)
        except ApiError as e:
            print(f'Failed to upload file {source_full_path}')

    print(f'{source_full_path} successfully uploaded to '
          f'{destination_full_path}')


def upload_large_dropbox_file(
        client,
        source_full_path,
        destination_full_path):
    """
    Uploads a large (>CHUNK_SIZE) single file to Dropbox.
    """
    file_size = os.path.getsize(source_full_path)
    with open(source_full_path, 'rb') as f:
        try:
            upload_session_start_result = client.files_upload_session_start(
                f.read(CHUNK_SIZE))
            session_id = upload_session_start_result.session_id
            cursor = UploadSessionCursor(
                session_id=session_id, offset=f.tell())
            commit = CommitInfo(path=destination_full_path)

            while f.tell() < file_size:
                if ((file_size - f.tell()) <= CHUNK_SIZE):
                    print(
                        client.files_upload_session_finish(
                            f.read(CHUNK_SIZE), cursor, commit))
                else:
                    client.files_upload_session_append(f.read(CHUNK_SIZE),
                                                       cursor.session_id,
                                                       cursor.offset)
                    cursor.offset = f.tell()
        except ApiError as e:
            print(f'Failed to upload file {source_full_path}')

    print(f'{source_full_path} successfully uploaded to '
          f'{destination_full_path}')


def get_dropbox_client(access_key):
    """
    Attempts to create the Dropbox Client with the associated
    """
    try:
        client = Dropbox(access_key)
        client.users_get_current_account()
        return client
    except AuthError as e:
        print(f'Failed to authenticate using key {access_key}')
        raise(e)


def main():
    args = get_args()
    access_key = args.access_key
    source_file_name = args.source_file_name
    source_folder_name = args.source_folder_name
    source_full_path = combine_folder_and_file_name(
        folder_name=f'{os.getcwd()}/{source_folder_name}',
        file_name=source_file_name)
    destination_folder_name = clean_folder_name(args.destination_folder_name)
    source_file_name_match_type = args.source_file_name_match_type

    client = get_dropbox_client(access_key=access_key)

    if source_file_name_match_type == 'regex_match':
        file_names = find_all_local_file_names(source_folder_name)
        matching_file_names = find_all_file_matches(
            file_names, re.compile(source_file_name))
        print(f'{len(matching_file_names)} files found. Preparing to upload...')

        for index, key_name in enumerate(matching_file_names):
            destination_full_path = determine_destination_full_path(
                destination_folder_name=destination_folder_name,
                destination_file_name=args.destination_file_name,
                source_full_path=key_name,
                file_number=index + 1)
            print(f'Uploading file {index+1} of {len(matching_file_names)}')
            upload_dropbox_file(
                source_full_path=key_name,
                destination_full_path=destination_full_path,
                client=client)

    else:
        destination_full_path = determine_destination_full_path(
            destination_folder_name=destination_folder_name,
            destination_file_name=args.destination_file_name,
            source_full_path=source_full_path)
        upload_dropbox_file(
            source_full_path=source_full_path,
            destination_full_path=destination_full_path,
            client=client)


if __name__ == '__main__':
    main()
