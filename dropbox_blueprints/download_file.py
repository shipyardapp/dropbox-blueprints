import os
import sys
import re
import argparse

from dropbox import Dropbox
from dropbox.files import FileMetadata, FolderMetadata
from dropbox.exceptions import *


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--source-file-name-match-type',
        dest='source_file_name_match_type',
        default='exact_match',
        choices={
            'exact_match',
            'regex_match'},
        required=False)
    parser.add_argument(
        '--source-folder-name',
        dest='source_folder_name',
        default='',
        required=False)
    parser.add_argument(
        '--source-file-name',
        dest='source_file_name',
        required=True)
    parser.add_argument(
        '--destination-file-name',
        dest='destination_file_name',
        default=None,
        required=False)
    parser.add_argument(
        '--destination-folder-name',
        dest='destination_folder_name',
        default='',
        required=False)
    parser.add_argument(
        '--access-key',
        dest='access_key',
        default=None,
        required=True)
    return parser.parse_args()


def extract_file_name_from_source_full_path(source_full_path):
    """
    Use the file name provided in the source_file_name variable. Should be run only
    if a destination_file_name is not provided.
    """
    destination_file_name = os.path.basename(source_full_path)
    return destination_file_name


def enumerate_destination_file_name(destination_file_name, file_number=1):
    """
    Append a number to the end of the provided destination file name.
    Only used when multiple files are matched to, preventing the destination file from being continuously overwritten.
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
    Determine if the destination_file_name was provided, or should be extracted from the source_file_name,
    or should be enumerated for multiple file downloads.
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
    Cleans folders name by removing duplicate '/' as well as leading and trailing '/' characters.
    """
    folder_name = folder_name.strip('/')
    if folder_name != '':
        folder_name = os.path.normpath(folder_name)
    return folder_name


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}')
    combined_name = os.path.normpath(combined_name)

    return combined_name


def determine_destination_name(
        destination_folder_name,
        destination_file_name,
        source_full_path,
        file_number=None):
    """
    Determine the final destination name of the file being downloaded.
    """
    destination_file_name = determine_destination_file_name(
        destination_file_name=destination_file_name,
        source_full_path=source_full_path,
        file_number=file_number)
    destination_name = combine_folder_and_file_name(
        destination_folder_name, destination_file_name)
    return destination_name


def find_dropbox_file_names(client, prefix=None):
    """
    Fetched all the files in the bucket which are returned in a list as
    file names
    """
    result = []
    folders = []
    if prefix and not prefix.startswith('/'):
        prefix = f'/{prefix}'
    try:
        files = client.files_list_folder(prefix)
    except Exception as e:
        print(f'Failed to search folder {prefix}')
        return []

    for f in files.entries:
        if isinstance(f, FileMetadata):
            result.append(f.path_lower)
        elif isinstance(f, FolderMetadata):
            folders.append(f.path_lower)
    for folder in folders:
        result.extend(find_dropbox_file_names(client, prefix=folder))
    return result


def find_matching_files(file_names, file_name_re):
    """
    Return a list of all file_names that matched the regular expression.
    """
    matching_file_names = []
    for file_name in file_names:
        if re.search(file_name_re, file_name):
            matching_file_names.append(file_name)

    return matching_file_names


def download_dropbox_file(file_name, client, destination_file_name=None):
    """
    Download a selected file from Dropbox to local storage in
    the current working directory.
    """
    local_path = os.path.normpath(f'{os.getcwd()}/{destination_file_name}')

    try:
        with open(local_path, 'wb') as f:
            metadata, _file = client.files_download(path=file_name)
            f.write(_file.content)
    except Exception as e:
        if 'not_found' in str(e):
            print(f'Download failed. Could not find {file_name}')
        elif 'not_file' in str(e):
            print(f'Download failed. {file_name} is not a file')
        else:
            print(f'Failed to download {file_name} to {local_path}')
        os.remove(local_path)
        raise(e)

    print(f'{file_name} successfully downloaded to {local_path}')

    return


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
    source_folder_name = clean_folder_name(args.source_folder_name)
    source_full_path = combine_folder_and_file_name(
        folder_name=source_folder_name, file_name=source_file_name)
    source_file_name_match_type = args.source_file_name_match_type

    destination_folder_name = clean_folder_name(args.destination_folder_name)
    if not os.path.exists(destination_folder_name) and \
            (destination_folder_name != ''):
        os.makedirs(destination_folder_name)

    client = get_dropbox_client(access_key=access_key)

    if source_file_name_match_type == 'regex_match':
        file_names = find_dropbox_file_names(client=client,
                                             prefix=source_folder_name)
        matching_file_names = find_matching_files(file_names,
                                                  re.compile(source_file_name))
        print(f'{len(matching_file_names)} files found. Preparing to download...')

        for index, file_name in enumerate(matching_file_names):
            destination_name = determine_destination_name(
                destination_folder_name=destination_folder_name,
                destination_file_name=args.destination_file_name,
                source_full_path=file_name, file_number=index + 1)

            if not file_name.startswith('/'):
                file_name = f'/{file_name}'
            print(f'Downloading file {index+1} of {len(matching_file_names)}')
            download_dropbox_file(file_name=file_name, client=client,
                                  destination_file_name=destination_name)
    else:
        destination_name = determine_destination_name(
            destination_folder_name=destination_folder_name,
            destination_file_name=args.destination_file_name,
            source_full_path=source_full_path)

        if not source_full_path.startswith('/'):
            source_full_path = f'/{source_full_path}'
        download_dropbox_file(file_name=source_full_path, client=client,
                              destination_file_name=destination_name)


if __name__ == '__main__':
    main()
