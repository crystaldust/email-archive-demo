from datetime import datetime
import requests
import shutil
import gzip
import json
from os import makedirs, remove

from dateutil.relativedelta import relativedelta
from perceval.backends.core.mbox import MBox
from perceval.backends.core.pipermail import Pipermail


class EmailArchive:
    def __init__(self, project_name, url_prefix, start_since=None):
        self.project_name = project_name
        self.dirpath = project_name  # TODO Is this proper?(dirpath should just be temp)
        self.url_prefix = url_prefix
        self.start_since = start_since

    def get_res(self, since=None, until=None):
        pass


class FileArchive(EmailArchive):
    def __init__(self, project_name, url_prefix, months=[], url_format='', start_since=None, file_ext=None):
        super().__init__(project_name, url_prefix, start_since)

        self.url_format = url_format
        self.file_ext = file_ext
        self.months = months
        self.file_paths = []
        # TODO Init date is needed

    def download_files(self, urls):
        for url in urls:
            filename = url.split('/')[-1]
            filepath = f'{self.dirpath}/{filename}'
            print(f'Downloading {url} to {filepath}')
            self.file_paths.append(filepath)
            makedirs(self.dirpath, exist_ok=True)
            with requests.get(url, stream=True) as res:
                with open(filepath, 'wb') as f:
                    shutil.copyfileobj(res.raw, f)

    def process_downloaded_file(self):
        pass

    def get_res(self, since=None, until=None):
        urls = self.generate_urls(since, until)
        self.download_files(urls)
        self.process_downloaded_file()

    def generate_urls(self, since=None, until=None):
        start, end = since, until
        if not since:
            start = self.start_since
        if not until:
            end = datetime.now()

        index = start
        urls = []
        while index <= end:
            date_qs = index.strftime(self.url_format)

            # TODO Generate qs according to self.months
            # Notes: python datetime format can cover full month name with %B
            # Add rules & code to handle more complex situation
            # if self.months:
            #     month_name = self.months[index.month-1]
            # do something with month_name
            url = f'{self.url_prefix}/{date_qs}'
            if self.file_ext:
                url = f'{url}.{self.file_ext}'
            urls.append(url)

            # TODO FileArchive should take an 'interval' param to customize the archive interval
            index += relativedelta(months=1)

        return urls


class GZipArchive(FileArchive):
    def process_downloaded_file(self):
        # When downloaded, the filepath points to the gzip file
        for gzip_filepath in self.file_paths:
            with gzip.open(gzip_filepath, 'rb') as gzip_in:
                # Forget about striping the '.tgz', 'gzip'
                # just add a postfix 'raw' to make life easy!
                with open(f'{gzip_filepath}.raw', 'wb') as raw_out:
                    shutil.copyfileobj(gzip_in, raw_out)
            remove(gzip_filepath)


archives = []
with open('./maillists.json', 'r') as f:
    maillists = json.loads(f.read())
    for maillist in maillists:
        archive_type = maillist['archive_type']

        kwargs = {
            'project_name': maillist['project_name'],
            'url_prefix': maillist['url_prefix']
        }

        if archive_type == 'txt' or archive_type == 'gzip':
            date_format = maillist['date_format']
            start_since = datetime.strptime(maillist['start_since'], maillist['date_format'])
            kwargs['url_format'] = date_format
            kwargs['start_since'] = start_since
            if 'file_ext' in maillist and maillist['file_ext']:
                kwargs['file_ext'] = maillist['file_ext']
            archive = FileArchive(**kwargs)  # TODO OK, perceval can parse gzip file into mbox!
            archives.append(archive)
        elif archive_type == 'pipermail':
            archive = EmailArchive(**kwargs)
            archives.append(archive)

for archive in archives:
    test_since = datetime.strptime('2021-01', '%Y-%m')
    test_until = datetime.strptime('2021-11', '%Y-%m')

    ArchiveType = type(archive)
    archive.get_res(since=test_since, until=test_until)

    # Type Pipermail is derived from MBox
    repo = None
    if ArchiveType == FileArchive or ArchiveType == GZipArchive:
        repo = MBox(uri=archive.url_prefix, dirpath=archive.project_name)
    elif ArchiveType == EmailArchive:
        repo = Pipermail(url=archive.url_prefix, dirpath=archive.dirpath)
    for message in repo.fetch(from_date=test_since):
        # Warning! Some content cannot be encoded as UTF-8
        print(message['data']['Subject'].encode('unicode_escape'))
