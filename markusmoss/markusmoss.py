import collections
import glob
import os
import sys
import csv
import shutil
import subprocess
import zipfile
import mosspy
import markusapi
import requests
import io
import bs4
import re
from typing import Optional, ClassVar, Tuple, Iterable, Dict, Pattern, Iterator, List


class MarkusMoss:
    SUBMISSION_FILES_DIR: ClassVar[str] = "submission_files"
    GROUP_MEMBERSHIP_FILE: ClassVar[str] = "group_data.csv"
    PDF_SUBMISSION_FILES_DIR: ClassVar[str] = "pdf_submission_files"
    STARTER_FILES_DIR: ClassVar[str] = "starter_files"
    MOSS_REPORT_DIR: ClassVar[str] = "moss_report"
    MOSS_REPORT_URL: ClassVar[str] = "report_url.txt"
    MOSS_REPORT_DOWNLOAD: ClassVar[str] = "report"
    FINAL_REPORT_DIR: ClassVar[str] = "final_report"
    FINAL_REPORT_CASE_OVERVIEW: ClassVar[str] = "case_overview.csv"
    OVERVIEW_INFO: ClassVar[Tuple[str]] = ("case", "groups", "similarity (%)", "matched_lines")
    USER_INFO: ClassVar[Tuple[str]] = ("group_name", "user_name", "first_name", "last_name", "email", "id_number")
    PRINT_PREFIX: ClassVar[str] = "[MARKUSMOSS]"
    ACTIONS: ClassVar[Tuple[str]] = (
        "download_submission_files",
        "download_starter_files",
        "copy_files_to_pdf",
        "run_moss",
        "download_moss_report",
        "write_final_report",
    )

    def __init__(
        self,
        markus_api_key: Optional[str] = None,
        markus_url: Optional[str] = None,
        markus_assignment: Optional[str] = None,
        markus_course: Optional[str] = None,
        moss_userid: Optional[int] = None,
        moss_report_url: Optional[str] = None,
        workdir: Optional[str] = None,
        language: Optional[str] = None,
        groups: Optional[List[str]] = None,
        file_glob: str = "**/*",
        force: bool = False,
        verbose: bool = False,
    ) -> None:
        self.force = force
        self.verbose = verbose
        self.file_glob = file_glob
        self.groups = groups
        self.__group_data = None
        self.__membership_data = None
        self.__assignment_id = None
        self.__api = None
        self.__moss = None
        self.__report_regex = None
        self.__starter_file_groups = None
        self.__markus_api_key = markus_api_key
        self.__markus_url = markus_url
        self.__markus_assignment = markus_assignment
        self.__markus_course = markus_course
        self.__markus_course_id = None
        self.__moss_userid = moss_userid
        self.__moss_report_url = moss_report_url
        self.__workdir = workdir
        self.__language = language

    def run(self, actions: Optional[Iterable[str]] = None) -> None:
        if actions is None:
            actions = self.ACTIONS
        for action in actions:
            getattr(self, action)()

    def download_submission_files(self) -> None:
        for data in self._group_data:
            clean_filename = self._clean_filename(data["group_name"])
            destination = os.path.join(self.submission_files_dir, clean_filename)
            if os.path.isdir(destination) and not self.force:
                continue
            self._print(f"Downloading submission files for group: {data['group_name']}")
            zip_byte_stream = self.api.get_files_from_repo(self._markus_course_id, self._assignment_id, data["id"], collected=True)
            if not isinstance(zip_byte_stream, bytes):
                sys.stderr.write(f"[MARKUSAPI ERROR]{zip_byte_stream}\n")
                sys.stderr.flush()
                continue
            self._unzip_file(zip_byte_stream, destination)

    def copy_files_to_pdf(self) -> None:
        self._copy_files_to_pdf(self.submission_files_dir, self.pdf_submission_files_dir)
        self._copy_files_to_pdf(self.org_starter_files_dir, self.pdf_starter_files_dir)

    def download_starter_files(self) -> None:
        for group_data in self._starter_file_groups:
            destination = os.path.join(self.org_starter_files_dir, str(group_data["id"]))
            if os.path.isdir(destination) and not self.force:
                continue
            self._print(f"Downloading starter files for starter_group with id: {group_data['id']}")
            zip_byte_stream = self.api.download_starter_file_entries(self._markus_course_id, self._assignment_id, group_data["id"])
            if not isinstance(zip_byte_stream, bytes):
                sys.stderr.write(f"[MARKUSAPI ERROR] {zip_byte_stream}\n")
                sys.stderr.flush()
                continue
            self._unzip_file(zip_byte_stream, destination)

    def run_moss(self) -> None:
        if os.path.isfile(self.moss_report_url_file) and not self.force:
            return
        starter_files = glob.glob(os.path.join(self.org_starter_files_dir, "*", self.file_glob), recursive=True)
        for i, filename in enumerate(starter_files):
            self._print(f"Sending starter files to MOSS {i+1}/{len(starter_files)}", end="\r")
            self.moss.addBaseFile(filename, os.path.relpath(filename, self.workdir))
        self._print()
        submission_files = glob.glob(os.path.join(self.submission_files_dir, "*", self.file_glob), recursive=True)
        for i, filename in enumerate(submission_files):
            self._print(f"Sending submission files to MOSS {i+1}/{len(submission_files)}", end="\r")
            self.moss.addFile(filename, os.path.relpath(filename, self.workdir))
        self._print()
        self._print(f"Running moss")
        self.__moss_report_url = self.moss.send()
        self._print(f"Saving MOSS results from: {self.moss_report_url}")
        os.makedirs(self.moss_report_dir, exist_ok=True)
        with open(self.moss_report_url_file, "w") as f:
            f.write(self.moss_report_url)

    def _parse_url(self, url):
        data = requests.get(url).content.decode()
        return bs4.BeautifulSoup(data, features='html5lib')

    def _moss_download(self, url, dest_dir):
        parsed_html = self._parse_url(url)
        with open(os.path.join(dest_dir, 'index.html'), 'w') as f:
            f.write(str(parsed_html))
        urls = {u for u in (a.attrs.get('href') for a in parsed_html.find_all('a')) if u.startswith(url)}
        for url_ in urls:
            parsed_html = self._parse_url(url_)
            with open(os.path.join(dest_dir, os.path.basename(url_)), 'w') as f:
                f.write(str(parsed_html))
            for src_url in [f.attrs['src'] for f in parsed_html.find_all('frame')]:
                with open(os.path.join(dest_dir, os.path.basename(src_url)), 'w') as f:
                    f.write(str(self._parse_url(os.path.join(url, src_url))))

    def download_moss_report(self) -> None:
        if not os.path.isdir(self.moss_report_download_dir) or self.force:
            self._print(f"Downloading MOSS report")
            os.makedirs(self.moss_report_download_dir, exist_ok=True)
            self._moss_download(self.moss_report_url, self.moss_report_download_dir)

    def write_final_report(self) -> None:
        assignment_report_dir = os.path.join(self.final_report_dir, self.markus_assignment)
        if not os.path.isdir(assignment_report_dir) or self.force:
            self._print(f"Organizing final report for assignment: {self.markus_assignment}")
            os.makedirs(assignment_report_dir, exist_ok=True)
            if os.path.isdir(self.starter_files_dir):
                self._copy_starter_files(assignment_report_dir)
            with open(os.path.join(assignment_report_dir, self.FINAL_REPORT_CASE_OVERVIEW), "w") as overview_f:
                overview_writer = csv.writer(overview_f)
                overview_writer.writerow(self.OVERVIEW_INFO)
                report_iter = self._parse_html_report()
                for i, (match_file, group1, group2, similarity, matched_lines) in enumerate(report_iter):
                    self._print(f"Creating report for groups {group1} and {group2} with {similarity}% similarity.")
                    case = f"case_{i+1}"
                    case_dir = os.path.join(assignment_report_dir, case)
                    os.makedirs(case_dir, exist_ok=True)
                    self._copy_moss_report(match_file, os.path.join(case_dir, f"moss.html"))
                    groups = [group1, group2]
                    for group in groups:
                        self._copy_submission_files(group, case_dir)
                    self._write_case_report(groups, case_dir)
                    overview_writer.writerow((case, ";".join(groups), similarity, matched_lines))

    @property
    def markus_api_key(self) -> str:
        if self.__markus_api_key is None:
            raise Exception("markus_api_key is required to perform this action")
        return self.__markus_api_key

    @property
    def markus_url(self) -> str:
        if self.__markus_url is None:
            raise Exception("markus_url is required to perform this action")
        return self.__markus_url

    @property
    def markus_assignment(self) -> str:
        if self.__markus_assignment is None:
            raise Exception("markus_assignment is required to perform this action")
        return self.__markus_assignment

    @property
    def moss_userid(self) -> int:
        if self.__moss_userid is None:
            raise Exception("moss_userid is required to perform this action")
        return self.__moss_userid

    @property
    def moss_report_url(self) -> str:
        if self.__moss_report_url is None:
            url = None
            if os.path.isfile(self.moss_report_url_file):
                self._print(f"Attempting to read moss report url from {self.moss_report_url_file}")
                with open(self.moss_report_url_file) as f:
                    url = f.read().strip()
            if url:
                self.__moss_report_url = url
            else:
                raise Exception("moss_report_url is required to perform this action")
        return self.__moss_report_url

    @property
    def workdir(self) -> str:
        if self.__workdir is None:
            raise Exception("workdir is required to perform this action")
        return self.__workdir

    @property
    def language(self) -> str:
        if self.__language is None:
            raise Exception("language is required to perform this action")
        return self.__language

    @property
    def submission_files_dir(self) -> str:
        return os.path.join(self.workdir, self.SUBMISSION_FILES_DIR)

    @property
    def pdf_submission_files_dir(self) -> str:
        return os.path.join(self.workdir, self.PDF_SUBMISSION_FILES_DIR)

    @property
    def pdf_starter_files_dir(self) -> str:
        return os.path.join(self.workdir, self.STARTER_FILES_DIR, 'pdf')

    @property
    def org_starter_files_dir(self) -> str:
        return os.path.join(self.workdir, self.STARTER_FILES_DIR, 'org')

    @property
    def starter_files_dir(self) -> str:
        return os.path.join(self.workdir, self.STARTER_FILES_DIR)

    @property
    def moss_report_dir(self) -> str:
        return os.path.join(self.workdir, self.MOSS_REPORT_DIR)

    @property
    def moss_report_url_file(self) -> str:
        return os.path.join(self.moss_report_dir, self.MOSS_REPORT_URL)

    @property
    def moss_report_download_dir(self) -> str:
        return os.path.join(self.moss_report_dir, self.MOSS_REPORT_DOWNLOAD)

    @property
    def final_report_dir(self) -> str:
        return os.path.join(self.workdir, self.FINAL_REPORT_DIR)

    @property
    def api(self) -> markusapi.Markus:
        if self.__api is None:
            self.__api = markusapi.Markus(url=self.markus_url, api_key=self.markus_api_key)
        return self.__api

    @property
    def moss(self) -> mosspy.Moss:
        if self.__moss is None:
            self.__moss = mosspy.Moss(self.moss_userid, self.language)
        return self.__moss

    @property
    def _group_data(self) -> Dict:
        if self.__group_data is None:
            group_data = self.api.get_groups(self._markus_course_id, self._assignment_id)
            if self.groups is not None:
                group_data = [g for g in group_data if g['group_name'] in self.groups]
            self.__group_data = group_data
        return self.__group_data

    @property
    def _membership_data(self) -> Dict:
        if self.__membership_data is None:
            self.__membership_data = self._get_group_membership_info()
        return self.__membership_data

    @property
    def _assignment_id(self) -> int:
        if self.__assignment_id is None:
            self.__assignment_id = self._find_assignment_id()
        return self.__assignment_id

    @property
    def _markus_course_id(self) -> str:
        if self.__markus_course_id is None:
            self.__markus_course_id = self._find_course_id()
        return self.__markus_course_id

    @property
    def _starter_file_groups(self) -> Dict:
        if self.__starter_file_groups is None:
            self.__starter_file_groups = self.api.get_starter_file_groups(self._markus_course_id, self._assignment_id)
        return self.__starter_file_groups

    @property
    def _pandoc(self) -> str:
        pandoc = shutil.which("pandoc")
        if pandoc is None:
            raise Exception(f"No 'pandoc' executable found in the path. Pandoc is required to run this action.")
        return pandoc

    @property
    def _report_regex(self) -> Pattern:
        if self.__report_regex is None:
            self.__report_regex = re.compile(rf"{self.SUBMISSION_FILES_DIR}/([^/]+)/(.*)\s\((\d+)\%\)")
        return self.__report_regex

    @staticmethod
    def _clean_filename(filename) -> str:
        return filename.replace(" ", "_")

    def _print(self, *args, **kwargs) -> None:
        if self.verbose:
            print(self.PRINT_PREFIX, *args, **kwargs)

    def _find_assignment_id(self) -> int:
        short_ids = []
        assignment_data = self.api.get_assignments(self._markus_course_id)
        for data in assignment_data:
            short_ids.append(data.get("short_identifier"))
            if data.get("short_identifier") == self.markus_assignment:
                return data["id"]
        msg = f"No MarkUs assignment found with short identifier: {self.markus_assignment}\noptions:{short_ids}"
        raise Exception(msg)

    def _find_course_id(self) -> int:
        short_ids = []
        course_data = self.api.get_all_courses()
        for data in course_data:
            short_ids.append(data.get("name"))
            if data.get("name") == self.__markus_course:
                return data["id"]
        msg = f"No MarkUs course found with name: {self.markus_course}\noptions:{short_ids}"
        raise Exception(msg)

    def _get_group_membership_info(self) -> Dict:
        user_info = {u["id"]: {k: u.get(k) for k in self.USER_INFO} for u in self.api.get_all_roles(self._markus_course_id)}
        members = collections.defaultdict(list)
        for data in self._group_data:
            for role_id in (m["role_id"] for m in data["members"]):
                user_info[role_id]["group_name"] = data["group_name"]
                members[data["group_name"]].append(user_info[role_id])
        return members

    def _copy_files_to_pdf(self, source_dir: str, dest_dir: str) -> None:
        for source_file in glob.iglob(os.path.join(source_dir, "*", self.file_glob), recursive=True):
            rel_source = os.path.relpath(source_file, source_dir)
            rel_destination = self._file_to_pdf(rel_source)
            abs_destination = os.path.join(dest_dir, rel_destination)
            if self._copy_file_to_pdf(source_file, abs_destination):
                self._print(f"Converting {rel_source} to pdf: {rel_destination}")

    def _copy_file_to_pdf(self, source_file: str, destination: str) -> bool:
        if os.path.isfile(source_file) and (not os.path.isfile(destination) or self.force):
            os.makedirs(os.path.dirname(destination), exist_ok=True)
            proc = subprocess.Popen(
                [self._pandoc, "-V", "geometry:margin=1cm", "-o", destination],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            with open(source_file, "rb") as f:
                content = b"```{.%b .numberLines}\n%b\n```" % (self.language.encode(), f.read())
            _out, err = proc.communicate(content)
            if proc.returncode != 0:
                sys.stderr.write(f"[PANDOC ERROR]{err}\n")
                sys.stderr.flush()
            return True
        return False

    def _parse_html_report(self) -> Iterator[Tuple[str, str, str, int, int]]:
        with open(os.path.join(self.moss_report_download_dir, "index.html")) as f:
            parsed_html = bs4.BeautifulSoup(f, features='html5lib')
            for row in parsed_html.body.find("table").find_all("tr"):
                if row.find("th"):
                    continue
                submission1, submission2, lines = row.find_all("td")
                match_file = os.path.join(
                    self.moss_report_download_dir, os.path.basename(submission1.find("a").get("href"))
                )
                matched_lines = int(lines.string.strip())
                group1, matched_file, similarity = re.match(self._report_regex, submission1.find("a").string).groups()
                group2, _, _ = re.match(self._report_regex, submission2.find("a").string).groups()
                yield match_file, group1, group2, similarity, matched_lines

    def _copy_submission_files(self, group: str, destination: str) -> None:
        for abs_file in glob.iglob(os.path.join(self.submission_files_dir, group, self.file_glob), recursive=True):
            rel_file = os.path.relpath(abs_file, self.submission_files_dir)
            rel_pdf = self._file_to_pdf(rel_file)
            abs_pdf = os.path.join(self.pdf_submission_files_dir, rel_pdf)
            file_dest = os.path.join(destination, group, "org", os.path.relpath(rel_file, group))
            pdf_dest = os.path.join(destination, group, "pdf", os.path.relpath(rel_pdf, group))
            os.makedirs(os.path.dirname(file_dest), exist_ok=True)
            os.makedirs(os.path.dirname(pdf_dest), exist_ok=True)
            self._copy_file(abs_file, file_dest)
            self._copy_file(abs_pdf, pdf_dest)

    def _copy_starter_files(self, destination: str) -> None:
        shutil.copytree(self.starter_files_dir, os.path.join(destination, self.STARTER_FILES_DIR), dirs_exist_ok=True)

    def _write_case_report(self, groups: Iterable[str], destination: str) -> None:
        for group in groups:
            group_membership_file = os.path.join(destination, group, self.GROUP_MEMBERSHIP_FILE)
            os.makedirs(os.path.join(destination, group), exist_ok=True)
            with open(group_membership_file, "w") as f:
                writer = csv.DictWriter(f, fieldnames=self.USER_INFO)
                writer.writeheader()
                for data in self._membership_data[group]:
                    writer.writerow(data)

    @staticmethod
    def _copy_file(source: str, dest: str) -> None:
        try:
            shutil.copy(source, dest)
        except FileNotFoundError as e:
            sys.stderr.write(f"{e}\n")
            sys.stderr.flush()

    @staticmethod
    def _file_to_pdf(source: str) -> str:
        return f"{source}.pdf"

    def _copy_moss_report(self, base_html_file: str, destination: str) -> None:
        base, _ = os.path.splitext(base_html_file)
        base_basename = os.path.basename(base)
        top = f"{base}-top.html"
        with open(os.path.join(os.path.dirname(__file__), 'templates', 'report_template.html')) as f:
            template = bs4.BeautifulSoup(f, features='html5lib')
        with open(base_html_file) as f:
            base_html = bs4.BeautifulSoup(f, features='html5lib')
            title = base_html.head.find("title").text
            template.head.find('title').string = title
        with open(top) as f:
            top_html = bs4.BeautifulSoup(f, features='html5lib')
            table = top_html.body.find("center")
            for a in table.find_all('a'):
                href = os.path.basename(a["href"])
                match_file, match_num = re.match(rf'{base_basename}-([01])\.html#(\d+)', href).groups()
                a["href"] = f"#match-{match_file}-{match_num}"
                a["target"] = "_self"
            top_div = template.body.find('div', {"id": "top"})
            top_div.append(table)
        for match_i in range(2):
            match_file = f"{base}-{match_i}.html"
            with open(match_file) as f:
                match_html = bs4.BeautifulSoup(f, features='html5lib')
                match_body = match_html.body
                for a in match_body.find_all('a'):
                    if a.get("href"):
                        match_file, match_num = re.match(rf'{base_basename}-([01])\.html#(\d+)', a["href"]).groups()
                        a["href"] = f"#match-{match_file}-{match_num}"
                        a["target"] = "_self"
                    if a.get("name"):
                        a["id"] = f"match-{match_i}-{a['name']}"
            match_div = template.body.find('div', {"id": f"match-{match_i}"})
            file_title = template.new_tag('h3')
            match_div.append(file_title)
            match_div.append(match_body)
        with open(destination, 'w') as f:
            f.write(str(template))

    @staticmethod
    def _unzip_file(zip_byte_stream: bytes, destination: str) -> None:
        with zipfile.ZipFile(io.BytesIO(zip_byte_stream)) as zf:
            for fname in zf.namelist():
                *dpaths, bname = fname.split(os.sep)
                dest = os.path.join(destination, *dpaths[1:])
                filename = os.path.join(dest, bname)
                if filename.endswith("/"):
                    os.makedirs(filename, exist_ok=True)
                else:
                    os.makedirs(dest, exist_ok=True)
                    with open(filename, "wb") as f:
                        f.write(zf.read(fname))
