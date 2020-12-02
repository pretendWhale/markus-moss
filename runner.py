#!/usr/bin/env python3

import subprocess
import toml
import os


def user_input(prompt, check, success=None, failure=None):
    while True:
        inp = input(prompt).strip()
        if check(inp):
            if success:
                print(success)
            return inp
        else:
            print(failure)


def choose_workdir():
    if user_input('Would you like to write files in the current folder? [yn]', lambda x: x.lower() in 'yn') == 'y':
        workdir = os.getcwd()
    else:
        workdir = user_input('Which directory would you like to use?',
                             lambda x: os.path.isdir(x),
                             failure='That directory does not exist')
    return os.path.abspath(workdir)


def install_venv(workdir):
    python = os.path.join(workdir, 'venv', 'bin', 'python3')
    if not os.path.isfile(python):
        subprocess.run(['python3', '-m', 'venv', f'{workdir}/venv'], check=True)
    if not os.path.isfile(os.path.join(workdir, 'venv', 'bin', 'markusmoss')):
        pip = os.path.join(workdir, 'venv', 'bin', 'pip')
        subprocess.run([pip, 'install', 'git+https://github.com/MarkUsProject/markus-moss.git'], check=True)
    return os.path.join(workdir, 'venv', 'bin', 'markusmoss')


def _update_conf_simple(conf, key, description, check=lambda x: x, failure=None):
    if conf.get(key):
        if user_input(f'Reuse existing {description}?: {conf[key]}', lambda x: x.lower() in 'yn') == 'n':
            conf[key] = user_input(f'New {description}:', check)
    else:
        conf[key] = user_input(f'New {description}:', check, failure=failure)


def update_config_file(workdir):
    config_file = os.path.join(workdir, 'markusmossrc')
    if os.path.isfile(config_file):
        if user_input('A configuration file already exists. Do you want to update it? [yn]',
                      lambda x: x.lower() in 'yn') == 'n':
            return
    else:
        with open(config_file, 'w'):
            pass

    with open(config_file) as f:
        conf = toml.load(f)

    _update_conf_simple(conf, 'markus_api_key', 'MarkUs API key')
    _update_conf_simple(conf, 'markus_url', 'MarkUs URL')
    _update_conf_simple(conf, 'markus_assignment', 'MarkUs Assignment')
    _update_conf_simple(conf, 'moss_userid', 'MOSS User ID', check=str.isdigit)
    conf['moss_userid'] = int(conf['moss_userid'])

    languages = ('c', 'cc', 'java', 'ml', 'pascal', 'ada',
                 'lisp', 'scheme', 'haskell', 'fortran', 'ascii',
                 'vhdl', 'perl', 'matlab', 'python', 'mips', 'prolog',
                 'spice', 'vb', 'csharp', 'modula2', 'a8086', 'javascript', 'plsql')
    _update_conf_simple(conf, 'language', 'Programming Language',
                        check=lambda x, l=languages: x in l, failure=f'Choose from: {languages}')
    _update_conf_simple(conf, 'file_glob', 'File Extension',
                        check=lambda x: x.startswith('.'), failure='examples: .py .java .hs .sql')
    conf['file_glob'] = f"**/*{conf['file_glob']}"
    with open(config_file, 'w') as f:
        toml.dump(conf, f)


def run_markusmoss(workdir, markusmoss):
    actions = (
        "download_submission_files",
        "download_starter_files",
        "copy_files_to_pdf",
        "run_moss",
        "download_moss_report",
        "write_final_report",
        "all"
    )
    action = user_input('What do you want markusmoss to do?',
                        check=lambda x, a=actions: x.lower() in a, failure=f"Choose from: {actions}")
    force = user_input('Do you want to re-run this action if files already exist? [yn]',
                       check=lambda x: x.lower() in 'yn') == 'y'
    args = [markusmoss, '--config', os.path.join(workdir, 'markusmossrc'), '-v']
    if action != "all":
        args.extend(["--actions", action])
    if force:
        args.append('-f')
    subprocess.run(args)


def main():
    workdir = choose_workdir()
    markusmoss = install_venv(workdir)
    update_config_file(workdir)
    run_markusmoss(workdir, markusmoss)


if __name__ == '__main__':
    main()

