import os
import mosspy
import toml
import argparse
from .markusmoss import MarkusMoss

DEFAULTRC = "markusmossrc"


DEFAULTS = {
    "workdir": os.getcwd(),
    "language": mosspy.Moss.languages,
    "file_glob": "**/*",
    "html_parser": "html.parser"
}


def _parse_config(pre_args):
    args_dict = vars(pre_args).copy()
    if os.path.isfile(pre_args.config):
        with open(pre_args.config) as cf:
            config_args = toml.load(cf)
        for key, value in config_args.items():
            if args_dict.get(key) is None:
                args_dict[key] = value
    for key, value in DEFAULTS.items():
        if args_dict.get(key) is None:
            args_dict[key] = value
    return args_dict


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--markus-api-key")
    parser.add_argument("--markus-url")
    parser.add_argument("--markus-assignment")
    parser.add_argument("--moss-userid")
    parser.add_argument("--moss-report-url")
    parser.add_argument("--config", default=os.path.join(os.getcwd(), DEFAULTRC))
    parser.add_argument("--workdir")
    parser.add_argument("--actions", nargs="*", default=None, choices=MarkusMoss.ACTIONS)
    parser.add_argument("--language")
    parser.add_argument("--file-glob")
    parser.add_argument("--html-parser")
    parser.add_argument("--groups", nargs="*", default=None)
    parser.add_argument("-f", "--force", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")

    return _parse_config(parser.parse_args())


def cli():
    kwargs = _parse_args()
    kwargs.pop("config")
    actions = kwargs.pop("actions")
    MarkusMoss(**kwargs).run(actions=actions)


if __name__ == "__main__":
    cli()
