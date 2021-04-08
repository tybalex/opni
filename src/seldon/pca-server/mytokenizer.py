# Standard Library
import re
from typing import List


class MaskingInstruction:
    def __init__(self, regex_pattern: str, mask_with: str):
        self.regex_pattern = regex_pattern
        self.mask_with = mask_with
        self.regex = re.compile(regex_pattern)
        self.mask_with_wrapped = "<" + mask_with + ">"  ## add space or not?


class RegexMasker:
    def __init__(self, masking_instructions: List[MaskingInstruction]):
        self.masking_instructions = masking_instructions

    def mask(self, content: str):
        for mi in self.masking_instructions:
            # content = re.sub(mi.regex, mi.mask_with_wrapped, content)
            content = mi.regex.sub(mi.mask_with_wrapped, content)
        return content


def masker():
    masking_list = [
        {
            "regex_pattern": "((?<=[^A-Za-z0-9])|^)(([0-9a-f]{2,}:){3,}([0-9a-f]{2,}))((?=[^A-Za-z0-9])|$)",
            "mask_with": "ID",
        },
        {
            "regex_pattern": "((?<=[^A-Za-z0-9])|^)(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\/\\d{1,3})((?=[^A-Za-z0-9])|$)",
            "mask_with": "IP",
        },
        {
            "regex_pattern": "((?<=[^A-Za-z0-9])|^)(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})((?=[^A-Za-z0-9])|$)",
            "mask_with": "IP",
        },
        {
            "regex_pattern": "((?<=[^A-Za-z0-9])|^)([0-9a-f]{6,} ?){3,}((?=[^A-Za-z0-9])|$)",
            "mask_with": "SEQ",
        },
        {
            "regex_pattern": "((?<=[^A-Za-z0-9])|^)([0-9A-F]{4} ?){4,}((?=[^A-Za-z0-9])|$)",
            "mask_with": "SEQ",
        },
        {
            "regex_pattern": "((?<=[^A-Za-z0-9])|^)(0x[a-f0-9A-F]+)((?=[^A-Za-z0-9])|$)",
            "mask_with": "HEX",
        },
        {
            "regex_pattern": "((?<=[^A-Za-z0-9])|^)(\\d+\\.\\d+ms)((?=[^A-Za-z0-9])|$)",
            "mask_with": "DURATION",
        },
        {
            "regex_pattern": "((?<=[^A-Za-z0-9])|^)(\\d+\\.\\d+s)((?=[^A-Za-z0-9])|$)",
            "mask_with": "DURATION",
        },
        {
            "regex_pattern": "((?<=[^A-Za-z0-9])|^)([\\-\\+]?\\d+)((?=[^A-Za-z0-9])|$)",
            "mask_with": "NUM",
        },
        {"regex_pattern": '(?<=executed cmd )(".+?")', "mask_with": "CMD"},
    ]
    masking_instructions = []
    for mi in masking_list:
        # logging.info("Adding custom mask {} --> {}".format(
        #     mi['mask_with'],
        #     mi['regex_pattern']))
        instruction = MaskingInstruction(mi["regex_pattern"], mi["mask_with"])
        masking_instructions.append(instruction)
    regex_masker = RegexMasker(masking_instructions)
    return regex_masker


regex_masker = masker()


def tokenize(sent):
    sent = sent.lower()
    sent = sent.replace("'", "")
    sent = sent.replace('"', "")
    sent = regex_masker.mask(sent)
    filters = '[ |:|\(|\)|\[|\]|\{|\}|"|,|=]'
    filtered = re.split(filters, sent)
    new_filtered = []
    for f in filtered:
        if f != None and f != "":
            new_filtered.append(f)
    return new_filtered
