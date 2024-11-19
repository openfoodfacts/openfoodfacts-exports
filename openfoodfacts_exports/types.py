import enum


class ExportFlavor(str, enum.Enum):
    off = "off"
    obf = "obf"
    opf = "opf"
    opff = "opff"
    op = "op"
