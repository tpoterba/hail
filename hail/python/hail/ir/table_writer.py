import abc
import json
from ..typecheck import *
from ..utils.java import escape_str


class TableWriter(object):
    @abc.abstractmethod
    def render(self):
        pass

    @abc.abstractmethod
    def __eq__(self, other):
        pass


class TableNativeWriter(TableWriter):
    @typecheck_method(path=str,
                      overwrite=bool,
                      stage_locally=bool,
                      codec_spec=nullable(str))
    def __init__(self, path, overwrite, stage_locally, codec_spec):
        self.path = path
        self.overwrite = overwrite
        self.stage_locally = stage_locally
        self.codec_spec = codec_spec

    def render(self):
        writer = {'name': 'TableNativeWriter',
                  'path': self.path,
                  'overwrite': self.overwrite,
                  'stageLocally': self.stage_locally,
                  'codecSpecJSONStr': self.codec_spec}
        return escape_str(json.dumps(writer))

    def __eq__(self, other):
        return isinstance(other, TableNativeWriter) and \
               other.path == self.path and \
               other.overwrite == self.overwrite and \
               other.stage_locally == self.stage_locally and \
               other.codec_spec == self.codec_spec


class TableTextWriter(TableWriter):
    @typecheck_method(path=str,
                      types_file=nullable(str),
                      header=bool,
                      export_type=int,
                      delimiter=str)
    def __init__(self, child, path, types_file, header, export_type, delimiter):
        super().__init__(child)
        self.path = path
        self.types_file = types_file
        self.header = header
        self.export_type = export_type
        self.delimiter = delimiter

    def render(self):
        writer = {'name': 'TableTextWriter',
                  'path': self.path,
                  'typesFile': self.types_file,
                  'header': self.header,
                  'exportType': self.export_type,
                  'delimiter': self.delimiter}
        return escape_str(json.dumps(writer))

    def __eq__(self, other):
        return isinstance(other, TableTextWriter) and \
               other.path == self.path and \
               other.types_file == self.types_file and \
               other.header == self.header and \
               other.export_type == self.export_type and \
               other.delimiter == self.delimiter
